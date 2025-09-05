use std::sync::Arc;
use tokio::fs;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use tokio::sync::Mutex as TokioMutex;
use tokio::time::{timeout, Duration};
use log::{info, debug, warn, error};
use anyhow::Context;
use crate::storage_engine::config::{
    SledConfig, SledStorage, SledDaemonPool, load_storage_config_from_yaml, 
    load_engine_specific_config, DEFAULT_DATA_DIRECTORY, SelectedStorageConfig,
    StorageConfigInner, DEFAULT_STORAGE_CONFIG_PATH, DEFAULT_STORAGE_CONFIG_PATH_RELATIVE,
    DEFAULT_STORAGE_PORT, is_port_in_cluster_range, parse_cluster_range, 
    StorageConfig, StorageEngineType
};
use crate::storage_engine::storage_utils::{serialize_vertex, deserialize_vertex, serialize_edge, deserialize_edge, create_edge_key};
use models::{Vertex, Edge, Identifier};
use models::errors::{GraphError, GraphResult};
use uuid::Uuid;
use async_trait::async_trait;
use crate::storage_engine::storage_engine::{StorageEngine, GraphStorageEngine};
use serde_json::Value;
use std::any::Any;
#[cfg(unix)]
use tokio::signal::unix::{signal, SignalKind};
use futures::future::join_all;
use std::process;
use crate::daemon_registry::GLOBAL_DAEMON_REGISTRY;

impl SledStorage {
    pub async fn new(config: &SledConfig) -> Result<Self, GraphError> {
        // Step 1: Determine config file path (not storage directory)
        let main_config_path = {
            let project_config_path = PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE);
            let default_config_path = PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH);
            if project_config_path.exists() {
                debug!("Using project config path: {:?}", project_config_path);
                project_config_path
            } else {
                debug!("Using default config path: {:?}", default_config_path);
                default_config_path
            }
        };

        // Step 2: Load or create storage configuration
        let mut storage_config = if main_config_path.exists() {
            debug!("Reading main configuration from file: {:?}", main_config_path);
            load_storage_config_from_yaml(Some(&main_config_path)).await
                .context(format!("Failed to load storage config from {:?}", main_config_path))?
        } else {
            debug!("No main config file found, creating default at {:?}", main_config_path);
            let default_config = StorageConfig {
                config_root_directory: PathBuf::from("./storage_daemon_server"),
                data_directory: PathBuf::from(DEFAULT_DATA_DIRECTORY),
                log_directory: String::from("/opt/graphdb/logs"),
                default_port: 8049,
                cluster_range: String::from("8049"),
                max_disk_space_gb: 1000,
                min_disk_space_gb: 10,
                use_raft_for_scale: false,
                storage_engine_type: StorageEngineType::Sled,
                connection_string: None,
                engine_specific_config: Some(HashMap::from([
                    ("path".to_string(), Value::String(config.path.to_string_lossy().into_owned())),
                    ("host".to_string(), Value::String(config.host.clone().unwrap_or("127.0.0.1".to_string()))),
                    ("port".to_string(), Value::Number(config.port.unwrap_or(8049).into())),
                    ("temporary".to_string(), Value::Bool(config.temporary)),
                    ("use_compression".to_string(), Value::Bool(config.use_compression)),
                    ("cache_capacity".to_string(), Value::Number(config.cache_capacity.unwrap_or(1024 * 1024 * 1024).into())),
                    ("storage_engine_type".to_string(), Value::String("sled".to_string())),
                ])),
                max_open_files: Some(1024),
            };
            // Create parent directory and save default config
            if let Some(parent) = main_config_path.parent() {
                debug!("Creating parent directory: {:?}", parent);
                if let Err(e) = fs::create_dir_all(parent).await {
                    error!("Failed to create parent directory {:?}: {}", parent, e);
                    return Err(GraphError::StorageError(format!("Failed to create parent directory for config: {}", e)));
                }
            }
            if let Err(e) = default_config.save().await {
                error!("Failed to save default config to {:?}: {}", main_config_path, e);
                return Err(GraphError::StorageError(format!("Failed to save default config: {}", e)));
            }
            debug!("Default config saved to {:?}", main_config_path);
            default_config
        };

        // Step 3: Validate storage directory (config.path)
        let storage_path = &config.path;
        if !storage_path.exists() {
            debug!("Creating storage directory: {:?}", storage_path);
            if let Err(e) = fs::create_dir_all(storage_path).await {
                error!("Failed to create storage directory {:?}: {}", storage_path, e);
                return Err(GraphError::StorageError(format!("Failed to create storage directory: {}", e)));
            }
        }

        // Step 4: Update engine-specific config if missing or incomplete
        if storage_config.engine_specific_config.is_none() || storage_config.engine_specific_config.as_ref().unwrap().is_empty() {
            debug!("Setting default engine-specific config for Sled");
            storage_config.engine_specific_config = Some(HashMap::from([
                ("path".to_string(), Value::String(storage_path.to_string_lossy().into_owned())),
                ("host".to_string(), Value::String(config.host.clone().unwrap_or("127.0.0.1".to_string()))),
                ("port".to_string(), Value::Number(config.port.unwrap_or(storage_config.default_port).into())),
                ("temporary".to_string(), Value::Bool(config.temporary)),
                ("use_compression".to_string(), Value::Bool(config.use_compression)),
                ("cache_capacity".to_string(), Value::Number(config.cache_capacity.unwrap_or(1024 * 1024 * 1024).into())),
                ("storage_engine_type".to_string(), Value::String("sled".to_string())),
            ]));
        }

        // Step 5: Initialize daemon pool
        let mut daemon_pool = SledDaemonPool::new();
        daemon_pool
            .initialize_cluster(&storage_config, config)
            .await
            .map_err(|e| GraphError::StorageError(format!("Failed to initialize SledDaemonPool: {}", e)))?;

        let pool = Arc::new(TokioMutex::new(daemon_pool));

        info!(
            "Successfully initialized SledStorage with config path: {:?}, storage path: {:?}", 
            main_config_path, storage_path
        );
        Ok(SledStorage { pool })
    }

    // ... (rest of the file remains unchanged, included for completeness)
    pub async fn set_key(&self, key: &str, value: &str) -> GraphResult<()> {
        let pool = self.pool.lock().await;
        let daemon = pool.leader_daemon().await?;
        debug!("Setting key '{}' to value '{}' in Sled database at {:?}", key, value, daemon.db_path());
        daemon.kv_pairs
            .insert(key.as_bytes(), value.as_bytes())
            .map_err(|e| {
                error!("Failed to set key '{}': {}", key, e);
                GraphError::StorageError(format!("Failed to set key '{}': {}", key, e))
            })?;
        daemon.db.flush_async().await.map_err(|e| {
            error!("Failed to flush Sled database after setting key '{}': {}", key, e);
            GraphError::StorageError(format!("Failed to flush Sled database after setting key '{}': {}", key, e))
        })?;
        debug!("Successfully set key '{}' and flushed database", key);
        Ok(())
    }

    pub async fn get_key(&self, key: &str) -> GraphResult<Option<String>> {
        let pool = self.pool.lock().await;
        let daemon = pool.leader_daemon().await?;
        debug!("Retrieving key '{}' from Sled database at {:?}", key, daemon.db_path());
        let value = daemon.kv_pairs
            .get(key.as_bytes())
            .map_err(|e| {
                error!("Failed to get key '{}': {}", key, e);
                GraphError::StorageError(format!("Failed to get key '{}': {}", key, e))
            })?
            .map(|v| String::from_utf8_lossy(&v).to_string());
        debug!("Retrieved value for key '{}': {:?}", key, value);
        Ok(value)
    }

    pub async fn force_unlock(path: &Path) -> GraphResult<()> {
        info!("Attempting to force unlock Sled database at {:?}", path);
        let lock_path = path.join("db");

        debug!("Checking for lock file at {:?}", lock_path);
        if lock_path.exists() {
            warn!("Found potential lock file at {:?}", lock_path);
            #[cfg(unix)]
            {
                const MAX_RETRIES: u32 = 3;
                const BASE_DELAY_MS: u64 = 1000;
                let mut attempt = 0;

                while attempt < MAX_RETRIES {
                    match timeout(Duration::from_secs(2), tokio::process::Command::new("lsof")
                        .arg("-t")
                        .arg(lock_path.to_str().ok_or_else(|| {
                            GraphError::StorageError("Invalid lock file path".to_string())
                        })?)
                        .output())
                        .await
                    {
                        Ok(Ok(output)) => {
                            let lsof_output = String::from_utf8_lossy(&output.stdout).to_string();
                            info!("lsof output for {:?}: {}", lock_path, lsof_output);
                            let pids: Vec<u32> = lsof_output
                                .lines()
                                .filter_map(|pid| pid.trim().parse().ok())
                                .collect();
                            info!("PIDs holding lock on {:?}: {:?}", lock_path, pids);
                            let current_pid = std::process::id();

                            for pid in pids {
                                if pid != current_pid {
                                    let proc_info = timeout(Duration::from_secs(2), tokio::process::Command::new("ps")
                                        .arg("-p")
                                        .arg(pid.to_string())
                                        .arg("-o")
                                        .arg("pid,comm,args")
                                        .output())
                                        .await
                                        .map_err(|_| GraphError::StorageError("Timeout getting process info".to_string()))?
                                        .map_err(|e| GraphError::StorageError(format!("Failed to get process info for PID {}: {}", pid, e)))?;
                                    let proc_info_str = String::from_utf8_lossy(&proc_info.stdout);
                                    warn!("Process {} is holding lock file {:?}:\n{}", pid, lock_path, proc_info_str);

                                    if let Err(e) = timeout(Duration::from_secs(2), tokio::process::Command::new("kill")
                                        .arg("-TERM")
                                        .arg(pid.to_string())
                                        .status())
                                        .await
                                        .map_err(|_| GraphError::StorageError("Timeout sending SIGTERM".to_string()))?
                                    {
                                        warn!("Failed to send SIGTERM to process {}: {}", pid, e);
                                    }
                                    tokio::time::sleep(Duration::from_millis(1000)).await;

                                    let is_running = timeout(Duration::from_secs(2), tokio::process::Command::new("ps")
                                        .arg("-p")
                                        .arg(pid.to_string())
                                        .status())
                                        .await
                                        .map_err(|_| GraphError::StorageError("Timeout checking process status".to_string()))?
                                        .map(|status| status.success())
                                        .unwrap_or(false);

                                    if is_running {
                                        warn!("Process {} still running after SIGTERM, attempting SIGKILL", pid);
                                        if let Err(e) = timeout(Duration::from_secs(2), tokio::process::Command::new("kill")
                                            .arg("-KILL")
                                            .arg(pid.to_string())
                                            .status())
                                            .await
                                            .map_err(|_| GraphError::StorageError("Timeout sending SIGKILL".to_string()))?
                                        {
                                            warn!("Failed to send SIGKILL to process {}: {}", pid, e);
                                        }
                                        tokio::time::sleep(Duration::from_millis(500)).await;
                                    }
                                }
                            }

                            if let Err(e) = timeout(Duration::from_secs(2), fs::remove_file(&lock_path)).await {
                                warn!("Failed to remove lock file at {:?} on attempt {}: {:?}", lock_path, attempt + 1, e);
                                attempt += 1;
                                tokio::time::sleep(Duration::from_millis(BASE_DELAY_MS * (attempt as u64 + 1))).await;
                                continue;
                            }
                            info!("Successfully removed lock file at {:?}", lock_path);
                            break;
                        }
                        Ok(Err(e)) => {
                            warn!("Failed to run lsof on {:?} on attempt {}: {}", lock_path, attempt + 1, e);
                            attempt += 1;
                            tokio::time::sleep(Duration::from_millis(BASE_DELAY_MS * (attempt as u64 + 1))).await;
                            continue;
                        }
                        Err(_) => {
                            warn!("Timeout running lsof on {:?} on attempt {}", lock_path, attempt + 1);
                            attempt += 1;
                            tokio::time::sleep(Duration::from_millis(BASE_DELAY_MS * (attempt as u64 + 1))).await;
                            continue;
                        }
                    }
                }

                if attempt >= MAX_RETRIES {
                    error!("Failed to unlock {:?} after {} attempts", lock_path, MAX_RETRIES);
                    return Err(GraphError::StorageError(format!("Failed to unlock {:?} after {} attempts", lock_path, MAX_RETRIES)));
                }
            }
            #[cfg(not(unix))]
            {
                if let Err(e) = timeout(Duration::from_secs(2), fs::remove_file(&lock_path)).await {
                    error!("Failed to remove lock file at {:?}: {:?}", lock_path, e);
                    return Err(GraphError::StorageError(format!("Failed to remove lock file at {:?}", lock_path)));
                }
                info!("Successfully removed lock file at {:?}", lock_path);
            }
        } else {
            info!("No lock file found at {:?}", lock_path);
        }
        Ok(())
    }

    pub async fn force_reset(config: &SledConfig) -> GraphResult<Self> {
        warn!("FORCE RESET: Completely destroying and recreating database at {:?}", config.path);
        if config.path.exists() {
            if let Err(e) = timeout(Duration::from_secs(10), tokio::fs::remove_dir_all(&config.path)).await {
                error!("Timeout or error removing database directory at {:?}: {:?}", config.path, e);
                return Err(GraphError::StorageError(format!("Failed to remove database directory: {:?}", e)));
            }
        }
        timeout(Duration::from_secs(30), SledStorage::new(config)).await
            .map_err(|_| GraphError::StorageError("Timeout initializing SledStorage after reset".to_string()))?
    }

    pub async fn close(&self) -> GraphResult<()> {
        let pool = self.pool.lock().await;
        info!("Closing SledStorage pool");
        let futures = pool.daemons.values().map(|daemon| async {
            let db_path = daemon.db_path();
            match timeout(Duration::from_secs(10), daemon.db.flush_async()).await {
                Ok(Ok(bytes_flushed)) => {
                    info!("Flushed {} bytes during close at {:?}", bytes_flushed, db_path);
                    Ok(())
                }
                Ok(Err(e)) => {
                    error!("Failed to flush daemon at {:?}: {}", db_path, e);
                    Err(GraphError::StorageError(e.to_string()))
                }
                Err(_) => {
                    error!("Timeout flushing daemon at {:?}", db_path);
                    Err(GraphError::StorageError(format!("Timeout flushing daemon at {:?}", db_path)))
                }
            }
        });
        let results = join_all(futures).await;
        let errors: Vec<_> = results.into_iter().filter_map(|r| r.err()).collect();
        if !errors.is_empty() {
            error!("Errors during close: {:?}", errors);
            return Err(GraphError::StorageError(format!("Close errors: {:?}", errors)));
        }

        match timeout(Duration::from_secs(10), pool.close(None)).await {
            Ok(Ok(_)) => {
                info!("Successfully closed SledStorage pool");
                Ok(())
            }
            Ok(Err(e)) => {
                error!("Failed to close SledStorage pool: {}", e);
                Err(e)
            }
            Err(_) => {
                error!("Timeout closing SledStorage pool");
                Err(GraphError::StorageError("Timeout closing SledStorage pool".to_string()))
            }
        }
    }
}

#[async_trait]
impl StorageEngine for SledStorage {
    async fn connect(&self) -> Result<(), GraphError> {
        info!("Connecting to SledStorage");
        Ok(())
    }

    async fn insert(&self, key: Vec<u8>, value: Vec<u8>) -> GraphResult<()> {
        let pool = self.pool.lock().await;
        let daemon = pool.leader_daemon().await?;
        let db_path = daemon.db_path();
        info!("Inserting key {:?} into kv_pairs at path {:?}", key, db_path);
        timeout(Duration::from_secs(5), daemon.insert(&key, &value)).await
            .map_err(|_| GraphError::StorageError("Timeout during insert".to_string()))??;
        let bytes_flushed = timeout(Duration::from_secs(10), daemon.db.flush_async()).await
            .map_err(|_| GraphError::StorageError("Timeout flushing after insert".to_string()))?
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        info!("Flushed {} bytes after insert at {:?}", bytes_flushed, db_path);
        let keys: Vec<_> = daemon.kv_pairs
            .iter()
            .keys()
            .filter_map(|k| k.ok())
            .map(|k| String::from_utf8_lossy(&k).to_string())
            .collect();
        info!("Current keys in kv_pairs at {:?}: {:?}", db_path, keys);
        Ok(())
    }

    async fn retrieve(&self, key: &Vec<u8>) -> GraphResult<Option<Vec<u8>>> {
        let pool = self.pool.lock().await;
        let daemon = pool.leader_daemon().await?;
        let db_path = daemon.db_path();
        info!("Retrieving key {:?} from kv_pairs at path {:?}", key, db_path);
        let value = timeout(Duration::from_secs(5), daemon.retrieve(key)).await
            .map_err(|_| GraphError::StorageError("Timeout during retrieve".to_string()))??;
        let keys: Vec<_> = daemon.kv_pairs
            .iter()
            .keys()
            .filter_map(|k| k.ok())
            .map(|k| String::from_utf8_lossy(&k).to_string())
            .collect();
        info!("Current keys in kv_pairs at {:?}: {:?}", db_path, keys);
        Ok(value)
    }

    async fn delete(&self, key: &Vec<u8>) -> GraphResult<()> {
        let pool = self.pool.lock().await;
        let daemon = pool.leader_daemon().await?;
        let db_path = daemon.db_path();
        info!("Deleting key {:?} from kv_pairs at path {:?}", key, db_path);
        timeout(Duration::from_secs(5), daemon.delete(key)).await
            .map_err(|_| GraphError::StorageError("Timeout during delete".to_string()))??;
        let bytes_flushed = timeout(Duration::from_secs(10), daemon.db.flush_async()).await
            .map_err(|_| GraphError::StorageError("Timeout flushing after delete".to_string()))?
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        info!("Flushed {} bytes after delete at {:?}", bytes_flushed, db_path);
        let keys: Vec<_> = daemon.kv_pairs
            .iter()
            .keys()
            .filter_map(|k| k.ok())
            .map(|k| String::from_utf8_lossy(&k).to_string())
            .collect();
        info!("Current keys in kv_pairs at {:?}: {:?}", db_path, keys);
        Ok(())
    }

    async fn flush(&self) -> Result<(), GraphError> {
        let pool = self.pool.lock().await;
        let futures = pool.daemons.values().map(|daemon| async {
            let db_path = daemon.db_path();
            info!("Flushing daemon at path {:?}", db_path);
            match timeout(Duration::from_secs(10), daemon.db.flush_async()).await {
                Ok(Ok(bytes_flushed)) => {
                    info!("Flushed {} bytes to disk at {:?}", bytes_flushed, db_path);
                    Ok(())
                }
                Ok(Err(e)) => {
                    error!("Failed to flush daemon at {:?}: {}", db_path, e);
                    Err(GraphError::StorageError(e.to_string()))
                }
                Err(_) => {
                    error!("Timeout flushing daemon at {:?}", db_path);
                    Err(GraphError::StorageError(format!("Timeout flushing daemon at {:?}", db_path)))
                }
            }
        });
        let results = join_all(futures).await;
        let errors: Vec<_> = results.into_iter().filter_map(|r| r.err()).collect();
        if !errors.is_empty() {
            error!("Errors during flush: {:?}", errors);
            return Err(GraphError::StorageError(format!("Flush errors: {:?}", errors)));
        }
        info!("Successfully flushed all daemons");
        Ok(())
    }
}

#[async_trait]
impl GraphStorageEngine for SledStorage {
    async fn create_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        let pool = self.pool.lock().await;
        let daemon = pool.leader_daemon().await?;
        let db_path = daemon.db_path();
        info!("Creating vertex at path {:?}", db_path);
        timeout(Duration::from_secs(5), pool.create_vertex(vertex, None)).await
            .map_err(|_| GraphError::StorageError("Timeout during create_vertex".to_string()))??;
        let bytes_flushed = timeout(Duration::from_secs(10), daemon.db.flush_async()).await
            .map_err(|_| GraphError::StorageError("Timeout flushing after create_vertex".to_string()))?
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        info!("Flushed {} bytes after creating vertex at {:?}", bytes_flushed, db_path);
        let vertex_keys: Vec<_> = daemon.vertices
            .iter()
            .keys()
            .filter_map(|k| k.ok())
            .map(|k| String::from_utf8_lossy(&k).to_string())
            .collect();
        info!("Current vertex keys at {:?}: {:?}", db_path, vertex_keys);
        Ok(())
    }

    async fn get_vertex(&self, id: &Uuid) -> GraphResult<Option<Vertex>> {
        let pool = self.pool.lock().await;
        let daemon = pool.leader_daemon().await?;
        let db_path = daemon.db_path();
        info!("Retrieving vertex with id {} from path {:?}", id, db_path);
        let vertex = timeout(Duration::from_secs(5), pool.get_vertex(id, None)).await
            .map_err(|_| GraphError::StorageError("Timeout during get_vertex".to_string()))??;
        let vertex_keys: Vec<_> = daemon.vertices
            .iter()
            .keys()
            .filter_map(|k| k.ok())
            .map(|k| String::from_utf8_lossy(&k).to_string())
            .collect();
        info!("Current vertex keys at {:?}: {:?}", db_path, vertex_keys);
        Ok(vertex)
    }

    async fn update_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        let pool = self.pool.lock().await;
        let daemon = pool.leader_daemon().await?;
        let db_path = daemon.db_path();
        info!("Updating vertex at path {:?}", db_path);
        timeout(Duration::from_secs(5), pool.update_vertex(vertex, None)).await
            .map_err(|_| GraphError::StorageError("Timeout during update_vertex".to_string()))??;
        let bytes_flushed = timeout(Duration::from_secs(10), daemon.db.flush_async()).await
            .map_err(|_| GraphError::StorageError("Timeout flushing after update_vertex".to_string()))?
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        info!("Flushed {} bytes after updating vertex at {:?}", bytes_flushed, db_path);
        let vertex_keys: Vec<_> = daemon.vertices
            .iter()
            .keys()
            .filter_map(|k| k.ok())
            .map(|k| String::from_utf8_lossy(&k).to_string())
            .collect();
        info!("Current vertex keys at {:?}: {:?}", db_path, vertex_keys);
        Ok(())
    }

    async fn delete_vertex(&self, id: &Uuid) -> GraphResult<()> {
        let pool = self.pool.lock().await;
        let daemon = pool.leader_daemon().await?;
        let db_path = daemon.db_path();
        info!("Deleting vertex with id {} from path {:?}", id, db_path);
        timeout(Duration::from_secs(5), pool.delete_vertex(id, None)).await
            .map_err(|_| GraphError::StorageError("Timeout during delete_vertex".to_string()))??;
        let bytes_flushed = timeout(Duration::from_secs(10), daemon.db.flush_async()).await
            .map_err(|_| GraphError::StorageError("Timeout flushing after delete_vertex".to_string()))?
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        info!("Flushed {} bytes after deleting vertex at {:?}", bytes_flushed, db_path);
        let vertex_keys: Vec<_> = daemon.vertices
            .iter()
            .keys()
            .filter_map(|k| k.ok())
            .map(|k| String::from_utf8_lossy(&k).to_string())
            .collect();
        info!("Current vertex keys at {:?}: {:?}", db_path, vertex_keys);
        Ok(())
    }

    async fn create_edge(&self, edge: Edge) -> GraphResult<()> {
        let pool = self.pool.lock().await;
        let daemon = pool.leader_daemon().await?;
        let db_path = daemon.db_path();
        info!("Creating edge at path {:?}", db_path);
        timeout(Duration::from_secs(5), pool.create_edge(edge, None)).await
            .map_err(|_| GraphError::StorageError("Timeout during create_edge".to_string()))??;
        let bytes_flushed = timeout(Duration::from_secs(10), daemon.db.flush_async()).await
            .map_err(|_| GraphError::StorageError("Timeout flushing after create_edge".to_string()))?
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        info!("Flushed {} bytes after creating edge at {:?}", bytes_flushed, db_path);
        let edge_keys: Vec<_> = daemon.edges
            .iter()
            .keys()
            .filter_map(|k| k.ok())
            .map(|k| String::from_utf8_lossy(&k).to_string())
            .collect();
        info!("Current edge keys at {:?}: {:?}", db_path, edge_keys);
        Ok(())
    }

    async fn get_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<Option<Edge>> {
        let pool = self.pool.lock().await;
        let daemon = pool.leader_daemon().await?;
        let db_path = daemon.db_path();
        info!("Retrieving edge ({}, {}, {}) from path {:?}", outbound_id, edge_type, inbound_id, db_path);
        let edge = timeout(Duration::from_secs(5), pool.get_edge(outbound_id, edge_type, inbound_id, None)).await
            .map_err(|_| GraphError::StorageError("Timeout during get_edge".to_string()))??;
        let edge_keys: Vec<_> = daemon.edges
            .iter()
            .keys()
            .filter_map(|k| k.ok())
            .map(|k| String::from_utf8_lossy(&k).to_string())
            .collect();
        info!("Current edge keys at {:?}: {:?}", db_path, edge_keys);
        Ok(edge)
    }

    async fn update_edge(&self, edge: Edge) -> GraphResult<()> {
        let pool = self.pool.lock().await;
        let daemon = pool.leader_daemon().await?;
        let db_path = daemon.db_path();
        info!("Updating edge at path {:?}", db_path);
        timeout(Duration::from_secs(5), pool.update_edge(edge, None)).await
            .map_err(|_| GraphError::StorageError("Timeout during update_edge".to_string()))??;
        let bytes_flushed = timeout(Duration::from_secs(10), daemon.db.flush_async()).await
            .map_err(|_| GraphError::StorageError("Timeout flushing after update_edge".to_string()))?
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        info!("Flushed {} bytes after updating edge at {:?}", bytes_flushed, db_path);
        let edge_keys: Vec<_> = daemon.edges
            .iter()
            .keys()
            .filter_map(|k| k.ok())
            .map(|k| String::from_utf8_lossy(&k).to_string())
            .collect();
        info!("Current edge keys at {:?}: {:?}", db_path, edge_keys);
        Ok(())
    }

    async fn delete_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<()> {
        let pool = self.pool.lock().await;
        let daemon = pool.leader_daemon().await?;
        let db_path = daemon.db_path();
        info!("Deleting edge ({}, {}, {}) from path {:?}", outbound_id, edge_type, inbound_id, db_path);
        timeout(Duration::from_secs(5), pool.delete_edge(outbound_id, edge_type, inbound_id, None)).await
            .map_err(|_| GraphError::StorageError("Timeout during delete_edge".to_string()))??;
        let bytes_flushed = timeout(Duration::from_secs(10), daemon.db.flush_async()).await
            .map_err(|_| GraphError::StorageError("Timeout flushing after delete_edge".to_string()))?
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        info!("Flushed {} bytes after deleting edge at {:?}", bytes_flushed, db_path);
        let edge_keys: Vec<_> = daemon.edges
            .iter()
            .keys()
            .filter_map(|k| k.ok())
            .map(|k| String::from_utf8_lossy(&k).to_string())
            .collect();
        info!("Current edge keys at {:?}: {:?}", db_path, edge_keys);
        Ok(())
    }

    async fn close(&self) -> GraphResult<()> {
        let pool = self.pool.lock().await;
        info!("Closing SledStorage pool");
        let futures = pool.daemons.values().map(|daemon| async {
            let db_path = daemon.db_path();
            match timeout(Duration::from_secs(10), daemon.db.flush_async()).await {
                Ok(Ok(bytes_flushed)) => {
                    info!("Flushed {} bytes during close at {:?}", bytes_flushed, db_path);
                    Ok(())
                }
                Ok(Err(e)) => {
                    error!("Failed to flush daemon at {:?}: {}", db_path, e);
                    Err(GraphError::StorageError(e.to_string()))
                }
                Err(_) => {
                    error!("Timeout flushing daemon at {:?}", db_path);
                    Err(GraphError::StorageError(format!("Timeout flushing daemon at {:?}", db_path)))
                }
            }
        });
        let results = join_all(futures).await;
        let errors: Vec<_> = results.into_iter().filter_map(|r| r.err()).collect();
        if !errors.is_empty() {
            error!("Errors during close: {:?}", errors);
            return Err(GraphError::StorageError(format!("Close errors: {:?}", errors)));
        }

        match timeout(Duration::from_secs(10), pool.close(None)).await {
            Ok(Ok(_)) => {
                info!("Successfully closed SledStorage pool");
                Ok(())
            }
            Ok(Err(e)) => {
                error!("Failed to close SledStorage pool: {}", e);
                Err(e)
            }
            Err(_) => {
                error!("Timeout closing SledStorage pool");
                Err(GraphError::StorageError("Timeout closing SledStorage pool".to_string()))
            }
        }
    }

    async fn start(&self) -> Result<(), GraphError> {
        info!("Starting SledStorage");
        Ok(())
    }

    async fn stop(&self) -> Result<(), GraphError> {
        info!("Stopping SledStorage");
        self.close().await
    }

    fn get_type(&self) -> &'static str {
        "sled"
    }

    async fn is_running(&self) -> bool {
        let pool = self.pool.lock().await;
        let daemon_count = pool.daemons.len();
        info!("Checking running status for {} daemons", daemon_count);
        let futures = pool.daemons.values().map(|daemon| async {
            timeout(Duration::from_secs(2), daemon.is_running()).await
                .map_err(|_| false)
                .unwrap_or(false)
        });
        let results = join_all(futures).await;
        let is_running = results.iter().any(|&r| r);
        info!("SledStorage running status: {}, daemon states: {:?}", is_running, results);
        is_running
    }

    async fn query(&self, _query_string: &str) -> Result<Value, GraphError> {
        info!("Executing query on SledStorage (returning null as not implemented)");
        Ok(Value::Null)
    }

    async fn get_all_vertices(&self) -> Result<Vec<Vertex>, GraphError> {
        let pool = self.pool.lock().await;
        let daemon = pool.leader_daemon().await?;
        let db_path = daemon.db_path();
        info!("Retrieving all vertices from path {:?}", db_path);
        let mut vertices = Vec::new();
        for result in daemon.vertices.iter() {
            let (_k, v) = result.map_err(|e| GraphError::StorageError(e.to_string()))?;
            vertices.push(deserialize_vertex(&v)?);
        }
        info!("Retrieved {} vertices from path {:?}", vertices.len(), db_path);
        Ok(vertices)
    }

    async fn get_all_edges(&self) -> Result<Vec<Edge>, GraphError> {
        let pool = self.pool.lock().await;
        let daemon = pool.leader_daemon().await?;
        let db_path = daemon.db_path();
        info!("Retrieving all edges from path {:?}", db_path);
        let mut edges = Vec::new();
        for result in daemon.edges.iter() {
            let (_k, v) = result.map_err(|e| GraphError::StorageError(e.to_string()))?;
            edges.push(deserialize_edge(&v)?);
        }
        info!("Retrieved {} edges from path {:?}", edges.len(), db_path);
        Ok(edges)
    }

    async fn clear_data(&self) -> Result<(), GraphError> {
        let pool = self.pool.lock().await;
        let daemon = pool.leader_daemon().await?;
        let db_path = daemon.db_path();
        info!("Clearing all data from path {:?}", db_path);
        timeout(Duration::from_secs(5), async {
            daemon.vertices.clear().map_err(|e| GraphError::StorageError(e.to_string()))?;
            daemon.edges.clear().map_err(|e| GraphError::StorageError(e.to_string()))?;
            daemon.kv_pairs.clear().map_err(|e| GraphError::StorageError(e.to_string()))?;
            Ok::<(), GraphError>(())
        }).await
            .map_err(|_| GraphError::StorageError("Timeout during clear_data".to_string()))??;
        let bytes_flushed = timeout(Duration::from_secs(10), daemon.db.flush_async()).await
            .map_err(|_| GraphError::StorageError("Timeout flushing after clear_data".to_string()))?
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        info!("Flushed {} bytes after clearing data at {:?}", bytes_flushed, db_path);
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

// Helper function to select an available port from cluster_range
pub async fn select_available_port(storage_config: &StorageConfig, preferred_port: u16) -> GraphResult<u16> {
    let cluster_ports = parse_cluster_range(&storage_config.cluster_range)?;
    let current_pid = process::id();
    
    // Validate preferred_port against cluster_range
    if !cluster_ports.contains(&preferred_port) {
        warn!("Preferred port {} is outside cluster range {:?}", preferred_port, storage_config.cluster_range);
        // Try to find an available port in cluster range
        for port in cluster_ports {
            if GLOBAL_DAEMON_REGISTRY.get_daemon_metadata(port).await?.is_none() {
                debug!("Selected available port {} from cluster range", port);
                return Ok(port);
            }
            if let Some(daemon_metadata) = GLOBAL_DAEMON_REGISTRY.get_daemon_metadata(port).await? {
                if daemon_metadata.pid == current_pid {
                    debug!("Port {} is CLI-owned (PID {}). Reusing it.", port, current_pid);
                    return Ok(port);
                }
            }
        }
        error!("No available ports in cluster range {:?}", storage_config.cluster_range);
        return Err(GraphError::StorageError(format!(
            "No available ports in cluster range {:?}", storage_config.cluster_range
        )));
    }
    
    // Check if preferred_port is available or CLI-owned
    if let Some(daemon_metadata) = GLOBAL_DAEMON_REGISTRY.get_daemon_metadata(preferred_port).await? {
        if daemon_metadata.pid == current_pid {
            debug!("Preferred port {} is CLI-owned (PID {}). Reusing it.", preferred_port, current_pid);
            return Ok(preferred_port);
        } else {
            warn!("Preferred port {} is in use by PID {}", preferred_port, daemon_metadata.pid);
        }
    } else {
        debug!("Preferred port {} is available.", preferred_port);
        return Ok(preferred_port);
    }
    
    // Try other ports in cluster_range
    for port in cluster_ports {
        if port == preferred_port {
            continue; // Already checked
        }
        if let Some(daemon_metadata) = GLOBAL_DAEMON_REGISTRY.get_daemon_metadata(port).await? {
            if daemon_metadata.pid == current_pid {
                debug!("Port {} is CLI-owned (PID {}). Reusing it.", port, current_pid);
                return Ok(port);
            }
        } else {
            debug!("Selected available port {} from cluster range", port);
            return Ok(port);
        }
    }
    
    error!("No available ports in cluster range {:?}", storage_config.cluster_range);
    Err(GraphError::StorageError(format!(
        "No available ports in cluster range {:?}", storage_config.cluster_range
    )))
}
