use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use sled::{Config, Db, Tree};
use tokio::sync::{Mutex as TokioMutex, RwLock};
use tokio::time::{Duration, timeout};
use tokio::fs;
use log::{info, debug, warn, error};
use crate::storage_engine::config::{SledConfig, SledDaemon, SledDaemonPool, SledStorage, StorageConfig,
                                    DAEMON_REGISTRY_DB_PATH};
use crate::storage_engine::storage_utils::{
    create_edge_key, deserialize_edge, deserialize_vertex, serialize_edge, serialize_vertex,
};
use models::{Edge, Identifier, Vertex};
use models::errors::{GraphError, GraphResult};
use serde_json::Value;
use crate::daemon_registry::{GLOBAL_DAEMON_REGISTRY, DaemonMetadata};
use std::time::{SystemTime, UNIX_EPOCH};
use futures::future::join_all;

#[cfg(feature = "with-openraft-sled")]
use {
    async_trait::async_trait,
    openraft::{Config as RaftConfig, NodeId, Raft, RaftNetwork, RaftStorage, BasicNode},
    openraft_sled::SledRaftStorage,
    tokio::net::TcpStream,
    tokio::io::{AsyncReadExt, AsyncWriteExt},
};

impl SledDaemon {
    pub async fn new(config: &SledConfig) -> GraphResult<Self> {
        println!("===> TRYING TO INITIALIZE POOL");
        let db_path = config.path.clone();
        info!("Initializing SledDaemon with path {:?}", db_path);
        if !db_path.exists() {
            tokio::fs::create_dir_all(&db_path)
                .await
                .map_err(|e| GraphError::Io(e))?;
        }

        let lock_path = db_path.join("db");
        debug!("Checking for lock file at {:?}", lock_path);
        if lock_path.exists() {
            warn!("Lock file found at {:?}", lock_path);
            #[cfg(unix)]
            {
                let output = tokio::process::Command::new("lsof")
                    .arg("-t")
                    .arg(lock_path.to_str().ok_or_else(|| {
                        GraphError::StorageError("Invalid lock file path".to_string())
                    })?)
                    .output()
                    .await;
                let lsof_output = match output {
                    Ok(out) => String::from_utf8_lossy(&out.stdout).to_string(),
                    Err(e) => {
                        warn!("Failed to run lsof on {:?}: {}", lock_path, e);
                        String::new()
                    }
                };
                let pids: Vec<u32> = lsof_output.lines().filter_map(|pid| pid.trim().parse().ok()).collect();
                if !pids.is_empty() {
                    warn!("Processes holding lock on {:?}: {:?}", lock_path, pids);
                    for pid in pids {
                        if pid != std::process::id() {
                            warn!("Attempting to send SIGTERM to process {} holding lock", pid);
                            if let Err(e) = tokio::process::Command::new("kill")
                                .arg("-TERM")
                                .arg(pid.to_string())
                                .status()
                                .await
                            {
                                warn!("Failed to send SIGTERM to process {}: {}", pid, e);
                            }
                            tokio::time::sleep(Duration::from_millis(1000)).await;
                        }
                    }
                }
            }
        }

        SledStorage::force_unlock(&db_path).await?;

        const MAX_RETRIES: u32 = 5;
        const BASE_DELAY_MS: u64 = 1000;
        let mut attempt = 0;
        let db = loop {
            debug!("Attempt {} to open Sled DB at {:?}", attempt + 1, db_path);
            match Config::new()
                .path(&db_path)
                .use_compression(config.use_compression)
                .cache_capacity(config.cache_capacity.unwrap_or(1024 * 1024 * 1024))
                .flush_every_ms(Some(100))
                .open()
            {
                Ok(db) => break db,
                Err(e) if e.to_string().contains("WouldBlock") && attempt < MAX_RETRIES => {
                    warn!("Failed to open Sled DB on attempt {}: {}. Retrying.", attempt + 1, e);
                    SledStorage::force_unlock(&db_path).await?;
                    tokio::time::sleep(Duration::from_millis(BASE_DELAY_MS * (attempt as u64 + 1))).await;
                    attempt += 1;
                }
                Err(e) => {
                    error!("Failed to open Sled DB at {:?}: {}. Giving up after {} attempts.", db_path, e, attempt + 1);
                    return Err(GraphError::StorageError(format!(
                        "Failed to open Sled DB at {:?}: {}. Another process may be holding the lock.",
                        db_path, e
                    )));
                }
            }
        };

        let vertices = db
            .open_tree("vertices")
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        let edges = db
            .open_tree("edges")
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        let kv_pairs = db
            .open_tree("kv_pairs")
            .map_err(|e| GraphError::StorageError(e.to_string()))?;

        let kv_keys: Vec<_> = kv_pairs.iter().keys().filter_map(|k| k.ok()).collect();
        let vertex_keys: Vec<_> = vertices.iter().keys().filter_map(|k| k.ok()).collect();
        let edge_keys: Vec<_> = edges.iter().keys().filter_map(|k| k.ok()).collect();
        info!("Initial kv_pairs keys at {:?}: {:?}", db_path, kv_keys);
        info!("Initial vertices keys at {:?}: {:?}", db_path, vertex_keys);
        info!("Initial edges keys at {:?}: {:?}", db_path, edge_keys);

        #[cfg(feature = "with-openraft-sled")]
        let (raft, raft_storage) = {
            let raft_db_path = db_path.join("raft");
            if !raft_db_path.exists() {
                tokio::fs::create_dir_all(&raft_db_path)
                    .await
                    .map_err(|e| GraphError::Io(e))?;
            }
            let mut attempt = 0;
            let raft_storage = loop {
                match timeout(Duration::from_secs(5), SledRaftStorage::new(&raft_db_path)).await {
                    Ok(Ok(storage)) => break storage,
                    Ok(Err(e)) => {
                        warn!("Failed to initialize Raft storage on attempt {}: {}. Retrying.", attempt + 1, e);
                        tokio::time::sleep(Duration::from_millis(BASE_DELAY_MS * (attempt as u64 + 1))).await;
                        attempt += 1;
                        if attempt >= MAX_RETRIES {
                            error!("Failed to initialize Raft storage at {:?} after {} attempts.", raft_db_path, MAX_RETRIES);
                            return Err(GraphError::StorageError(format!(
                                "Failed to initialize Raft storage at {:?} after {} attempts: {}", raft_db_path, MAX_RETRIES, e
                            )));
                        }
                    }
                    Err(_) => {
                        warn!("Timeout initializing Raft storage at {:?} on attempt {}. Retrying.", raft_db_path, attempt + 1);
                        tokio::time::sleep(Duration::from_millis(BASE_DELAY_MS * (attempt as u64 + 1))).await;
                        attempt += 1;
                        if attempt >= MAX_RETRIES {
                            error!("Timeout initializing Raft storage at {:?} after {} attempts.", raft_db_path, MAX_RETRIES);
                            return Err(GraphError::StorageError(format!(
                                "Timeout initializing Raft storage at {:?} after {} attempts.", raft_db_path, MAX_RETRIES
                            )));
                        }
                    }
                }
            };
            let raft_config = RaftConfig {
                cluster_name: "graphdb-cluster".to_string(),
                heartbeat_interval: 250,
                election_timeout_min: 1000,
                election_timeout_max: 2000,
                ..Default::default()
            };
            let port = config.port.ok_or_else(|| {
                GraphError::ConfigurationError("No port specified in SledConfig".to_string())
            })?;
            let raft = Raft::new(
                port as u64,
                Arc::new(raft_storage.clone()),
                Arc::new(RaftTcpNetwork {}),
                raft_config,
            )
            .await
            .map_err(|e| GraphError::StorageError(format!("Failed to initialize Raft: {}", e)))?;
            (Arc::new(raft), Arc::new(raft_storage))
        };

        let port = config.port.ok_or_else(|| {
            GraphError::ConfigurationError("No port specified in SledConfig".to_string())
        })?;
        let daemon = Self {
            port,
            db_path,
            db: Arc::new(db),
            vertices,
            edges,
            kv_pairs,
            running: Arc::new(TokioMutex::new(true)),
            #[cfg(feature = "with-openraft-sled")]
            raft_storage,
            #[cfg(feature = "with-openraft-sled")]
            raft,
            #[cfg(feature = "with-openraft-sled")]
            node_id: port as u64,
        };

        #[cfg(unix)]
        {
            let daemon_clone = Arc::new(daemon.clone());
            tokio::spawn(async move {
                let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("Failed to register SIGTERM handler");
                while sigterm.recv().await.is_some() {
                    warn!("Received SIGTERM in SledDaemon at {:?}", chrono::Local::now());
                    if let Err(e) = daemon_clone.close().await {
                        error!("Failed to close SledDaemon on SIGTERM: {}", e);
                    } else {
                        info!("SledDaemon shutdown complete");
                    }
                    if let Err(e) = SledStorage::force_unlock(&daemon_clone.db_path).await {
                        error!("Failed to clean up lock file at {:?} after SIGTERM: {}", daemon_clone.db_path, e);
                    } else {
                        info!("Cleaned up lock file at {:?}", daemon_clone.db_path);
                    }
                }
            });
        }

        Ok(daemon)
    }

    pub async fn is_running(&self) -> bool {
        *self.running.lock().await
    }

    pub async fn close(&self) -> GraphResult<()> {
        info!("Closing SledDaemon at path {:?}", self.db_path);
        let db_flush = self.db.flush_async()
            .await
            .map_err(|e| GraphError::StorageError(format!("Failed to flush Sled DB: {}", e)))?;
        let vertices_flush = self.vertices.flush_async()
            .await
            .map_err(|e| GraphError::StorageError(format!("Failed to flush vertices tree: {}", e)))?;
        let edges_flush = self.edges.flush_async()
            .await
            .map_err(|e| GraphError::StorageError(format!("Failed to flush edges tree: {}", e)))?;
        let kv_pairs_flush = self.kv_pairs.flush_async()
            .await
            .map_err(|e| GraphError::StorageError(format!("Failed to flush kv_pairs tree: {}", e)))?;
        info!("Flushed SledDaemon at {:?}: db={} bytes, vertices={} bytes, edges={} bytes, kv_pairs={} bytes",
            self.db_path, db_flush, vertices_flush, edges_flush, kv_pairs_flush);
        *self.running.lock().await = false;
        Ok(())
    }

    pub fn db_path(&self) -> PathBuf {
        self.db_path.clone()
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    #[cfg(feature = "with-openraft-sled")]
    pub async fn is_leader(&self) -> GraphResult<bool> {
        let metrics = self.raft.metrics().await;
        let is_leader = matches!(metrics.raft_state, openraft::RaftState::Leader);
        info!("Checking Raft leader status for node {} at path {:?}", self.node_id, self.db_path);
        Ok(is_leader)
    }

    async fn ensure_write_access(&self) -> GraphResult<()> {
        if !self.is_running().await {
            return Err(GraphError::StorageError(format!("Daemon at path {:?} is not running", self.db_path)));
        }
        #[cfg(feature = "with-openraft-sled")]
        {
            if !self.is_leader().await? {
                return Err(GraphError::StorageError(
                    format!("Node {} at path {:?} is not Raft leader, write access denied", self.node_id, self.db_path)
                ));
            }
        }
        Ok(())
    }

    pub async fn insert(&self, key: &[u8], value: &[u8]) -> GraphResult<()> {
        self.ensure_write_access().await?;
        info!("Inserting key into kv_pairs at path {:?}", self.db_path);
        timeout(Duration::from_secs(5), async {
            self.kv_pairs
                .insert(key, value)
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
            let bytes_flushed = self.db
                .flush_async()
                .await
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
            let keys: Vec<_> = self.kv_pairs
                .iter()
                .keys()
                .filter_map(|k| k.ok())
                .collect();
            info!("Flushed {} bytes after insert at {:?}, current kv_pairs keys: {:?}", bytes_flushed, self.db_path, keys);
            #[cfg(feature = "with-openraft-sled")]
            {
                let request = openraft::raft::ClientWriteRequest::new(
                    openraft::EntryPayload::AppWrite {
                        key: key.to_vec(),
                        value: value.to_vec(),
                    }
                );
                self.raft.client_write(request).await
                    .map_err(|e| GraphError::StorageError(format!("Raft write failed: {}", e)))?;
                info!("Raft write replicated for key at {:?}", self.db_path);
            }
            Ok(())
        })
        .await
        .map_err(|_| GraphError::StorageError("Timeout during insert".to_string()))?
    }

    pub async fn retrieve(&self, key: &[u8]) -> GraphResult<Option<Vec<u8>>> {
        if !self.is_running().await {
            return Err(GraphError::StorageError(format!("Daemon at path {:?} is not running", self.db_path)));
        }
        info!("Retrieving key from kv_pairs at path {:?}", self.db_path);
        let value = timeout(Duration::from_secs(5), async {
            self.kv_pairs
                .get(key)
                .map_err(|e| GraphError::StorageError(e.to_string()))
                .ok()?
                .map(|v| v.to_vec())
        })
        .await
        .map_err(|_| GraphError::StorageError("Timeout during retrieve".to_string()))?;
        let keys: Vec<_> = self.kv_pairs
            .iter()
            .keys()
            .filter_map(|k| k.ok())
            .collect();
        info!("Current kv_pairs keys at {:?}: {:?}", self.db_path, keys);
        Ok(value)
    }

    pub async fn delete(&self, key: &[u8]) -> GraphResult<()> {
        self.ensure_write_access().await?;
        info!("Deleting key from kv_pairs at path {:?}", self.db_path);
        timeout(Duration::from_secs(5), async {
            self.kv_pairs
                .remove(key)
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
            let bytes_flushed = self.db
                .flush_async()
                .await
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
            let keys: Vec<_> = self.kv_pairs
                .iter()
                .keys()
                .filter_map(|k| k.ok())
                .collect();
            info!("Flushed {} bytes after delete at {:?}, current kv_pairs keys: {:?}", bytes_flushed, self.db_path, keys);
            #[cfg(feature = "with-openraft-sled")]
            {
                let request = openraft::raft::ClientWriteRequest::new(
                    openraft::EntryPayload::AppWrite {
                        key: key.to_vec(),
                        value: vec![],
                    }
                );
                self.raft.client_write(request).await
                    .map_err(|e| GraphError::StorageError(format!("Raft delete failed: {}", e)))?;
                info!("Raft delete replicated for key at {:?}", self.db_path);
            }
            Ok(())
        })
        .await
        .map_err(|_| GraphError::StorageError("Timeout during delete".to_string()))?
    }

    pub async fn create_vertex(&self, vertex: &Vertex) -> GraphResult<()> {
        self.ensure_write_access().await?;
        let key = vertex.id.0.as_bytes();
        let value = serialize_vertex(vertex)?;
        info!("Creating vertex with id {} at path {:?}", vertex.id, self.db_path);
        timeout(Duration::from_secs(5), async {
            self.vertices
                .insert(key, value)
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
            let bytes_flushed = self.db
                .flush_async()
                .await
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
            let vertex_keys: Vec<_> = self.vertices
                .iter()
                .keys()
                .filter_map(|k| k.ok())
                .collect();
            info!("Flushed {} bytes after creating vertex at {:?}, current vertices keys: {:?}", bytes_flushed, self.db_path, vertex_keys);
            #[cfg(feature = "with-openraft-sled")]
            {
                let request = openraft::raft::ClientWriteRequest::new(
                    openraft::EntryPayload::AppWrite {
                        key: key.to_vec(),
                        value: vertex.id.0.as_bytes().to_vec(),
                    }
                );
                self.raft.client_write(request).await
                    .map_err(|e| GraphError::StorageError(format!("Raft vertex create failed: {}", e)))?;
                info!("Raft vertex create replicated at {:?}", self.db_path);
            }
            Ok(())
        })
        .await
        .map_err(|_| GraphError::StorageError("Timeout during create_vertex".to_string()))?
    }

    pub async fn get_vertex(&self, id: &uuid::Uuid) -> GraphResult<Option<Vertex>> {
        if !self.is_running().await {
            return Err(GraphError::StorageError(format!("Daemon at path {:?} is not running", self.db_path)));
        }
        let key = id.as_bytes();
        info!("Retrieving vertex with id {} from path {:?}", id, self.db_path);
        let res = timeout(Duration::from_secs(5), async {
            self.vertices
                .get(key)
                .map_err(|e| GraphError::StorageError(e.to_string()))?
                .map(|b| deserialize_vertex(&b))
                .transpose()
        })
        .await
        .map_err(|_| GraphError::StorageError("Timeout during get_vertex".to_string()))??;
        let vertex_keys: Vec<_> = self.vertices
            .iter()
            .keys()
            .filter_map(|k| k.ok())
            .collect();
        info!("Current vertices keys at {:?}: {:?}", self.db_path, vertex_keys);
        Ok(res)
    }

    pub async fn update_vertex(&self, vertex: &Vertex) -> GraphResult<()> {
        self.delete_vertex(&vertex.id.0).await?;
        self.create_vertex(vertex).await
    }

    pub async fn delete_vertex(&self, id: &uuid::Uuid) -> GraphResult<()> {
        self.ensure_write_access().await?;
        let key = id.as_bytes();
        info!("Deleting vertex with id {} from path {:?}", id, self.db_path);
        timeout(Duration::from_secs(5), async {
            self.vertices
                .remove(key)
                .map_err(|e| GraphError::StorageError(e.to_string()))?;

            let mut batch = sled::Batch::default();
            let prefix = id.as_bytes();
            for item in self.edges.iter().keys() {
                let k = item.map_err(|e| GraphError::StorageError(e.to_string()))?;
                if k.starts_with(prefix) {
                    batch.remove(k);
                }
            }
            self.edges
                .apply_batch(batch)
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
            let bytes_flushed = self.db
                .flush_async()
                .await
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
            let vertex_keys: Vec<_> = self.vertices
                .iter()
                .keys()
                .filter_map(|k| k.ok())
                .collect();
            let edge_keys: Vec<_> = self.edges
                .iter()
                .keys()
                .filter_map(|k| k.ok())
                .collect();
            info!("Flushed {} bytes after deleting vertex at {:?}, current vertices keys: {:?}, edges keys: {:?}", bytes_flushed, self.db_path, vertex_keys, edge_keys);
            #[cfg(feature = "with-openraft-sled")]
            {
                let request = openraft::raft::ClientWriteRequest::new(
                    openraft::EntryPayload::AppWrite {
                        key: key.to_vec(),
                        value: vec![],
                    }
                );
                self.raft.client_write(request).await
                    .map_err(|e| GraphError::StorageError(format!("Raft vertex delete failed: {}", e)))?;
                info!("Raft vertex delete replicated at {:?}", self.db_path);
            }
            Ok(())
        })
        .await
        .map_err(|_| GraphError::StorageError("Timeout during delete_vertex".to_string()))?
    }

    pub async fn create_edge(&self, edge: &Edge) -> GraphResult<()> {
        self.ensure_write_access().await?;
        let key = create_edge_key(&edge.outbound_id.into(), &edge.t, &edge.inbound_id.into())?;
        let value = serialize_edge(edge)?;
        info!("Creating edge ({}, {}, {}) at path {:?}", edge.outbound_id, edge.t, edge.inbound_id, self.db_path);
        timeout(Duration::from_secs(5), async {
            self.edges
                .insert(key, value)
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
            let bytes_flushed = self.db
                .flush_async()
                .await
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
            let edge_keys: Vec<_> = self.edges
                .iter()
                .keys()
                .filter_map(|k| k.ok())
                .collect();
            info!("Flushed {} bytes after creating edge at {:?}, current edges keys: {:?}", bytes_flushed, self.db_path, edge_keys);
            #[cfg(feature = "with-openraft-sled")]
            {
                let request = openraft::raft::ClientWriteRequest::new(
                    openraft::EntryPayload::AppWrite {
                        key: key.to_vec(),
                        value: edge.t.to_string().into_bytes(),
                    }
                );
                self.raft.client_write(request).await
                    .map_err(|e| GraphError::StorageError(format!("Raft edge create failed: {}", e)))?;
                info!("Raft edge create replicated at {:?}", self.db_path);
            }
            Ok(())
        })
        .await
        .map_err(|_| GraphError::StorageError("Timeout during create_edge".to_string()))?
    }

    pub async fn get_edge(
        &self,
        outbound_id: &uuid::Uuid,
        edge_type: &Identifier,
        inbound_id: &uuid::Uuid,
    ) -> GraphResult<Option<Edge>> {
        if !self.is_running().await {
            return Err(GraphError::StorageError(format!("Daemon at path {:?} is not running", self.db_path)));
        }
        let key = create_edge_key(&(*outbound_id).into(), edge_type, &(*inbound_id).into())?;
        info!("Retrieving edge ({}, {}, {}) from path {:?}", outbound_id, edge_type, inbound_id, self.db_path);
        let res = timeout(Duration::from_secs(5), async {
            self.edges
                .get(&key)
                .map_err(|e| GraphError::StorageError(e.to_string()))?
                .map(|b| deserialize_edge(&b))
                .transpose()
        })
        .await
        .map_err(|_| GraphError::StorageError("Timeout during get_edge".to_string()))??;
        let edge_keys: Vec<_> = self.edges
            .iter()
            .keys()
            .filter_map(|k| k.ok())
            .collect();
        info!("Current edges keys at {:?}: {:?}", self.db_path, edge_keys);
        Ok(res)
    }

    pub async fn update_edge(&self, edge: &Edge) -> GraphResult<()> {
        self.create_edge(edge).await
    }

    pub async fn delete_edge(
        &self,
        outbound_id: &uuid::Uuid,
        edge_type: &Identifier,
        inbound_id: &uuid::Uuid,
    ) -> GraphResult<()> {
        self.ensure_write_access().await?;
        let key = create_edge_key(&(*outbound_id).into(), edge_type, &(*inbound_id).into())?;
        info!("Deleting edge ({}, {}, {}) from path {:?}", outbound_id, edge_type, inbound_id, self.db_path);
        timeout(Duration::from_secs(5), async {
            self.edges
                .remove(key)
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
            let bytes_flushed = self.db
                .flush_async()
                .await
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
            let edge_keys: Vec<_> = self.edges
                .iter()
                .keys()
                .filter_map(|k| k.ok())
                .collect();
            info!("Flushed {} bytes after deleting edge at {:?}, current edges keys: {:?}", bytes_flushed, self.db_path, edge_keys);
            #[cfg(feature = "with-openraft-sled")]
            {
                let request = openraft::raft::ClientWriteRequest::new(
                    openraft::EntryPayload::AppWrite {
                        key: key.to_vec(),
                        value: vec![],
                    }
                );
                self.raft.client_write(request).await
                    .map_err(|e| GraphError::StorageError(format!("Raft edge delete failed: {}", e)))?;
                info!("Raft edge delete replicated at {:?}", self.db_path);
            }
            Ok(())
        })
        .await
        .map_err(|_| GraphError::StorageError("Timeout during delete_edge".to_string()))?
    }

    pub async fn force_reset(&self) -> GraphResult<()> {
        info!("Resetting SledDaemon at path {:?}", self.db_path);
        timeout(Duration::from_secs(5), async {
            self.db
                .clear()
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
            let bytes_flushed = self.db
                .flush_async()
                .await
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
            info!("Flushed {} bytes after resetting daemon at {:?}", bytes_flushed, self.db_path);
            #[cfg(feature = "with-openraft-sled")]
            {
                self.raft_storage
                    .reset()
                    .await
                    .map_err(|e| GraphError::StorageError(format!("Raft reset failed: {}", e)))?;
                info!("Raft storage reset at {:?}", self.db_path);
            }
            Ok(())
        })
        .await
        .map_err(|_| GraphError::StorageError("Timeout during force_reset".to_string()))?
    }

    pub async fn force_unlock(&self) -> GraphResult<()> {
        SledStorage::force_unlock(&self.db_path).await
    }

    pub async fn force_unlock_path(path: &Path) -> GraphResult<()> {
        SledStorage::force_unlock(path).await
    }
}

impl SledDaemonPool {
    pub fn new() -> Self {
        Self {
            daemons: HashMap::new(),
            registry: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn add_daemon(&mut self, storage_config: &StorageConfig, port: u16, config: &SledConfig) -> GraphResult<()> {
        let mut port_config = config.clone();
        port_config.port = Some(port);
        if self.daemons.contains_key(&port) {
            return Err(GraphError::StorageError(format!("Daemon already exists on port {}", port)));
        }
        if GLOBAL_DAEMON_REGISTRY.get_daemon_metadata(port).await?.is_some() {
            warn!("Clearing stale daemon registry entry for port {}", port);
            GLOBAL_DAEMON_REGISTRY.unregister_daemon(port).await?;
        }
        let daemon = Arc::new(SledDaemon::new(&port_config).await?);
        self.daemons.insert(port, daemon.clone());
        let metadata = DaemonMetadata {
            service_type: "storage".to_string(),
            port,
            pid: std::process::id(),
            ip_address: "127.0.0.1".to_string(),
            data_dir: Some(port_config.path.clone()),
            config_path: Some(storage_config.config_root_directory.join("storage_config.yaml")),
            engine_type: Some("sled".to_string()),
            last_seen_nanos: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos() as i64)
                .unwrap_or(0),
        };
        self.registry.write().await.insert(port, metadata.clone());
        GLOBAL_DAEMON_REGISTRY
            .register_daemon(metadata)
            .await
            .map_err(|e| GraphError::StorageError(format!("Failed to register daemon in GLOBAL_DAEMON_REGISTRY: {}", e)))?;
        info!("Added daemon for port {} at path {:?}", port, port_config.path);
        Ok(())
    }

    pub async fn initialize_cluster(&mut self, storage_config: &StorageConfig, config: &SledConfig) -> GraphResult<()> {
        debug!("Initializing cluster with use_raft_for_scale: {}", storage_config.use_raft_for_scale);

        let registry_path = Path::new(DAEMON_REGISTRY_DB_PATH);
        if let Some(parent) = registry_path.parent() {
            debug!("Ensuring daemon registry directory exists: {:?}", parent);
            fs::create_dir_all(parent)
                .await
                .map_err(|e| {
                    error!("Failed to create daemon registry directory {:?}: {}", parent, e);
                    GraphError::Io(e)
                })?;
        }

        let range = storage_config.cluster_range.as_str();
        let (start_port, end_port) = if range.contains('-') {
            let parts: Vec<&str> = range.split('-').collect();
            if parts.len() != 2 {
                return Err(GraphError::StorageError(format!("Invalid cluster range format: {}", range)));
            }
            let start = parts[0].parse::<u16>().map_err(|_| {
                GraphError::StorageError("Invalid start port".to_string())
            })?;
            let end = parts[1].parse::<u16>().map_err(|_| {
                GraphError::StorageError("Invalid end port".to_string())
            })?;
            (start, end)
        } else {
            let port = range.parse::<u16>().map_err(|_| {
                GraphError::StorageError("Invalid single port".to_string())
            })?;
            (port, port)
        };

        if end_port < start_port {
            return Err(GraphError::StorageError(format!("Invalid port range: {}-{}", start_port, end_port)));
        }
        if end_port - start_port > 100 {
            return Err(GraphError::StorageError(format!("Cluster size too large: {}-{}", start_port, end_port)));
        }

        let cluster_ports = start_port..=end_port;
        for port in cluster_ports.clone() {
            if GLOBAL_DAEMON_REGISTRY.get_daemon_metadata(port).await?.is_some() {
                warn!("Clearing stale daemon registry entry for port {}", port);
                GLOBAL_DAEMON_REGISTRY.unregister_daemon(port).await?;
            }
        }

        if !storage_config.use_raft_for_scale {
            info!("use_raft_for_scale is false, selecting port within cluster range {}-{}", start_port, end_port);
            let port = crate::storage_engine::sled_storage::select_available_port(storage_config, storage_config.default_port).await?;
            if port < start_port || port > end_port {
                return Err(GraphError::StorageError(format!(
                    "Selected port {} is outside cluster range {}-{}", port, start_port, end_port
                )));
            }
            return self.add_daemon(storage_config, port, config).await;
        }

        info!("Initializing Raft cluster for ports {}-{}", start_port, end_port);
        #[cfg(feature = "with-openraft-sled")]
        let mut nodes: HashSet<BasicNode> = HashSet::new();
        #[cfg(feature = "with-openraft-sled")]
        for port in cluster_ports.clone() {
            nodes.insert(BasicNode {
                addr: format!("127.0.0.1:{}", port),
            });
        }

        for port in cluster_ports {
            let mut port_config = config.clone();
            port_config.port = Some(port);
            let daemon = Arc::new(SledDaemon::new(&port_config).await?);
            self.daemons.insert(port, daemon.clone());
            let metadata = DaemonMetadata {
                service_type: "storage".to_string(),
                port,
                pid: std::process::id(),
                ip_address: "127.0.0.1".to_string(),
                data_dir: Some(port_config.path.clone()),
                config_path: Some(storage_config.config_root_directory.join("storage_config.yaml")),
                engine_type: Some("sled".to_string()),
                last_seen_nanos: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_nanos() as i64)
                    .unwrap_or(0),
            };
            self.registry.write().await.insert(port, metadata.clone());
            GLOBAL_DAEMON_REGISTRY
                .register_daemon(metadata)
                .await
                .map_err(|e| {
                    error!("Failed to register daemon on port {}: {}", port, e);
                    GraphError::StorageError(format!("Failed to register daemon on port {}: {}", port, e))
                })?;

            #[cfg(feature = "with-openraft-sled")]
            {
                let node_id = port as u64;
                let raft = &daemon.raft;
                let initial_nodes = nodes
                    .iter()
                    .map(|node| (node_id, node.clone()))
                    .collect::<HashMap<_, _>>();
                raft.initialize(initial_nodes).await
                    .map_err(|e| {
                        error!("Failed to initialize Raft for node {}: {}", node_id, e);
                        GraphError::StorageError(format!("Failed to initialize Raft for node {}: {}", node_id, e))
                    })?;
                info!("Initialized Raft for node {} on port {}", node_id, port);
            }
        }

        info!("Successfully initialized cluster with {} daemons", self.daemons.len());
        Ok(())
    }

    pub async fn any_daemon(&self) -> GraphResult<Arc<SledDaemon>> {
        if let Some(daemon) = self.daemons.values().next() {
            info!("Selected daemon on port {} at path {:?}", daemon.port(), daemon.db_path());
            Ok(Arc::clone(daemon))
        } else {
            error!("No daemons available in the pool");
            Err(GraphError::StorageError("No daemons available".to_string()))
        }
    }

    pub async fn close(&self, _port: Option<u16>) -> GraphResult<()> {
        let futures = self.daemons.values().map(|daemon| async {
            let db_path = daemon.db_path();
            match timeout(Duration::from_secs(10), daemon.close()).await {
                Ok(Ok(())) => {
                    info!("Closed daemon at {:?}", db_path);
                    Ok(())
                }
                Ok(Err(e)) => {
                    error!("Failed to close daemon at {:?}: {}", db_path, e);
                    Err(GraphError::StorageError(e.to_string()))
                }
                Err(_) => {
                    error!("Timeout closing daemon at {:?}", db_path);
                    Err(GraphError::StorageError(format!("Timeout closing daemon at {:?}", db_path)))
                }
            }
        });
        let results = join_all(futures).await;
        let errors: Vec<_> = results.into_iter().filter_map(|r| r.err()).collect();
        if !errors.is_empty() {
            error!("Errors during close: {:?}", errors);
            return Err(GraphError::StorageError(format!("Close errors: {:?}", errors)));
        }
        info!("Successfully closed all daemons");
        Ok(())
    }

    pub async fn leader_daemon(&self) -> GraphResult<Arc<SledDaemon>> {
        for daemon in self.daemons.values() {
            #[cfg(feature = "with-openraft-sled")]
            {
                if daemon.is_leader().await? {
                    info!("Selected leader daemon on port {} at path {:?}", daemon.port(), daemon.db_path());
                    return Ok(Arc::clone(daemon));
                }
            }
            #[cfg(not(feature = "with-openraft-sled"))]
            {
                info!("Selected daemon on port {} at path {:?}", daemon.port(), daemon.db_path());
                return Ok(Arc::clone(daemon));
            }
        }
        error!("No leader daemon found in the pool");
        Err(GraphError::StorageError("No leader daemon found".to_string()))
    }

    pub async fn create_edge(&self, edge: Edge, _use_raft: bool) -> GraphResult<()> {
        let daemon = self.leader_daemon().await?;
        timeout(Duration::from_secs(5), daemon.create_edge(&edge)).await
            .map_err(|_| GraphError::StorageError("Timeout during create_edge".to_string()))?
    }

    pub async fn get_edge(
        &self,
        outbound_id: &uuid::Uuid,
        edge_type: &Identifier,
        inbound_id: &uuid::Uuid,
        _use_raft: bool,
    ) -> GraphResult<Option<Edge>> {
        let daemon = self.leader_daemon().await?;
        timeout(Duration::from_secs(5), daemon.get_edge(outbound_id, edge_type, inbound_id)).await
            .map_err(|_| GraphError::StorageError("Timeout during get_edge".to_string()))?
    }

    pub async fn update_edge(&self, edge: Edge, _use_raft: bool) -> GraphResult<()> {
        let daemon = self.leader_daemon().await?;
        timeout(Duration::from_secs(5), daemon.update_edge(&edge)).await
            .map_err(|_| GraphError::StorageError("Timeout during update_edge".to_string()))?
    }

    pub async fn delete_edge(
        &self,
        outbound_id: &uuid::Uuid,
        edge_type: &Identifier,
        inbound_id: &uuid::Uuid,
        _use_raft: bool,
    ) -> GraphResult<()> {
        let daemon = self.leader_daemon().await?;
        timeout(Duration::from_secs(5), daemon.delete_edge(outbound_id, edge_type, inbound_id)).await
            .map_err(|_| GraphError::StorageError("Timeout during delete_edge".to_string()))?
    }

    pub async fn create_vertex(&self, vertex: Vertex, _use_raft: bool) -> GraphResult<()> {
        let daemon = self.leader_daemon().await?;
        timeout(Duration::from_secs(5), daemon.create_vertex(&vertex)).await
            .map_err(|_| GraphError::StorageError("Timeout during create_vertex".to_string()))?
    }

    pub async fn get_vertex(&self, id: &uuid::Uuid, _use_raft: bool) -> GraphResult<Option<Vertex>> {
        let daemon = self.leader_daemon().await?;
        timeout(Duration::from_secs(5), daemon.get_vertex(id)).await
            .map_err(|_| GraphError::StorageError("Timeout during get_vertex".to_string()))?
    }

    pub async fn update_vertex(&self, vertex: Vertex, _use_raft: bool) -> GraphResult<()> {
        let daemon = self.leader_daemon().await?;
        timeout(Duration::from_secs(5), daemon.update_vertex(&vertex)).await
            .map_err(|_| GraphError::StorageError("Timeout during update_vertex".to_string()))?
    }

    pub async fn delete_vertex(&self, id: &uuid::Uuid, _use_raft: bool) -> GraphResult<()> {
        let daemon = self.leader_daemon().await?;
        timeout(Duration::from_secs(5), daemon.delete_vertex(id)).await
            .map_err(|_| GraphError::StorageError("Timeout during delete_vertex".to_string()))?
    }
}

#[cfg(feature = "with-openraft-sled")]
#[async_trait]
impl RaftNetwork<NodeId, BasicNode> for RaftTcpNetwork {
    async fn send_append_entries(
        &self,
        target: NodeId,
        rpc: openraft::raft::AppendEntriesRequest<BasicNode>,
    ) -> Result<openraft::raft::AppendEntriesResponse, openraft::error::RPCError<NodeId, BasicNode>> {
        let addr = format!("127.0.0.1:{}", target);
        let mut stream = TcpStream::connect(&addr).await
            .map_err(|e| openraft::error::RPCError::Network {
                error: openraft::error::NetworkError::new(&e),
                target,
                node: BasicNode { addr: addr.clone() },
            })?;
        let request_data = serde_json::to_vec(&rpc)
            .map_err(|e| openraft::error::RPCError::PayloadTooLarge {
                error: openraft::error::ClientError::new(&e, target, &addr),
            })?;
        stream.write_all(&request_data).await
            .map_err(|e| openraft::error::RPCError::Network {
                error: openraft::error::NetworkError::new(&e),
                target,
                node: BasicNode { addr: addr.clone() },
            })?;
        stream.flush().await
            .map_err(|e| openraft::error::RPCError::Network {
                error: openraft::error::NetworkError::new(&e),
                target,
                node: BasicNode { addr: addr.clone() },
            })?;
        let mut buffer = Vec::new();
        stream.read_to_end(&mut buffer).await
            .map_err(|e| openraft::error::RPCError::Network {
                error: openraft::error::NetworkError::new(&e),
                target,
                node: BasicNode { addr: addr.clone() },
            })?;
        let response: openraft::raft::AppendEntriesResponse = serde_json::from_slice(&buffer)
            .map_err(|e| openraft::error::RPCError::PayloadTooLarge {
                error: openraft::error::ClientError::new(&e, target, &addr),
            })?;
        Ok(response)
    }

    async fn send_install_snapshot(
        &self,
        target: NodeId,
        rpc: openraft::raft::InstallSnapshotRequest<BasicNode>,
    ) -> Result<openraft::raft::InstallSnapshotResponse, openraft::error::RPCError<NodeId, BasicNode>> {
        let addr = format!("127.0.0.1:{}", target);
        let mut stream = TcpStream::connect(&addr).await
            .map_err(|e| openraft::error::RPCError::Network {
                error: openraft::error::NetworkError::new(&e),
                target,
                node: BasicNode { addr: addr.clone() },
            })?;
        let request_data = serde_json::to_vec(&rpc)
            .map_err(|e| openraft::error::RPCError::PayloadTooLarge {
                error: openraft::error::ClientError::new(&e, target, &addr),
            })?;
        stream.write_all(&request_data).await
            .map_err(|e| openraft::error::RPCError::Network {
                error: openraft::error::NetworkError::new(&e),
                target,
                node: BasicNode { addr: addr.clone() },
            })?;
        stream.flush().await
            .map_err(|e| openraft::error::RPCError::Network {
                error: openraft::error::NetworkError::new(&e),
                target,
                node: BasicNode { addr: addr.clone() },
            })?;
        let mut buffer = Vec::new();
        stream.read_to_end(&mut buffer).await
            .map_err(|e| openraft::error::RPCError::Network {
                error: openraft::error::NetworkError::new(&e),
                target,
                node: BasicNode { addr: addr.clone() },
            })?;
        let response: openraft::raft::InstallSnapshotResponse = serde_json::from_slice(&buffer)
            .map_err(|e| openraft::error::RPCError::PayloadTooLarge {
                error: openraft::error::ClientError::new(&e, target, &addr),
            })?;
        Ok(response)
    }

    async fn send_vote(
        &self,
        target: NodeId,
        rpc: openraft::raft::VoteRequest<NodeId>,
    ) -> Result<openraft::raft::VoteResponse<NodeId>, openraft::error::RPCError<NodeId, BasicNode>> {
        let addr = format!("127.0.0.1:{}", target);
        debug!("Sending vote to node {} at {}", target, addr);
        let mut stream = TcpStream::connect(&addr).await
            .map_err(|e| openraft::error::RPCError::Network {
                error: openraft::error::NetworkError::new(&e),
                target,
                node: BasicNode { addr: addr.clone() },
            })?;
        let request_data = serde_json::to_vec(&rpc)
            .map_err(|e| openraft::error::RPCError::PayloadTooLarge {
                error: openraft::error::ClientError::new(&e, target, &addr),
            })?;
        stream.write_all(&request_data).await
            .map_err(|e| openraft::error::RPCError::Network {
                error: openraft::error::NetworkError::new(&e),
                target,
                node: BasicNode { addr: addr.clone() },
            })?;
        stream.flush().await
            .map_err(|e| openraft::error::RPCError::Network {
                error: openraft::error::NetworkError::new(&e),
                target,
                node: BasicNode { addr: addr.clone() },
            })?;
        let mut buffer = Vec::new();
        stream.read_to_end(&mut buffer).await
            .map_err(|e| openraft::error::RPCError::Network {
                error: openraft::error::NetworkError::new(&e),
                target,
                node: BasicNode { addr: addr.clone() },
            })?;
        let response: openraft::raft::VoteResponse<NodeId> = serde_json::from_slice(&buffer)
            .map_err(|e| openraft::error::RPCError::PayloadTooLarge {
                error: openraft::error::ClientError::new(&e, target, &addr),
            })?;
        Ok(response)
    }
}