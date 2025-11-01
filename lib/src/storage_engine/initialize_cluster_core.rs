```rust
    async fn _initialize_cluster_core(
        &mut self,
        storage_config: &StorageConfig,
        config: &RocksDBConfig,
        cli_port: Option<u16>,
        client: Option<(RocksDBClient, Arc<TokioMutex<ZmqSocketWrapper>>)>,
        existing_db: Option<Arc<DB>>,
    ) -> GraphResult<()> {
        println!("===> IN _initialize_cluster_core");
        info!("Starting initialization of RocksDBDaemonPool with config: {:?}", storage_config);

        // Acquire global initialization lock to prevent concurrent initialization
        let _guard = get_cluster_init_lock().await.lock().await;

        let mut initialized = self.initialized.write().await;
        if *initialized {
            warn!("RocksDBDaemonPool already initialized, skipping");
            println!("===> WARNING: ROCKSDB DAEMON POOL ALREADY INITIALIZED, SKIPPING");
            return Ok(());
        }

        const DEFAULT_STORAGE_PORT: u16 = 8049;
        const DEFAULT_DATA_DIRECTORY: &str = "/opt/graphdb/storage_data";
        let intended_port = cli_port.unwrap_or(config.port.unwrap_or(DEFAULT_STORAGE_PORT));
        info!(
            "Intended port: {} (cli_port: {:?}, config.port: {:?}, DEFAULT_STORAGE_PORT: {})",
            intended_port, cli_port, config.port, DEFAULT_STORAGE_PORT
        );
        println!(
            "===> INTENDED PORT: {} (cli_port: {:?}, config.port: {:?}, DEFAULT_STORAGE_PORT: {})",
            intended_port, cli_port, config.port, DEFAULT_STORAGE_PORT
        );

        // Initialize GLOBAL_DB_DAEMON_REGISTRY
        info!("Attempting to initialize GLOBAL_DB_DAEMON_REGISTRY");
        println!("===> ATTEMPTING TO INITIALIZE GLOBAL_DB_DAEMON_REGISTRY");
        let storage_config_arc = Arc::new(storage_config.clone());
        let mut attempts = 0;
        const MAX_REGISTRY_ATTEMPTS: u32 = 5;
        while attempts < MAX_REGISTRY_ATTEMPTS {
            match timeout(
                TokioDuration::from_secs(10),
                GLOBAL_DB_DAEMON_REGISTRY.get_or_init_instance(storage_config_arc.clone()),
            )
            .await
            {
                Ok(Ok(_)) => {
                    info!("Successfully initialized GLOBAL_DB_DAEMON_REGISTRY for RocksDBDaemonPool");
                    println!("===> SUCCESSFULLY INITIALIZED GLOBAL_DB_DAEMON_REGISTRY FOR ROCKSDB DAEMON POOL");
                    break;
                }
                Ok(Err(e)) => {
                    error!("Failed to initialize GLOBAL_DB_DAEMON_REGISTRY: {}", e);
                    println!("===> ERROR: FAILED TO INITIALIZE GLOBAL_DB_DAEMON_REGISTRY: {}", e);
                    return Err(GraphError::StorageError(format!(
                        "Failed to initialize GLOBAL_DB_DAEMON_REGISTRY: {}",
                        e
                    )));
                }
                Err(_) => {
                    warn!(
                        "Timeout initializing GLOBAL_DB_DAEMON_REGISTRY, attempt {}/{}",
                        attempts + 1,
                        MAX_REGISTRY_ATTEMPTS
                    );
                    println!(
                        "===> WARNING: TIMEOUT INITIALIZING GLOBAL_DB_DAEMON_REGISTRY, ATTEMPT {}/{}",
                        attempts + 1,
                        MAX_REGISTRY_ATTEMPTS
                    );
                    attempts += 1;
                    sleep(TokioDuration::from_millis(1000)).await;
                }
            }
        }
        if attempts >= MAX_REGISTRY_ATTEMPTS {
            error!(
                "Failed to initialize GLOBAL_DB_DAEMON_REGISTRY after {} attempts",
                MAX_REGISTRY_ATTEMPTS
            );
            println!(
                "===> ERROR: FAILED TO INITIALIZE GLOBAL_DB_DAEMON_REGISTRY AFTER {} ATTEMPTS",
                MAX_REGISTRY_ATTEMPTS
            );
            return Err(GraphError::StorageError(format!(
                "Failed to initialize GLOBAL_DB_DAEMON_REGISTRY after {} attempts",
                MAX_REGISTRY_ATTEMPTS
            )));
        }

        let daemon_registry = GLOBAL_DAEMON_REGISTRY.get().await;
        let db_registry = GLOBAL_DB_DAEMON_REGISTRY.get().await;
        let mut valid_ports: Vec<u16> = Vec::new();

        // Determine cluster ports
        let cluster_ports: Vec<u16> = if !storage_config.cluster_range.is_empty() {
            let range: Vec<&str> = storage_config.cluster_range.split('-').collect();
            let start: u16 = range[0].parse().unwrap_or(intended_port);
            let end: u16 = range.get(1).and_then(|s| s.parse().ok()).unwrap_or(start);
            (start..=end).collect()
        } else {
            vec![intended_port]
        };

        // Check registry for existing daemons
        let all_daemons = daemon_registry.get_all_daemon_metadata().await?;
        let mut active_ports: Vec<u16> = all_daemons
            .into_iter()
            .filter(|metadata| metadata.service_type == "storage" && metadata.engine_type == Some("rocksdb".to_string()))
            .map(|metadata| metadata.port)
            .collect();

        if !active_ports.contains(&intended_port) {
            info!("Intended port {} not found in registry, adding it", intended_port);
            println!("===> INTENDED PORT {} NOT FOUND IN REGISTRY, ADDING IT", intended_port);
            active_ports.push(intended_port);
        }

        // Process each port (deduplicated)
        let all_ports: Vec<u16> = active_ports.into_iter().collect::<std::collections::HashSet<_>>().into_iter().collect();
        for port in all_ports {
            let is_intended_port = port == intended_port;
            let metadata_option = daemon_registry.get_daemon_metadata(port).await?;

            // Compute database path
            let engine_dir = daemon_api_storage_engine_type_to_string(&storage_config.storage_engine_type);
            let db_path = storage_config
                .data_directory
                .as_ref()
                .unwrap_or(&PathBuf::from(DEFAULT_DATA_DIRECTORY))
                .join(&engine_dir)
                .join(port.to_string());

            if let Some(mut metadata) = metadata_option {
                // Check if daemon is valid (PID exists and process is running)
                let is_pid_valid = {
                    #[cfg(unix)]
                    {
                        use nix::sys::signal;
                        signal::kill(nix::unistd::Pid::from_raw(metadata.pid as i32), None).is_ok()
                    }
                    #[cfg(not(unix))]
                    {
                        std::process::Command::new("ps").arg(metadata.pid.to_string()).status().is_ok()
                    }
                };

                if is_pid_valid {
                    if metadata.zmq_ready {
                        info!("Found valid daemon on port {} with ZMQ ready, reusing", port);
                        println!("===> FOUND VALID DAEMON ON PORT {} WITH ZMQ READY, REUSING", port);
                    } else {
                        info!("Found valid daemon on port {} with ZMQ not ready, proceeding to start ZMQ server", port);
                        println!("===> FOUND VALID DAEMON ON PORT {} WITH ZMQ NOT READY, PROCEEDING TO START ZMQ SERVER", port);
                    }

                    // Check for existing database metadata
                    let existing_db_metadata = db_registry.get_db_daemon_metadata_by_port(port).await?;
                    if existing_db_metadata.is_none() {
                        // Register database metadata early since daemon exists
                        info!("No DB metadata found for port {}, registering new DB metadata", port);
                        println!("===> NO DB METADATA FOUND FOR PORT {}, REGISTERING NEW DB METADATA", port);

                        // Open database to get instance
                        let mut daemon_config = config.clone();
                        daemon_config.path = db_path.clone();
                        daemon_config.port = Some(port);
                        let (db, kv_pairs, vertices, edges) = if is_intended_port && existing_db.is_some() {
                            let db = existing_db.clone().unwrap();
                            let kv_pairs = db.cf_handle("kv_pairs").ok_or_else(|| {
                                GraphError::StorageError("kv_pairs not found".to_string())
                            })?;
                            let vertices = db.cf_handle("vertices").ok_or_else(|| {
                                GraphError::StorageError("vertices not found".to_string())
                            })?;
                            let edges = db.cf_handle("edges").ok_or_else(|| {
                                GraphError::StorageError("edges not found".to_string())
                            })?;
                            (db, kv_pairs, vertices, edges)
                        } else {
                            RocksDBClient::force_unlock(&db_path).await?;
                            let (_db, _kv_pairs, _vertices, _edges) = RocksDBDaemon::open_rocksdb_with_cfs(&daemon_config, &db_path, daemon_config.use_compression).await?;
                            (_db, _kv_pairs, _vertices, _edges)
                        };

                        let db_metadata = DBDaemonMetadata {
                            port,
                            pid: metadata.pid,
                            ip_address: metadata.ip_address.clone(),
                            data_dir: Some(db_path.clone()),
                            config_path: storage_config.config_root_directory.clone().map(PathBuf::from),
                            engine_type: Some("rocksdb".to_string()),
                            last_seen_nanos: SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .map(|d| d.as_nanos() as i64)
                                .unwrap_or(0),
                            rocksdb_db_instance: Some(db.clone()),
                            rocksdb_kv_pairs: db.cf_handle("kv_pairs").map(|arc| Arc::try_unwrap(arc).ok()).flatten(),
                            rocksdb_vertices: db.cf_handle("vertices").map(|arc| Arc::try_unwrap(arc).ok()).flatten(),
                            rocksdb_edges: db.cf_handle("edges").map(|arc| Arc::try_unwrap(arc).ok()).flatten(),
                            sled_db_instance: None,
                            sled_kv_pairs: None,
                            sled_vertices: None,
                            sled_edges: None,
                        };
                        db_registry.register_db_daemon(db_metadata).await?;
                        info!("Registered new DB metadata for port {}", port);
                        println!("===> REGISTERED NEW DB METADATA FOR PORT {}", port);
                    }

                    if !self.daemons.contains_key(&port) {
                        let mut daemon_config = config.clone();
                        daemon_config.path = db_path.clone();
                        daemon_config.port = Some(port);

                        let daemon_creation_result: GraphResult<(RocksDBDaemon, tokio::sync::mpsc::Receiver<()>)> = if is_intended_port && existing_db.is_some() {
                            timeout(
                                TokioDuration::from_secs(10),
                                RocksDBDaemon::new_with_db(daemon_config.clone(), existing_db.clone().unwrap()),
                            ).await
                            .map_err(|_| {
                                error!("Timeout creating RocksDBDaemon (with existing DB) on port {}", port);
                                println!("===> ERROR: TIMEOUT CREATING ROCKSDB DAEMON (EXISTING DB) ON PORT {}", port);
                                GraphError::StorageError(format!("Timeout creating RocksDBDaemon on port {}", port))
                            })?
                            .map_err(|e| {
                                error!("Failed to create RocksDBDaemon (with existing DB) on port {}: {}", port, e);
                                println!("===> ERROR: FAILED TO CREATE ROCKSDB DAEMON (EXISTING DB) ON PORT {}: {}", port, e);
                                e
                            })
                        } else {
                            timeout(
                                TokioDuration::from_secs(10),
                                RocksDBDaemon::new(daemon_config.clone()),
                            ).await
                            .map_err(|_| {
                                error!("Timeout creating RocksDBDaemon on port {}", port);
                                println!("===> ERROR: TIMEOUT CREATING ROCKSDB DAEMON ON PORT {}", port);
                                GraphError::StorageError(format!("Timeout creating RocksDBDaemon on port {}", port))
                            })?
                            .map_err(|e| {
                                error!("Failed to create RocksDBDaemon on port {}: {}", port, e);
                                println!("===> ERROR: FAILED TO CREATE ROCKSDB DAEMON ON PORT {}: {}", port, e);
                                e
                            })
                        };

                        let (daemon, mut ready_rx) = daemon_creation_result?;

                        metadata.last_seen_nanos = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .map(|d| d.as_nanos() as i64)
                            .unwrap_or(0);
                        timeout(TokioDuration::from_secs(5), daemon_registry.register_daemon(metadata.clone()))
                            .await
                            .map_err(|_| {
                                error!("Timeout updating daemon metadata on port {}", port);
                                println!("===> ERROR: TIMEOUT UPDATING DAEMON METADATA ON PORT {}", port);
                                GraphError::StorageError(format!("Timeout updating daemon metadata on port {}", port))
                            })?
                            .map_err(|e| {
                                error!("Failed to update daemon metadata on port {}: {}", port, e);
                                println!("===> ERROR: FAILED TO UPDATE DAEMON METADATA ON PORT {}: {}", port, e);
                                GraphError::StorageError(format!("Failed to update daemon metadata on port {}: {}", port, e))
                            })?;

                        timeout(TokioDuration::from_secs(10), ready_rx.recv())
                            .await
                            .map_err(|_| {
                                error!("Timeout waiting for ZeroMQ server readiness signal on port {}", port);
                                println!("===> ERROR: TIMEOUT WAITING FOR ZEROMQ SERVER READINESS SIGNAL ON PORT {}", port);
                                GraphError::StorageError(format!(
                                    "Timeout waiting for ZeroMQ server readiness signal on port {}",
                                    port
                                ))
                            })?
                            .ok_or_else(|| {
                                error!("ZeroMQ server readiness channel closed for port {}", port);
                                println!("===> ERROR: ZEROMQ SERVER READINESS CHANNEL CLOSED FOR PORT {}", port);
                                GraphError::StorageError(format!("ZeroMQ server readiness channel closed for port {}", port))
                            })?;

                        self.daemons.insert(port, Arc::new(daemon));
                        info!("Registered existing daemon on port {} in self.daemons", port);
                        println!("===> REGISTERED EXISTING DAEMON ON PORT {} IN SELF.DAEMONS", port);
                    }
                    valid_ports.push(port);
                    self.load_balancer.update_node_health(port, true, 0).await;
                    continue;
                } else {
                    warn!("Stale daemon found on port {}, cleaning up", port);
                    println!("===> WARNING: STALE DAEMON FOUND ON PORT {}, CLEANING UP", port);
                    let mut attempts = 0;
                    while attempts < MAX_REGISTRY_ATTEMPTS {
                        match timeout(TokioDuration::from_secs(10), daemon_registry.unregister_daemon(port)).await {
                            Ok(Ok(_)) => {
                                info!("Unregistered stale daemon on port {}", port);
                                println!("===> UNREGISTERED STALE DAEMON ON PORT {}", port);
                                break;
                            }
                            Ok(Err(e)) => {
                                warn!(
                                    "Failed to unregister daemon on port {}: {}. Attempt {}/{}",
                                    port, e, attempts + 1, MAX_REGISTRY_ATTEMPTS
                                );
                                println!(
                                    "===> WARNING: FAILED TO UNREGISTER DAEMON ON PORT {}: {}. ATTEMPT {}/{}",
                                    port, e, attempts + 1, MAX_REGISTRY_ATTEMPTS
                                );
                                attempts += 1;
                                sleep(TokioDuration::from_millis(1000)).await;
                            }
                            Err(_) => {
                                warn!(
                                    "Timeout unregistering daemon on port {}. Attempt {}/{}",
                                    port, attempts + 1, MAX_REGISTRY_ATTEMPTS
                                );
                                println!(
                                    "===> WARNING: TIMEOUT UNREGISTERING DAEMON ON PORT {}. ATTEMPT {}/{}",
                                    port, attempts + 1, MAX_REGISTRY_ATTEMPTS
                                );
                                attempts += 1;
                                sleep(TokioDuration::from_millis(1000)).await;
                            }
                        }
                    }
                    if attempts >= MAX_REGISTRY_ATTEMPTS {
                        error!("Failed to unregister daemon on port {} after {} attempts", port, MAX_REGISTRY_ATTEMPTS);
                        println!(
                            "===> ERROR: FAILED TO UNREGISTER DAEMON ON PORT {} AFTER {} ATTEMPTS",
                            port, MAX_REGISTRY_ATTEMPTS
                        );
                        return Err(GraphError::StorageError(format!(
                            "Failed to unregister daemon on port {} after {} attempts",
                            port, MAX_REGISTRY_ATTEMPTS
                        )));
                    }

                    let ipc_path = format!("/tmp/graphdb-{}.ipc", port);
                    if Path::new(&ipc_path).exists() {
                        warn!("Stale IPC socket found at {}. Attempting cleanup.", ipc_path);
                        println!("===> WARNING: STALE IPC SOCKET FOUND AT {}. ATTEMPTING CLEANUP.", ipc_path);
                        if let Err(e) = tokio::fs::remove_file(&ipc_path).await {
                            warn!("Failed to remove stale IPC socket at {}: {}", ipc_path, e);
                            println!("===> WARNING: FAILED TO REMOVE STALE IPC SOCKET AT {}: {}", ipc_path, e);
                        } else {
                            info!("Successfully removed stale IPC socket at {}", ipc_path);
                            println!("===> SUCCESSFULLY REMOVED STALE IPC SOCKET AT {}", ipc_path);
                        }
                    }

                    RocksDBClient::force_unlock(&db_path).await?;
                    println!("===> Performed force unlock on RocksDB DB at {:?}", db_path);
                }
            }

            if is_intended_port || valid_ports.is_empty() {
                info!(
                    "Initializing daemon on port {} with path {:?} (engine: {})",
                    port, db_path, storage_config.storage_engine_type
                );
                println!(
                    "===> INITIALIZING DAEMON ON PORT {} WITH PATH {:?} (ENGINE: {})",
                    port, db_path, storage_config.storage_engine_type
                );

                if !db_path.exists() {
                    info!("Creating {} directory at {:?}", engine_dir, db_path);
                    println!("===> CREATING {} DIRECTORY AT {:?}", engine_dir.to_uppercase(), db_path);
                    tokio::fs::create_dir_all(&db_path).await.map_err(|e| {
                        error!("Failed to create directory at {:?}: {}", db_path, e);
                        println!("===> ERROR: FAILED TO CREATE DIRECTORY AT {:?}: {}", db_path, e);
                        GraphError::Io(e.to_string())
                    })?;
                    tokio::fs::set_permissions(&db_path, fs::Permissions::from_mode(0o700))
                        .await
                        .map_err(|e| {
                            error!("Failed to set permissions on directory at {:?}: {}", db_path, e);
                            println!("===> ERROR: FAILED TO SET PERMISSIONS ON DIRECTORY AT {:?}: {}", db_path, e);
                            GraphError::Io(e.to_string())
                        })?;
                }

                RocksDBClient::force_unlock(&db_path).await?;
                let mut daemon_config = config.clone();
                daemon_config.path = db_path.clone();
                daemon_config.port = Some(port);
                let (db, kv_pairs, vertices, edges) = if is_intended_port && existing_db.is_some() {
                    let db = existing_db.clone().unwrap();
                    let kv_pairs = db.cf_handle("kv_pairs").ok_or_else(|| {
                        GraphError::StorageError("kv_pairs not found".to_string())
                    })?;
                    let vertices = db.cf_handle("vertices").ok_or_else(|| {
                        GraphError::StorageError("vertices not found".to_string())
                    })?;
                    let edges = db.cf_handle("edges").ok_or_else(|| {
                        GraphError::StorageError("edges not found".to_string())
                    })?;
                    (db, kv_pairs, vertices, edges)
                } else {
                    let (_db, _kv_pairs, _vertices, _edges) = RocksDBDaemon::open_rocksdb_with_cfs(&daemon_config, &db_path, daemon_config.use_compression).await?;
                    (_db, _kv_pairs, _vertices, _edges)
                };

                let db_metadata = DBDaemonMetadata {
                    port,
                    pid: std::process::id(),
                    ip_address: config.host.clone().unwrap_or("127.0.0.1".to_string()),
                    data_dir: Some(db_path.clone()),
                    config_path: storage_config.config_root_directory.clone().map(PathBuf::from),
                    engine_type: Some("rocksdb".to_string()),
                    last_seen_nanos: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_nanos() as i64)
                        .unwrap_or(0),
                    rocksdb_db_instance: Some(db.clone()),
                    rocksdb_kv_pairs: db.cf_handle("kv_pairs").map(|arc| Arc::try_unwrap(arc).ok()).flatten(),
                    rocksdb_vertices: db.cf_handle("vertices").map(|arc| Arc::try_unwrap(arc).ok()).flatten(),
                    rocksdb_edges: db.cf_handle("edges").map(|arc| Arc::try_unwrap(arc).ok()).flatten(),
                    sled_db_instance: None,
                    sled_kv_pairs: None,
                    sled_vertices: None,
                    sled_edges: None,
                };
                db_registry.register_db_daemon(db_metadata).await?;
                info!("Registered new DB metadata for port {}", port);
                println!("===> REGISTERED NEW DB METADATA FOR PORT {}", port);

                let mut daemon_config = config.clone();
                daemon_config.path = db_path.clone();
                daemon_config.port = Some(port);

                let daemon_creation_result: GraphResult<(RocksDBDaemon, tokio::sync::mpsc::Receiver<()>)> = if is_intended_port && existing_db.is_some() {
                    timeout(
                        TokioDuration::from_secs(10),
                        RocksDBDaemon::new_with_db(daemon_config.clone(), existing_db.clone().unwrap()),
                    ).await
                    .map_err(|_| {
                        error!("Timeout creating RocksDBDaemon (with existing DB) on port {}", port);
                        println!("===> ERROR: TIMEOUT CREATING ROCKSDB DAEMON (EXISTING DB) ON PORT {}", port);
                        GraphError::StorageError(format!("Timeout creating RocksDBDaemon on port {}", port))
                    })?
                    .map_err(|e| {
                        error!("Failed to create RocksDBDaemon (with existing DB) on port {}: {}", port, e);
                        println!("===> ERROR: FAILED TO CREATE ROCKSDB DAEMON (EXISTING DB) ON PORT {}: {}", port, e);
                        e
                    })
                } else {
                    timeout(
                        TokioDuration::from_secs(10),
                        RocksDBDaemon::new(daemon_config.clone()),
                    ).await
                    .map_err(|_| {
                        error!("Timeout creating RocksDBDaemon on port {}", port);
                        println!("===> ERROR: TIMEOUT CREATING ROCKSDB DAEMON ON PORT {}", port);
                        GraphError::StorageError(format!("Timeout creating RocksDBDaemon on port {}", port))
                    })?
                    .map_err(|e| {
                        error!("Failed to create RocksDBDaemon on port {}: {}", port, e);
                        println!("===> ERROR: FAILED TO CREATE ROCKSDB DAEMON ON PORT {}: {}", port, e);
                        e
                    })
                };

                let (daemon, mut ready_rx) = daemon_creation_result?;
                        
                info!("Waiting for ZMQ server readiness signal on port {}", port);
                println!("===> WAITING FOR ZMQ SERVER READINESS SIGNAL ON PORT {}", port);
                timeout(TokioDuration::from_secs(10), ready_rx.recv())
                    .await
                    .map_err(|_| {
                        error!("Timeout waiting for ZeroMQ server readiness signal on port {}", port);
                        println!("===> ERROR: TIMEOUT WAITING FOR ZEROMQ SERVER READINESS SIGNAL ON PORT {}", port);
                        GraphError::StorageError(format!("Timeout waiting for ZeroMQ server readiness signal on port {}", port))
                    })?
                    .ok_or_else(|| {
                        error!("ZeroMQ server readiness channel closed for port {}", port);
                        println!("===> ERROR: ZEROMQ SERVER READINESS CHANNEL CLOSED FOR PORT {}", port);
                        GraphError::StorageError(format!("ZeroMQ server readiness channel closed for port {}", port))
                    })?;

                let ipc_path = format!("/tmp/graphdb-{}.ipc", port);
                if !Path::new(&ipc_path).exists() {
                    error!("ZMQ IPC file not created at {} after binding", ipc_path);
                    println!("===> ERROR: ZMQ IPC FILE NOT CREATED AT {} AFTER BINDING", ipc_path);
                    return Err(GraphError::StorageError(format!("ZMQ IPC file not created at {}", ipc_path)));
                }
                info!("ZMQ IPC file verified at {}", ipc_path);
                println!("===> ZMQ IPC FILE VERIFIED AT {}", ipc_path);

                info!("Checking ZMQ server responsiveness on port {}", port);
                println!("===> CHECKING ZMQ SERVER RESPONSIVENESS ON PORT {}", port);
                timeout(TokioDuration::from_secs(10), async {
                    let mut attempts = 0;
                    const MAX_ATTEMPTS: usize = 20;
                    sleep(TokioDuration::from_millis(500)).await;
                    while !self.is_zmq_server_running(port).await? {
                        attempts += 1;
                        if attempts >= MAX_ATTEMPTS {
                            error!("ZMQ server failed to start on port {} after {} attempts", port, MAX_ATTEMPTS);
                            println!("===> ERROR: ZMQ SERVER FAILED TO START ON PORT {} AFTER {} ATTEMPTS", port, MAX_ATTEMPTS);
                            return Err(GraphError::StorageError(format!(
                                "ZMQ server failed to start on port {} after {} attempts",
                                port, MAX_ATTEMPTS
                            )));
                        }
                        info!("ZMQ server not ready on port {}, attempt {}/{}", port, attempts, MAX_ATTEMPTS);
                        println!("===> ZMQ SERVER NOT READY ON PORT {}, ATTEMPT {}/{}", port, attempts, MAX_ATTEMPTS);
                        sleep(TokioDuration::from_millis(500)).await;
                    }
                    info!("ZMQ server is ready for port {}", port);
                    println!("===> ZEROMQ SERVER IS READY FOR PORT {}", port);
                    Ok(())
                })
                .await
                .map_err(|_| {
                    error!("Timeout waiting for ZMQ server to start on port {}", port);
                    println!("===> ERROR: TIMEOUT WAITING FOR ZMQ SERVER TO START ON PORT {}", port);
                    GraphError::StorageError(format!("Timeout waiting for ZMQ server on port {}", port))
                })??;

                let daemon_metadata = DaemonMetadata {
                    service_type: "storage".to_string(),
                    port,
                    pid: std::process::id(),
                    ip_address: config.host.clone().unwrap_or("127.0.0.1".to_string()),
                    data_dir: Some(db_path.clone()),
                    config_path: storage_config.config_root_directory.clone().map(PathBuf::from),
                    engine_type: Some("rocksdb".to_string()),
                    last_seen_nanos: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_nanos() as i64)
                        .unwrap_or(0),
                    zmq_ready: true,
                };

                timeout(TokioDuration::from_secs(5), daemon_registry.register_daemon(daemon_metadata))
                    .await
                    .map_err(|_| {
                        error!("Timeout registering daemon on port {}", port);
                        println!("===> ERROR: TIMEOUT REGISTERING DAEMON ON PORT {}", port);
                        GraphError::StorageError(format!("Timeout registering daemon on port {}", port))
                    })?
                    .map_err(|e| {
                        error!("Failed to register daemon on port {}: {}", port, e);
                        println!("===> ERROR: FAILED TO REGISTER DAEMON ON PORT {}: {}", port, e);
                        GraphError::StorageError(format!("Failed to register daemon on port {}: {}", port, e))
                    })?;

                self.daemons.insert(port, Arc::new(daemon));
                valid_ports.push(port);
                self.load_balancer.update_node_health(port, true, 0).await;
                info!("Initialized and registered new daemon on port {}", port);
                println!("===> INITIALIZED AND REGISTERED NEW DAEMON ON PORT {}", port);
            }
        }

        if valid_ports.is_empty() && !storage_config.cluster_range.is_empty() {
            warn!("No active storage daemons found in registry, using cluster_range: {}", storage_config.cluster_range);
            println!(
                "===> WARNING: NO ACTIVE STORAGE DAEMONS FOUND IN REGISTRY, USING CLUSTER_RANGE: {}",
                storage_config.cluster_range
            );
            let range: Vec<&str> = storage_config.cluster_range.split('-').collect();
            let start: u16 = range[0].parse().unwrap_or(intended_port);
            let end: u16 = range.get(1).and_then(|s| s.parse().ok()).unwrap_or(start);
            let cluster_ports: Vec<u16> = (start..=end).filter(|port| !self.daemons.contains_key(port)).collect();

            for port in cluster_ports {
                if port == intended_port || valid_ports.is_empty() {
                    let engine_dir = daemon_api_storage_engine_type_to_string(&storage_config.storage_engine_type);
                    let db_path = storage_config
                        .data_directory
                        .as_ref()
                        .unwrap_or(&PathBuf::from(DEFAULT_DATA_DIRECTORY))
                        .join(&engine_dir)
                        .join(port.to_string());

                    info!(
                        "Initializing daemon from cluster_range on port {} with path {:?} (engine: {})",
                        port, db_path, storage_config.storage_engine_type
                    );
                    println!(
                        "===> INITIALIZING DAEMON FROM CLUSTER_RANGE ON PORT {} WITH PATH {:?} (ENGINE: {})",
                        port, db_path, storage_config.storage_engine_type
                    );

                    if !db_path.exists() {
                        info!("Creating {} directory at {:?}", engine_dir, db_path);
                        println!("===> CREATING {} DIRECTORY AT {:?}", engine_dir.to_uppercase(), db_path);
                        tokio::fs::create_dir_all(&db_path).await.map_err(|e| {
                            error!("Failed to create directory at {:?}: {}", db_path, e);
                            println!("===> ERROR: FAILED TO CREATE DIRECTORY AT {:?}: {}", db_path, e);
                            GraphError::Io(e.to_string())
                        })?;
                        tokio::fs::set_permissions(&db_path, fs::Permissions::from_mode(0o700))
                            .await
                            .map_err(|e| {
                                error!("Failed to set permissions on directory at {:?}: {}", db_path, e);
                                println!("===> ERROR: FAILED TO SET PERMISSIONS ON DIRECTORY AT {:?}: {}", db_path, e);
                                GraphError::Io(e.to_string())
                            })?;
                    }

                    RocksDBClient::force_unlock(&db_path).await?;
                    println!("===> Performed force unlock on RocksDB DB at {:?}", db_path);

                    let mut daemon_config = config.clone();
                    daemon_config.path = db_path.clone();
                    daemon_config.port = Some(port);
                    let (db, kv_pairs, vertices, edges) = {
                        let (_db, _kv_pairs, _vertices, _edges) = RocksDBDaemon::open_rocksdb_with_cfs(&daemon_config, &db_path, daemon_config.use_compression).await?;
                        (_db, _kv_pairs, _vertices, _edges)
                    };

                    let db_metadata = DBDaemonMetadata {
                        port,
                        pid: std::process::id(),
                        ip_address: config.host.clone().unwrap_or("127.0.0.1".to_string()),
                        data_dir: Some(db_path.clone()),
                        config_path: storage_config.config_root_directory.clone().map(PathBuf::from),
                        engine_type: Some("rocksdb".to_string()),
                        last_seen_nanos: SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .map(|d| d.as_nanos() as i64)
                            .unwrap_or(0),
                        rocksdb_db_instance: Some(db.clone()),
                        rocksdb_kv_pairs: db.cf_handle("kv_pairs").map(|arc| Arc::try_unwrap(arc).ok()).flatten(),
                        rocksdb_vertices: db.cf_handle("vertices").map(|arc| Arc::try_unwrap(arc).ok()).flatten(),
                        rocksdb_edges: db.cf_handle("edges").map(|arc| Arc::try_unwrap(arc).ok()).flatten(),
                        sled_db_instance: None,
                        sled_kv_pairs: None,
                        sled_vertices: None,
                        sled_edges: None,
                    };
                    db_registry.register_db_daemon(db_metadata).await?;
                    info!("Registered new DB metadata for port {}", port);
                    println!("===> REGISTERED NEW DB METADATA FOR PORT {}", port);

                    let Ok(Ok((daemon, mut ready_rx))) = timeout(TokioDuration::from_secs(10), RocksDBDaemon::new(daemon_config.clone()))
                        .await
                        .map_err(|_| {
                            error!("Timeout creating RocksDBDaemon on port {}", port);
                            println!("===> ERROR: TIMEOUT CREATING ROCKSDB DAEMON ON PORT {}", port);
                            GraphError::StorageError(format!("Timeout creating RocksDBDaemon on port {}", port))
                        }) else {
                            return Err(GraphError::StorageError(format!("Failed to create RocksDBDaemon on port {}", port)));
                        };

                    info!("Waiting for ZMQ server readiness signal on port {}", port);
                    println!("===> WAITING FOR ZMQ SERVER READINESS SIGNAL ON PORT {}", port);
                    timeout(TokioDuration::from_secs(10), ready_rx.recv())
                        .await
                        .map_err(|_| {
                            error!("Timeout waiting for ZeroMQ server readiness signal on port {}", port);
                            println!("===> ERROR: TIMEOUT WAITING FOR ZEROMQ SERVER READINESS SIGNAL ON PORT {}", port);
                            GraphError::StorageError(format!("Timeout waiting for ZeroMQ server readiness signal on port {}", port))
                        })?
                        .ok_or_else(|| {
                            error!("ZeroMQ server readiness channel closed for port {}", port);
                            println!("===> ERROR: ZEROMQ SERVER READINESS CHANNEL CLOSED FOR PORT {}", port);
                            GraphError::StorageError(format!("ZeroMQ server readiness channel closed for port {}", port))
                        })?;

                    let ipc_path = format!("/tmp/graphdb-{}.ipc", port);
                    if !Path::new(&ipc_path).exists() {
                        error!("ZMQ IPC file not created at {} after binding", ipc_path);
                        println!("===> ERROR: ZMQ IPC FILE NOT CREATED AT {} AFTER BINDING", ipc_path);
                        return Err(GraphError::StorageError(format!("ZMQ IPC file not created at {}", ipc_path)));
                    }
                    info!("ZMQ IPC file verified at {}", ipc_path);
                    println!("===> ZMQ IPC FILE VERIFIED AT {}", ipc_path);

                    info!("Checking ZMQ server responsiveness on port {}", port);
                    println!("===> CHECKING ZMQ SERVER RESPONSIVENESS ON PORT {}", port);
                    timeout(TokioDuration::from_secs(10), async {
                        let mut attempts = 0;
                        const MAX_ATTEMPTS: usize = 20;
                        sleep(TokioDuration::from_millis(500)).await;
                        while !self.is_zmq_server_running(port).await? {
                            attempts += 1;
                            if attempts >= MAX_ATTEMPTS {
                                error!("ZMQ server failed to start on port {} after {} attempts", port, MAX_ATTEMPTS);
                                println!("===> ERROR: ZMQ SERVER FAILED TO START ON PORT {} AFTER {} ATTEMPTS", port, MAX_ATTEMPTS);
                                return Err(GraphError::StorageError(format!(
                                    "ZMQ server failed to start on port {} after {} attempts",
                                    port, MAX_ATTEMPTS
                                )));
                            }
                            info!("ZMQ server not ready on port {}, attempt {}/{}", port, attempts, MAX_ATTEMPTS);
                            println!("===> ZMQ SERVER NOT READY ON PORT {}, ATTEMPT {}/{}", port, attempts, MAX_ATTEMPTS);
                            sleep(TokioDuration::from_millis(500)).await;
                        }
                        info!("ZMQ server is ready for port {}", port);
                        println!("===> ZEROMQ SERVER IS READY FOR PORT {}", port);
                        Ok(())
                    })
                    .await
                    .map_err(|_| {
                        error!("Timeout waiting for ZMQ server to start on port {}", port);
                        println!("===> ERROR: TIMEOUT WAITING FOR ZMQ SERVER TO START ON PORT {}", port);
                        GraphError::StorageError(format!("Timeout waiting for ZMQ server on port {}", port))
                    })??;

                    let daemon_metadata = DaemonMetadata {
                        service_type: "storage".to_string(),
                        port,
                        pid: std::process::id(),
                        ip_address: config.host.clone().unwrap_or("127.0.0.1".to_string()),
                        data_dir: Some(db_path.clone()),
                        config_path: storage_config.config_root_directory.clone().map(PathBuf::from),
                        engine_type: Some("rocksdb".to_string()),
                        last_seen_nanos: SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .map(|d| d.as_nanos() as i64)
                            .unwrap_or(0),
                        zmq_ready: true,
                    };

                    timeout(TokioDuration::from_secs(5), daemon_registry.register_daemon(daemon_metadata))
                        .await
                        .map_err(|_| {
                            error!("Timeout registering daemon on port {}", port);
                            println!("===> ERROR: TIMEOUT REGISTERING DAEMON ON PORT {}", port);
                            GraphError::StorageError(format!("Timeout registering daemon on port {}", port))
                        })?
                        .map_err(|e| {
                            error!("Failed to register daemon on port {}: {}", port, e);
                            println!("===> ERROR: FAILED TO REGISTER DAEMON ON PORT {}: {}", port, e);
                            GraphError::StorageError(format!("Failed to register daemon on port {}: {}", port, e))
                        })?;

                    self.daemons.insert(port, Arc::new(daemon));
                    valid_ports.push(port);
                    self.load_balancer.update_node_health(port, true, 0).await;
                    info!("Initialized and registered new daemon from cluster_range on port {}", port);
                    println!("===> INITIALIZED AND REGISTERED NEW DAEMON FROM CLUSTER_RANGE ON PORT {}", port);
                }
            }
        }

        if valid_ports.is_empty() {
            error!("No valid ports available for load balancing");
            println!("===> ERROR: NO VALID PORTS AVAILABLE FOR LOAD BALANCING");
            return Err(GraphError::StorageError("No valid ports available for load balancing".to_string()));
        }

        if let Some((client, _socket)) = client {
            let mut clients = self.clients.write().await; // Updated to use write().await assuming self.clients is RwLock
            clients.insert(intended_port, Arc::new(client));
            info!("Registered provided client for port {}", intended_port);
            println!("===> REGISTERED PROVIDED CLIENT FOR PORT {}", intended_port);
        }

        *initialized = true;
        let health_config = HealthCheckConfig {
            interval: TokioDuration::from_secs(10),
            connect_timeout: TokioDuration::from_secs(2),
            response_buffer_size: 1024,
        };
        self.start_health_monitoring(health_config).await;
        info!("Started health monitoring for ports {:?}", valid_ports);
        println!("===> STARTED HEALTH MONITORING FOR PORTS {:?}", valid_ports);

        info!("RocksDBDaemonPool initialized successfully with load balancing on ports {:?}", valid_ports);
        println!("===> ROCKSDB DAEMON POOL INITIALIZED SUCCESSFULLY WITH LOAD BALANCING ON PORTS {:?}", valid_ports);
        Ok(())
    }
```