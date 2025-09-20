use std::io::{self, Cursor};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};
use std::time::Instant;
use std::collections::HashMap;
use std::pin::Pin;
use futures::future::Future; 

use async_trait::async_trait;
use openraft::{
    AnyError, BasicNode, Entry, EntryPayload, LogId, LeaderId, RaftLogReader, RaftSnapshotBuilder,
    RaftStorage, RaftTypeConfig, Snapshot, SnapshotMeta, StorageError, StorageIOError,
    StoredMembership, Vote, storage::SnapshotSignature, ErrorSubject, ErrorVerb,
};
use rocksdb::{ColumnFamilyDescriptor, DB, Options, WriteOptions, BoundColumnFamily};
use serde::{Deserialize, Serialize};
use tokio::{fs, task, time::{timeout, Duration}};
use tokio::sync::{Mutex as TokioMutex, OnceCell};
use log::{debug, error, info, warn};
use chrono::Utc;

use crate::config::{
    RocksDBConfig, StorageConfig, TypeConfig, RaftCommand, AppResponse, RocksDBRaftStorage, 
    RocksDBDaemonPool, DEFAULT_DATA_DIRECTORY, DEFAULT_STORAGE_PORT, RocksDbWithPath,
    AppRequest,
};
use crate::daemon::daemon_registry::{GLOBAL_DAEMON_REGISTRY, DaemonMetadata};
use models::errors::{GraphError, GraphResult};

pub static ROCKSDB_DB: LazyLock<OnceCell<TokioMutex<RocksDbWithPath>>> = LazyLock::new(|| OnceCell::new());
pub static ROCKSDB_POOL_MAP: LazyLock<OnceCell<TokioMutex<HashMap<u16, Arc<TokioMutex<RocksDBDaemonPool>>>>>> = LazyLock::new(|| OnceCell::new());

#[async_trait]
impl RaftStorage<TypeConfig> for RocksDBRaftStorage {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    fn save_vote(&mut self, vote: &Vote<u64>) -> impl futures::Future<Output = Result<(), StorageError<u64>>> + Send {
        let db_arc = self.db.clone();
        let state_cf = self.state_cf.clone();
        let vote = vote.clone();

        async move {
            task::spawn_blocking(move || {
                let vote_bytes = serde_json::to_vec(&vote).map_err(|e| StorageError::IO {
                    source: StorageIOError::new(
                        ErrorSubject::Vote,
                        ErrorVerb::Write,
                        AnyError::new(&e),
                    ),
                })?;
                let mut write_opts = WriteOptions::default();
                write_opts.set_sync(true);
                db_arc
                    .put_cf_opt(&state_cf, b"vote", &vote_bytes, &write_opts)
                    .map_err(|e| StorageError::IO {
                        source: StorageIOError::new(
                            ErrorSubject::Vote,
                            ErrorVerb::Write,
                            AnyError::new(&e),
                        ),
                    })?;
                debug!("Saved vote: {:?}", vote);
                Ok(())
            })
            .await
            .map_err(|e| StorageError::IO {
                source: StorageIOError::new(
                    ErrorSubject::Vote,
                    ErrorVerb::Write,
                    AnyError::new(&e),
                ),
            })?
        }
    }

    fn read_vote(&mut self) -> impl futures::Future<Output = Result<Option<Vote<u64>>, StorageError<u64>>> + Send {
        let db_arc = self.db.clone();
        let state_cf = self.state_cf.clone();

        async move {
            task::spawn_blocking(move || {
                let vote_bytes = db_arc
                    .get_cf(&state_cf, b"vote")
                    .map_err(|e| StorageError::IO {
                        source: StorageIOError::new(
                            ErrorSubject::Vote,
                            ErrorVerb::Read,
                            AnyError::new(&e),
                        ),
                    })?;

                let vote = vote_bytes
                    .map(|data| {
                        serde_json::from_slice::<Vote<u64>>(&data[..]).map_err(|e| StorageError::IO {
                            source: StorageIOError::new(
                                ErrorSubject::Vote,
                                ErrorVerb::Read,
                                AnyError::new(&e),
                            ),
                        })
                    })
                    .transpose()?;

                debug!("Read vote: {:?}", vote);
                Ok(vote)
            })
            .await
            .map_err(|e| StorageError::IO {
                source: StorageIOError::new(
                    ErrorSubject::Vote,
                    ErrorVerb::Read,
                    AnyError::new(&e),
                ),
            })?
        }
    }

    fn append_to_log<I: IntoIterator<Item = Entry<TypeConfig>> + Send>(
        &mut self,
        entries: I,
    ) -> impl futures::Future<Output = Result<(), StorageError<u64>>> + Send {
        let db_arc = self.db.clone();
        let log_cf = self.log_cf.clone();
        let entries_vec: Vec<_> = entries.into_iter().collect();

        async move {
            task::spawn_blocking(move || {
                let mut write_opts = WriteOptions::default();
                write_opts.set_sync(true);
                for entry in entries_vec {
                    let key = entry.log_id.index.to_be_bytes();
                    let value = serde_json::to_vec(&entry).map_err(|e| StorageError::IO {
                        source: StorageIOError::new(
                            ErrorSubject::Log(entry.log_id),
                            ErrorVerb::Write,
                            AnyError::new(&e),
                        ),
                    })?;
                    db_arc
                        .put_cf_opt(&log_cf, &key, &value, &write_opts)
                        .map_err(|e| StorageError::IO {
                            source: StorageIOError::new(
                                ErrorSubject::Log(entry.log_id),
                                ErrorVerb::Write,
                                AnyError::new(&e),
                            ),
                        })?;
                    debug!("Appended log entry with index: {}", entry.log_id.index);
                }
                Ok(())
            })
            .await
            .map_err(|e| StorageError::IO {
                source: StorageIOError::new(
                    ErrorSubject::Log(LogId {
                        leader_id: LeaderId::new(0, 0),
                        index: 0,
                    }),
                    ErrorVerb::Write,
                    AnyError::new(&e),
                ),
            })?
        }
    }

    fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<u64>,
    ) -> impl futures::Future<Output = Result<(), StorageError<u64>>> + Send {
        let db_arc = self.db.clone();
        let log_cf = self.log_cf.clone();

        async move {
            task::spawn_blocking(move || {
                let start_key = log_id.index.to_be_bytes();
                let iter = db_arc.iterator_cf(&log_cf, rocksdb::IteratorMode::From(&start_key, rocksdb::Direction::Forward));
                let mut write_opts = WriteOptions::default();
                write_opts.set_sync(true);

                for item in iter {
                    let (key, _) = item.map_err(|e| StorageError::IO {
                        source: StorageIOError::new(
                            ErrorSubject::Log(log_id),
                            ErrorVerb::Read,
                            AnyError::new(&e),
                        ),
                    })?;
                    db_arc
                        .delete_cf_opt(&log_cf, &key, &write_opts)
                        .map_err(|e| StorageError::IO {
                            source: StorageIOError::new(
                                ErrorSubject::Log(log_id),
                                ErrorVerb::Delete,
                                AnyError::new(&e),
                            ),
                        })?;
                    debug!("Deleted conflicting log entry with key: {:?}", key);
                }
                Ok(())
            })
            .await
            .map_err(|e| StorageError::IO {
                source: StorageIOError::new(
                    ErrorSubject::Log(log_id),
                    ErrorVerb::Delete,
                    AnyError::new(&e),
                ),
            })?
        }
    }

    fn purge_logs_upto(
        &mut self,
        log_id: LogId<u64>,
    ) -> impl futures::Future<Output = Result<(), StorageError<u64>>> + Send {
        let db_arc = self.db.clone();
        let log_cf = self.log_cf.clone();

        async move {
            task::spawn_blocking(move || {
                let end_key = log_id.index.to_be_bytes();
                let iter = db_arc.iterator_cf(&log_cf, rocksdb::IteratorMode::Start);
                let mut write_opts = WriteOptions::default();
                write_opts.set_sync(true);

                for item in iter {
                    let (key, _) = item.map_err(|e| StorageError::IO {
                        source: StorageIOError::new(
                            ErrorSubject::Log(log_id),
                            ErrorVerb::Read,
                            AnyError::new(&e),
                        ),
                    })?;
                    let index = u64::from_be_bytes(key.as_ref().try_into().map_err(|e| StorageError::IO {
                        source: StorageIOError::new(
                            ErrorSubject::Log(log_id),
                            ErrorVerb::Read,
                            AnyError::new(&e),
                        ),
                    })?);
                    if index > log_id.index {
                        break;
                    }
                    db_arc
                        .delete_cf_opt(&log_cf, &key, &write_opts)
                        .map_err(|e| StorageError::IO {
                            source: StorageIOError::new(
                                ErrorSubject::Log(log_id),
                                ErrorVerb::Delete,
                                AnyError::new(&e),
                            ),
                        })?;
                    debug!("Purged log entry with key: {:?}", key);
                }
                Ok(())
            })
            .await
            .map_err(|e| StorageError::IO {
                source: StorageIOError::new(
                    ErrorSubject::Log(log_id),
                    ErrorVerb::Delete,
                    AnyError::new(&e),
                ),
            })?
        }
    }

    fn last_applied_state(
        &mut self,
    ) -> impl futures::Future<Output = Result<(Option<LogId<u64>>, StoredMembership<u64, u64>), StorageError<u64>>> + Send {
        let db_arc = self.db.clone();
        let state_cf = self.state_cf.clone();

        async move {
            task::spawn_blocking(move || {
                let last_applied_bytes = db_arc
                    .get_cf(&state_cf, b"last_applied")
                    .map_err(|e| StorageError::IO {
                        source: StorageIOError::new(
                            ErrorSubject::StateMachine,
                            ErrorVerb::Read,
                            AnyError::new(&e),
                        ),
                    })?;

                let last_applied: Option<LogId<u64>> = last_applied_bytes
                    .map(|data| {
                        serde_json::from_slice::<LogId<u64>>(&data[..]).map_err(|e| StorageError::IO {
                            source: StorageIOError::new(
                                ErrorSubject::StateMachine,
                                ErrorVerb::Read,
                                AnyError::new(&e),
                            ),
                        })
                    })
                    .transpose()?;

                let membership_bytes = db_arc
                    .get_cf(&state_cf, b"membership")
                    .map_err(|e| StorageError::IO {
                        source: StorageIOError::new(
                            ErrorSubject::StateMachine,
                            ErrorVerb::Read,
                            AnyError::new(&e),
                        ),
                    })?;

                let membership: StoredMembership<u64, u64> = membership_bytes
                    .map(|data| {
                        serde_json::from_slice::<StoredMembership<u64, u64>>(&data[..]).map_err(|e| {
                            StorageError::IO {
                                source: StorageIOError::new(
                                    ErrorSubject::StateMachine,
                                    ErrorVerb::Read,
                                    AnyError::new(&e),
                                ),
                            }
                        })
                    })
                    .transpose()?
                    .unwrap_or_else(|| StoredMembership::new(None, Default::default()));

                Ok((last_applied, membership))
            })
            .await
            .map_err(|e| StorageError::IO {
                source: StorageIOError::new(
                    ErrorSubject::StateMachine,
                    ErrorVerb::Read,
                    AnyError::new(&e),
                ),
            })?
        }
    }

    fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<TypeConfig>],
    ) -> impl futures::Future<Output = Result<Vec<AppResponse>, StorageError<u64>>> + Send {
        let db_arc = self.db.clone();
        let state_cf = self.state_cf.clone();
        let entries_vec = entries.to_vec();

        async move {
            task::spawn_blocking(move || {
                let mut responses = Vec::new();
                let mut write_opts = WriteOptions::default();
                write_opts.set_sync(true);

                for entry in entries_vec {
                    match &entry.payload {
                        EntryPayload::Blank => {
                            responses.push(AppResponse::SetResponse("Blank entry applied".to_string()));
                        }
                        EntryPayload::Normal(app_req) => {
                            if let AppRequest::Command(cmd) = app_req {
                                match cmd {
                                    RaftCommand::Set { key, value, cf } => {
                                        let cf_handle = db_arc
                                            .cf_handle(&cf)
                                            .ok_or_else(|| StorageError::IO {
                                                source: StorageIOError::new(
                                                    ErrorSubject::StateMachine,
                                                    ErrorVerb::Write,
                                                    AnyError::new(&GraphError::StorageError(format!(
                                                        "Column family {} not found",
                                                        cf
                                                    ))),
                                                ),
                                            })?;
                                        db_arc
                                            .put_cf_opt(&cf_handle, key.as_bytes(), value.as_bytes(), &write_opts)
                                            .map_err(|e| StorageError::IO {
                                                source: StorageIOError::new(
                                                    ErrorSubject::StateMachine,
                                                    ErrorVerb::Write,
                                                    AnyError::new(&e),
                                                ),
                                            })?;
                                        responses.push(AppResponse::SetResponse(format!("Set key {} in cf {}", key, cf)));
                                    }
                                    RaftCommand::Delete { key, cf } => {
                                        let cf_handle = db_arc
                                            .cf_handle(&cf)
                                            .ok_or_else(|| StorageError::IO {
                                                source: StorageIOError::new(
                                                    ErrorSubject::StateMachine,
                                                    ErrorVerb::Write,
                                                    AnyError::new(&GraphError::StorageError(format!(
                                                        "Column family {} not found",
                                                        cf
                                                    ))),
                                                ),
                                            })?;
                                        db_arc
                                            .delete_cf_opt(&cf_handle, key.as_bytes(), &write_opts)
                                            .map_err(|e| StorageError::IO {
                                                source: StorageIOError::new(
                                                    ErrorSubject::StateMachine,
                                                    ErrorVerb::Delete,
                                                    AnyError::new(&e),
                                                ),
                                            })?;
                                        responses.push(AppResponse::DeleteResponse);
                                    }
                                    other => {
                                        responses.push(AppResponse::SetResponse(format!(
                                            "Unhandled RaftCommand: {:?}",
                                            other
                                        )));
                                    }
                                }
                            }
                        }
                        EntryPayload::Membership(membership) => {
                            let membership_bytes = serde_json::to_vec(membership).map_err(|e| StorageError::IO {
                                source: StorageIOError::new(
                                    ErrorSubject::StateMachine,
                                    ErrorVerb::Write,
                                    AnyError::new(&e),
                                ),
                            })?;
                            db_arc
                                .put_cf_opt(&state_cf, b"membership", &membership_bytes, &write_opts)
                                .map_err(|e| StorageError::IO {
                                    source: StorageIOError::new(
                                        ErrorSubject::StateMachine,
                                        ErrorVerb::Write,
                                        AnyError::new(&e),
                                    ),
                                })?;
                            responses.push(AppResponse::SetResponse("Membership updated".to_string()));
                        }
                    }

                    let last_applied_bytes = serde_json::to_vec(&entry.log_id).map_err(|e| StorageError::IO {
                        source: StorageIOError::new(
                            ErrorSubject::StateMachine,
                            ErrorVerb::Write,
                            AnyError::new(&e),
                        ),
                    })?;
                    db_arc
                        .put_cf_opt(&state_cf, b"last_applied", &last_applied_bytes, &write_opts)
                        .map_err(|e| StorageError::IO {
                            source: StorageIOError::new(
                                ErrorSubject::StateMachine,
                                ErrorVerb::Write,
                                AnyError::new(&e),
                            ),
                        })?;
                    debug!("Applied entry to state machine: {:?}", entry);
                }
                Ok(responses)
            })
            .await
            .map_err(|e| StorageError::IO {
                source: StorageIOError::new(
                    ErrorSubject::StateMachine,
                    ErrorVerb::Write,
                    AnyError::new(&e),
                ),
            })?
        }
    }

    fn begin_receiving_snapshot(
        &mut self,
    ) -> impl futures::Future<Output = Result<Box<<TypeConfig as RaftTypeConfig>::SnapshotData>, StorageError<u64>>> + Send {
        async move {
            Ok(Box::new(Cursor::new(Vec::new())))
        }
    }

    fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, u64>,
        snapshot: Box<<TypeConfig as RaftTypeConfig>::SnapshotData>,
    ) -> impl futures::Future<Output = Result<(), StorageError<u64>>> + Send {
        let db_arc = self.db.clone();
        let snapshot_cf = self.snapshot_cf.clone();
        let snapshot_meta_cf = self.snapshot_meta_cf.clone();
        let meta_clone = meta.clone();

        async move {
            let meta_bytes = serde_json::to_vec(&meta_clone).map_err(|e| StorageError::IO {
                source: StorageIOError::new(
                    ErrorSubject::Snapshot(None),
                    ErrorVerb::Write,
                    AnyError::new(&e),
                ),
            })?;

            task::spawn_blocking(move || -> Result<(), StorageError<u64>> {
                let mut write_opts = WriteOptions::default();
                write_opts.set_sync(true);

                // Clear existing snapshot data
                let iter = db_arc.iterator_cf(&snapshot_cf, rocksdb::IteratorMode::Start);
                for item in iter {
                    let (key, _) = item.map_err(|e| StorageError::IO {
                        source: StorageIOError::new(
                            ErrorSubject::Snapshot(None),
                            ErrorVerb::Read,
                            AnyError::new(&e),
                        ),
                    })?;
                    db_arc
                        .delete_cf_opt(&snapshot_cf, &key, &write_opts)
                        .map_err(|e| StorageError::IO {
                            source: StorageIOError::new(
                                ErrorSubject::Snapshot(None),
                                ErrorVerb::Delete,
                                AnyError::new(&e),
                            ),
                        })?;
                }

                // Write new snapshot data
                let data = snapshot.into_inner();
                let mut offset = 0usize;
                while offset < data.len() {
                    if offset + 8 > data.len() {
                        break;
                    }
                    let key_len = u64::from_be_bytes(data[offset..offset + 8].try_into().unwrap()) as usize;
                    offset += 8;
                    if offset + key_len > data.len() {
                        break;
                    }
                    let key = &data[offset..offset + key_len];
                    offset += key_len;

                    if offset + 8 > data.len() {
                        break;
                    }
                    let value_len = u64::from_be_bytes(data[offset..offset + 8].try_into().unwrap()) as usize;
                    offset += 8;
                    if offset + value_len > data.len() {
                        break;
                    }
                    let value = &data[offset..offset + value_len];
                    offset += value_len;

                    db_arc
                        .put_cf_opt(&snapshot_cf, key, value, &write_opts)
                        .map_err(|e| StorageError::IO {
                            source: StorageIOError::new(
                                ErrorSubject::Snapshot(None),
                                ErrorVerb::Write,
                                AnyError::new(&e),
                            ),
                        })?;
                }

                // Store snapshot metadata
                db_arc
                    .put_cf_opt(&snapshot_meta_cf, meta_clone.snapshot_id.as_bytes(), &meta_bytes, &write_opts)
                    .map_err(|e| StorageError::IO {
                        source: StorageIOError::new(
                            ErrorSubject::Snapshot(None),
                            ErrorVerb::Write,
                            AnyError::new(&e),
                        ),
                    })?;

                // Store last applied log id
                let last_applied_bytes = serde_json::to_vec(&meta_clone.last_log_id).map_err(|e| StorageError::IO {
                    source: StorageIOError::new(
                        ErrorSubject::Snapshot(None),
                        ErrorVerb::Write,
                        AnyError::new(&e),
                    ),
                })?;

                db_arc
                    .put_cf_opt(&snapshot_meta_cf, b"last_applied", &last_applied_bytes, &write_opts)
                    .map_err(|e| StorageError::IO {
                        source: StorageIOError::new(
                            ErrorSubject::Snapshot(None),
                            ErrorVerb::Write,
                            AnyError::new(&e),
                        ),
                    })?;

                Ok(())
            })
            .await
            .map_err(|e| StorageError::IO {
                source: StorageIOError::new(
                    ErrorSubject::Snapshot(None),
                    ErrorVerb::Write,
                    AnyError::new(&e),
                ),
            })?
        }
    }

    fn get_current_snapshot(
        &mut self,
    ) -> impl futures::Future<Output = Result<Option<Snapshot<TypeConfig>>, StorageError<u64>>> + Send {
        let db_arc = self.db.clone();
        let snapshot_cf = self.snapshot_cf.clone();
        let snapshot_meta_cf = self.snapshot_meta_cf.clone();
        
        async move {
            task::spawn_blocking(move || -> Result<Option<Snapshot<TypeConfig>>, StorageError<u64>> {
                let meta_bytes = db_arc.get_cf(&snapshot_meta_cf, b"latest_snapshot").map_err(|e| StorageError::IO {
                    source: StorageIOError::new(
                        ErrorSubject::Snapshot(None),
                        ErrorVerb::Read,
                        AnyError::new(&e),
                    ),
                })?;
                
                let meta: Option<SnapshotMeta<u64, u64>> = meta_bytes
                    .map(|data| {
                        serde_json::from_slice::<SnapshotMeta<u64, u64>>(&data[..]).map_err(|e| StorageError::IO {
                            source: StorageIOError::new(
                                ErrorSubject::Snapshot(None),
                                ErrorVerb::Read,
                                AnyError::new(&e),
                            ),
                        })
                    })
                    .transpose()?;
                    
                if let Some(meta) = meta {
                    let mut snapshot_data = Vec::new();
                    let iter = db_arc.iterator_cf(&snapshot_cf, rocksdb::IteratorMode::Start);
                    for item in iter {
                        let (key, value) = item.map_err(|e| StorageError::IO {
                            source: StorageIOError::new(
                                ErrorSubject::Snapshot(None),
                                ErrorVerb::Read,
                                AnyError::new(&e),
                            ),
                        })?;
                        snapshot_data.extend_from_slice(&(key.len() as u64).to_be_bytes());
                        snapshot_data.extend_from_slice(&key);
                        snapshot_data.extend_from_slice(&(value.len() as u64).to_be_bytes());
                        snapshot_data.extend_from_slice(&value);
                    }
                    
                    // Convert SnapshotMeta<u64, u64> to SnapshotMeta<u64, TypeConfig> for return
                    let type_config_meta = SnapshotMeta {
                        snapshot_id: meta.snapshot_id,
                        last_log_id: meta.last_log_id,
                        last_membership: StoredMembership::new(
                            meta.last_membership.log_id().clone(), 
                            meta.last_membership.membership().clone()
                        ),
                    };
                    
                    Ok(Some(Snapshot {
                        meta: type_config_meta,
                        snapshot: Box::new(Cursor::new(snapshot_data)),
                    }))
                } else {
                    Ok(None)
                }
            })
            .await
            .map_err(|e| StorageError::IO {
                source: StorageIOError::new(
                    ErrorSubject::Snapshot(None),
                    ErrorVerb::Read,
                    AnyError::new(&e),
                ),
            })?
        }
    }

    fn get_log_state(
        &mut self,
    ) -> impl futures::Future<Output = Result<openraft::LogState<TypeConfig>, StorageError<u64>>> + Send {
        let db_arc = self.db.clone();
        let log_cf = self.log_cf.clone();
        
        async move {
            task::spawn_blocking(move || {
                let mut last_purged_log_id = None;
                let mut last_log_id = None;
                let iter = db_arc.iterator_cf(&log_cf, rocksdb::IteratorMode::Start);
                
                for item in iter {
                    let (key, value) = item.map_err(|e| StorageError::IO {
                        source: StorageIOError::new(
                            ErrorSubject::Log(LogId {
                                leader_id: LeaderId::new(0, 0),
                                index: 0,
                            }),
                            ErrorVerb::Read,
                            AnyError::new(&e),
                        ),
                    })?;
                    let index = u64::from_be_bytes(key.as_ref().try_into().map_err(|e| StorageError::IO {
                        source: StorageIOError::new(
                            ErrorSubject::Log(LogId {
                                leader_id: LeaderId::new(0, 0),
                                index: 0,
                            }),
                            ErrorVerb::Read,
                            AnyError::new(&e),
                        ),
                    })?);
                    let entry: Entry<TypeConfig> = serde_json::from_slice(&value).map_err(|e| StorageError::IO {
                        source: StorageIOError::new(
                            ErrorSubject::Log(LogId {
                                leader_id: LeaderId::new(0, index),
                                index,
                            }),
                            ErrorVerb::Read,
                            AnyError::new(&e),
                        ),
                    })?;
                    if last_purged_log_id.is_none() {
                        last_purged_log_id = Some(entry.log_id);
                    }
                    last_log_id = Some(entry.log_id);
                }
                
                Ok(openraft::LogState {
                    last_purged_log_id,
                    last_log_id,
                })
            })
            .await
            .map_err(|e| StorageError::IO {
                source: StorageIOError::new(
                    ErrorSubject::Log(LogId {
                        leader_id: LeaderId::new(0, 0),
                        index: 0,
                    }),
                    ErrorVerb::Read,
                    AnyError::new(&e),
                ),
            })?
        }
    }

    fn get_log_reader(&mut self) -> impl futures::Future<Output = Self::LogReader> + Send {
        let cloned = self.clone();
        async move { cloned }
    }

    fn get_snapshot_builder(&mut self) -> impl futures::Future<Output = Self::SnapshotBuilder> + Send {
        let cloned = self.clone();
        async move { cloned }
    }
}

impl RaftLogReader<TypeConfig> for RocksDBRaftStorage {
    fn try_get_log_entries<RB: std::ops::RangeBounds<u64> + Clone + std::fmt::Debug + Send>(
        &mut self,
        range: RB,
    ) -> impl futures::Future<Output = Result<Vec<Entry<TypeConfig>>, StorageError<u64>>> + Send {
        let db_arc = self.db.clone();
        let log_cf = self.log_cf.clone();
        
        let start = match range.start_bound() {
            std::ops::Bound::Included(&i) => i,
            std::ops::Bound::Excluded(&i) => i + 1,
            std::ops::Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            std::ops::Bound::Included(&i) => i + 1,
            std::ops::Bound::Excluded(&i) => i,
            std::ops::Bound::Unbounded => u64::MAX,
        };
        
        async move {
            task::spawn_blocking(move || {
                let mut entries = Vec::new();
                let start_key = start.to_be_bytes();
                let iter = db_arc.iterator_cf(&log_cf, rocksdb::IteratorMode::From(&start_key, rocksdb::Direction::Forward));
                
                for item in iter {
                    let (key, value) = item.map_err(|e| StorageError::IO {
                        source: StorageIOError::new(
                            ErrorSubject::Log(LogId {
                                leader_id: LeaderId::new(0, 0),
                                index: start,
                            }),
                            ErrorVerb::Read,
                            AnyError::new(&e),
                        ),
                    })?;
                    let index = u64::from_be_bytes(key.as_ref().try_into().map_err(|e| StorageError::IO {
                        source: StorageIOError::new(
                            ErrorSubject::Log(LogId {
                                leader_id: LeaderId::new(0, 0),
                                index: start,
                            }),
                            ErrorVerb::Read,
                            AnyError::new(&e),
                        ),
                    })?);
                    if index >= end {
                        break;
                    }
                    let entry: Entry<TypeConfig> = serde_json::from_slice(&value).map_err(|e| StorageError::IO {
                        source: StorageIOError::new(
                            ErrorSubject::Log(LogId {
                                leader_id: LeaderId::new(0, index),
                                index,
                            }),
                            ErrorVerb::Read,
                            AnyError::new(&e),
                        ),
                    })?;
                    entries.push(entry);
                }
                Ok(entries)
            })
            .await
            .map_err(|e| StorageError::IO {
                source: StorageIOError::new(
                    ErrorSubject::Log(LogId {
                        leader_id: LeaderId::new(0, 0),
                        index: 0,
                    }),
                    ErrorVerb::Read,
                    AnyError::new(&e),
                ),
            })?
        }
    }
}

impl RaftSnapshotBuilder<TypeConfig> for RocksDBRaftStorage {
    fn build_snapshot(
        &mut self,
    ) -> impl futures::Future<Output = std::result::Result<openraft::Snapshot<TypeConfig>, openraft::StorageError<u64>>> + std::marker::Send {
        let db_arc = self.db.clone();
        let snapshot_cf = self.snapshot_cf.clone();
        let snapshot_meta_cf = self.snapshot_meta_cf.clone();
        
        async move {
            task::spawn_blocking(move || {
                let snapshot_id = format!("snapshot-{}", Utc::now().timestamp_nanos_opt().unwrap());
                let mut snapshot_data = Vec::new();
                let iter = db_arc.iterator_cf(&snapshot_cf, rocksdb::IteratorMode::Start);
                
                for item in iter {
                    let (key, value) = item.map_err(|e| StorageError::IO {
                        source: StorageIOError::new(
                            ErrorSubject::Snapshot(None),
                            ErrorVerb::Read,
                            AnyError::new(&e),
                        ),
                    })?;
                    snapshot_data.extend_from_slice(&(key.len() as u64).to_be_bytes());
                    snapshot_data.extend_from_slice(&key);
                    snapshot_data.extend_from_slice(&(value.len() as u64).to_be_bytes());
                    snapshot_data.extend_from_slice(&value);
                }
                
                let meta = SnapshotMeta {
                    snapshot_id: snapshot_id.clone(),
                    last_log_id: None,
                    last_membership: StoredMembership::new(None, Default::default()),
                };
                let meta_bytes = serde_json::to_vec(&meta).map_err(|e| StorageError::IO {
                    source: StorageIOError::new(
                        ErrorSubject::Snapshot(None),
                        ErrorVerb::Write,
                        AnyError::new(&e),
                    ),
                })?;
                let mut write_opts = WriteOptions::default();
                write_opts.set_sync(true);
                db_arc
                    .put_cf_opt(&snapshot_meta_cf, b"latest_snapshot", &meta_bytes, &write_opts)
                    .map_err(|e| StorageError::IO {
                        source: StorageIOError::new(
                            ErrorSubject::Snapshot(None),
                            ErrorVerb::Write,
                            AnyError::new(&e),
                        ),
                    })?;
                    
                Ok(Snapshot {
                    meta,
                    snapshot: Box::new(Cursor::new(snapshot_data)),
                })
            })
            .await
            .map_err(|e| StorageError::IO {
                source: StorageIOError::new(
                    ErrorSubject::Snapshot(None),
                    ErrorVerb::Write,
                    AnyError::new(&e),
                ),
            })?
        }
    }
}

impl RocksDBRaftStorage {
    pub async fn new_from_config(
        config: &RocksDBConfig,
        storage_config: &StorageConfig,
    ) -> GraphResult<Self> {
        let start_time = Instant::now();
        info!("Initializing RocksDBRaftStorage with config: {:?}", config);

        let default_data_dir = PathBuf::from(DEFAULT_DATA_DIRECTORY);
        let base_data_dir = storage_config
            .data_directory
            .as_ref()
            .unwrap_or(&default_data_dir);
        let port = config.port.unwrap_or(DEFAULT_STORAGE_PORT);
        let db_path = base_data_dir.join("rocksdb_raft").join(port.to_string());

        info!("Using RocksDB path {:?}", db_path);
        fs::create_dir_all(&db_path)
            .await
            .map_err(|e| {
                error!("Failed to create database directory at {:?}: {}", db_path, e);
                GraphError::StorageError(format!("Failed to create database directory at {:?}: {}", db_path, e))
            })?;

        if !db_path.is_dir() {
            error!("Path {:?} exists but is not a directory", db_path);
            return Err(GraphError::StorageError(format!("Path {:?} is not a directory", db_path)));
        }

        let metadata = fs::metadata(&db_path)
            .await
            .map_err(|e| {
                error!("Failed to access directory metadata at {:?}: {}", db_path, e);
                GraphError::StorageError(format!("Failed to access directory metadata at {:?}: {}", db_path, e))
            })?;

        if metadata.permissions().readonly() {
            error!("Directory at {:?} is not writable", db_path);
            return Err(GraphError::StorageError(format!("Directory at {:?} is not writable", db_path)));
        }

        let daemon_metadata_opt = GLOBAL_DAEMON_REGISTRY.get_daemon_metadata(port).await.ok().flatten();
        let pool = match daemon_metadata_opt {
            Some(_) => {
                info!("Found existing daemon on port {}, attempting to reuse pool", port);
                let pool_map = ROCKSDB_POOL_MAP.get_or_init(|| async {
                    TokioMutex::new(HashMap::new())
                }).await;
                
                let mut pool_map_guard = timeout(Duration::from_secs(5), pool_map.lock())
                    .await
                    .map_err(|_| GraphError::StorageError("Failed to acquire pool map lock".to_string()))?;

                if let Some(existing_pool) = pool_map_guard.get(&port) {
                    info!("Reusing existing RocksDBDaemonPool for port {}", port);
                    existing_pool.clone()
                } else {
                    info!("Stale daemon registry entry found, creating new pool");
                    Self::force_unlock(&db_path).await?;
                    let new_pool = Arc::new(TokioMutex::new(RocksDBDaemonPool::new()));
                    pool_map_guard.insert(port, new_pool.clone());
                    new_pool
                }
            }
            None => {
                info!("No existing daemon found, creating a new RocksDBDaemonPool");
                Self::force_unlock(&db_path).await?;
                let pool_map = ROCKSDB_POOL_MAP.get_or_init(|| async {
                    TokioMutex::new(HashMap::new())
                }).await;
                
                let mut pool_map_guard = timeout(Duration::from_secs(5), pool_map.lock())
                    .await
                    .map_err(|_| GraphError::StorageError("Failed to acquire pool map lock".to_string()))?;
                
                let new_pool = Arc::new(TokioMutex::new(RocksDBDaemonPool::new()));
                pool_map_guard.insert(port, new_pool.clone());
                new_pool
            }
        };

        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.set_compaction_style(rocksdb::DBCompactionStyle::Level);

        let cf_names = vec!["state", "log", "snapshot", "snapshot_meta"];
        let cfs: Vec<ColumnFamilyDescriptor> = cf_names
            .into_iter()
            .map(|name| ColumnFamilyDescriptor::new(name, Options::default()))
            .collect();

        let db = timeout(Duration::from_secs(10), async {
            info!("Attempting to open RocksDB at {:?}", db_path);
            let db = DB::open_cf_descriptors(&opts, &db_path, cfs)
                .map_err(|e| {
                    error!("Failed to open RocksDB at {:?}", db_path);
                    GraphError::StorageError(format!("Failed to open RocksDB database at {:?}: {}", db_path, e))
                })?;
            info!("Successfully opened RocksDB database at {:?}", db_path);
            Ok::<_, GraphError>(db)
        }).await
            .map_err(|_| GraphError::StorageError("Timeout opening RocksDB database".to_string()))??;

        let db_arc = Arc::new(db);
        ROCKSDB_DB.set(TokioMutex::new(RocksDbWithPath { db: db_arc.clone(), path: db_path.clone() }))
            .map_err(|_| GraphError::StorageError("Failed to set ROCKSDB_DB singleton".to_string()))?;

        {
            let mut pool_guard = timeout(Duration::from_secs(10), pool.lock())
                .await
                .map_err(|_| GraphError::StorageError("Failed to acquire pool lock for initialization".to_string()))?;
            info!("Initializing cluster with use_raft: {}", config.use_raft_for_scale);
            timeout(Duration::from_secs(10), pool_guard.initialize_with_db(config, db_arc.clone()))
                .await
                .map_err(|_| GraphError::StorageError("Timeout initializing RocksDBDaemonPool".to_string()))??;
            info!("Initialized cluster on port {} with existing DB", port);
        }

        // Create a separate scope to handle the column family references
        let storage = {
            let db_ref = &db_arc;
            Self {
                db: db_arc.clone(),
                state_cf: Arc::new(unsafe { 
                    std::mem::transmute::<_, BoundColumnFamily<'static>>(
                        db_ref.cf_handle("state").unwrap().clone()
                    )
                }),
                log_cf: Arc::new(unsafe { 
                    std::mem::transmute::<_, BoundColumnFamily<'static>>(
                        db_ref.cf_handle("log").unwrap().clone()
                    )
                }),
                snapshot_cf: Arc::new(unsafe { 
                    std::mem::transmute::<_, BoundColumnFamily<'static>>(
                        db_ref.cf_handle("snapshot").unwrap().clone()
                    )
                }),
                snapshot_meta_cf: Arc::new(unsafe { 
                    std::mem::transmute::<_, BoundColumnFamily<'static>>(
                        db_ref.cf_handle("snapshot_meta").unwrap().clone()
                    )
                }),
                state_cf_name: "state".to_string(),
                log_cf_name: "log".to_string(),
                snapshot_cf_name: "snapshot".to_string(),
                snapshot_meta_cf_name: "snapshot_meta".to_string(),
                config: storage_config.clone(),
                client: None,
            }
        };

        info!("Successfully initialized RocksDBRaftStorage in {}ms", start_time.elapsed().as_millis());
        Ok(storage)
    }

    pub async fn force_unlock(path: &Path) -> GraphResult<()> {
        info!("Attempting to force unlock RocksDB database at {:?}", path);
        let lock_path = path.join("LOCK");

        debug!("Checking for lock file at {:?}", lock_path);
        if lock_path.exists() {
            warn!("Found potential lock file at {:?}", lock_path);
            #[cfg(unix)]
            {
                const MAX_RETRIES: u32 = 3;
                const BASE_DELAY_MS: u64 = 1000;
                let mut attempt = 0;

                while attempt < MAX_RETRIES {
                    if let Err(e) = timeout(Duration::from_secs(2), fs::remove_file(&lock_path)).await {
                        warn!("Failed to remove lock file at {:?} on attempt {}: {:?}", lock_path, attempt + 1, e);
                        attempt += 1;
                        tokio::time::sleep(Duration::from_millis(BASE_DELAY_MS * (attempt as u64 + 1))).await;
                        continue;
                    }
                    info!("Successfully removed lock file at {:?}", lock_path);
                    return Ok(());
                }
                return Err(GraphError::StorageError(format!("Failed to remove lock file at {:?}", lock_path)));
            }
            #[cfg(not(unix))]
            {
                if let Err(e) = timeout(Duration::from_secs(2), fs::remove_file(&lock_path)).await {
                    warn!("Failed to remove lock file at {:?}: {:?}", lock_path, e);
                    return Err(GraphError::StorageError(format!("Failed to remove lock file at {:?}", lock_path)));
                }
                info!("Successfully removed lock file at {:?}", lock_path);
            }
        }
        Ok(())
    }
}