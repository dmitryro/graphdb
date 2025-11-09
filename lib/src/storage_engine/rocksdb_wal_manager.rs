// ============================================
// PROBLEM: wal-rs consumes entries on read!
// SOLUTION: Use simple append-only file-based WAL
// ============================================
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::io::{AsyncWriteExt, AsyncReadExt, AsyncSeekExt};
use serde::{Serialize, Deserialize};
use anyhow::{Result, Context};
use bincode::{Encode, Decode};

#[derive(Serialize, Deserialize, Debug, Clone, Encode, Decode)]
pub enum RocksDBWalOperation {
    Put { cf: String, key: Vec<u8>, value: Vec<u8> },
    Delete { cf: String, key: Vec<u8> },
    Flush { cf: String },
}

pub struct RocksDBWalManager {
    wal_file: Arc<RwLock<tokio::fs::File>>,
    wal_path: PathBuf,
    port: u16,
}

impl std::fmt::Debug for RocksDBWalManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RocksDBWalManager")
            .field("port", &self.port)
            .field("wal_path", &self.wal_path)
            .finish()
    }
}

impl RocksDBWalManager {
    /// Create a WAL manager - ALL ports share the SAME WAL file
    pub async fn new(data_dir: PathBuf, port: u16) -> Result<Self> {
        let wal_dir = data_dir.join("wal_shared");
        tokio::fs::create_dir_all(&wal_dir).await?;

        let wal_path = wal_dir.join("shared.wal");
        println!("===> CREATING ROCKSDB WAL MANAGER FOR PORT {} AT {:?}", port, wal_path);

        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&wal_path)
            .await?;

        println!("===> OPENED SHARED WAL FILE AT {:?}", wal_path);

        Ok(Self {
            wal_file: Arc::new(RwLock::new(file)),
            wal_path,
            port,
        })
    }

    /// Append an operation to the shared WAL
    pub async fn append(&self, op: &RocksDBWalOperation) -> Result<u64> {
        let data = bincode::encode_to_vec(op, bincode::config::standard())
            .map_err(|e| anyhow::anyhow!("bincode encode error: {}", e))?;
        let len = data.len() as u32;

        let mut file = self.wal_file.write().await;

        // Get current position (this is our LSN)
        let lsn = file.metadata().await?.len();

        // Write length prefix + data
        file.write_all(&len.to_le_bytes()).await?;
        file.write_all(&data).await?;
        file.flush().await?;

        println!("===> PORT {} WROTE TO SHARED WAL AT OFFSET {} (len={})",
                 self.port, lsn, len);

        Ok(lsn)
    }

    /// Read all operations since a given offset
    pub async fn read_since(&self, since_offset: u64) -> Result<Vec<(u64, RocksDBWalOperation)>> {
        let mut operations = Vec::new();

        // Open file for reading
        let mut file = tokio::fs::File::open(&self.wal_path).await?;

        // Get total file size
        let file_size = file.metadata().await?.len();

        if since_offset >= file_size {
            return Ok(operations);
        }

        // Seek to start position
        file.seek(std::io::SeekFrom::Start(since_offset)).await?;

        let mut current_offset = since_offset;

        // Read entries until EOF
        while current_offset < file_size {
            // Read length prefix
            let mut len_bytes = [0u8; 4];
            match file.read_exact(&mut len_bytes).await {
                Ok(_) => {},
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }

            let len = u32::from_le_bytes(len_bytes) as usize;

            // Read data
            let mut data = vec![0u8; len];
            file.read_exact(&mut data).await?;

            // Deserialize
            if let Ok((op, _)) = bincode::decode_from_slice::<RocksDBWalOperation, _>(
                &data,
                bincode::config::standard()
            ) {
                operations.push((current_offset, op));
            }

            current_offset += 4 + len as u64;
        }

        Ok(operations)
    }

    /// Get the current file size (LSN)
    pub async fn current_lsn(&self) -> Result<u64> {
        let file = self.wal_file.read().await;
        Ok(file.metadata().await?.len())
    }

    pub async fn truncate(&self, _up_to_lsn: u64) -> Result<()> {
        Ok(())
    }
}
