// ============================================
// PROBLEM: wal-rs consumes entries on read!
// SOLUTION: Use simple append-only file-based WAL
// ============================================

use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::io::{AsyncWriteExt, AsyncReadExt, AsyncSeekExt};
use anyhow::{Result, Context};
use bincode::{Encode, Decode};

#[derive(Encode, Decode, Debug, Clone)]
pub enum SledWalOperation {
    Put { tree: String, key: Vec<u8>, value: Vec<u8> },
    Delete { tree: String, key: Vec<u8> },
    Flush { tree: String },
}

pub struct SledWalManager {
    /// ONLY used for **append** operations (write-locked).
    wal_write: Arc<RwLock<tokio::fs::File>>,
    wal_path:  PathBuf,
    port:      u16,
}

impl std::fmt::Debug for SledWalManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SledWalManager")
            .field("port", &self.port)
            .field("wal_path", &self.wal_path)
            .finish()
    }
}

impl SledWalManager {
    pub async fn new(data_dir: PathBuf, port: u16) -> Result<Self> {
        let wal_dir = data_dir.join("wal_shared");
        tokio::fs::create_dir_all(&wal_dir).await?;
        let wal_path = wal_dir.join("shared.wal");

        println!("===> CREATING WAL MANAGER FOR PORT {} AT {:?}", port, wal_path);

        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&wal_path)
            .await?;

        println!("===> OPENED SHARED WAL FILE AT {:?}", wal_path);

        Ok(Self {
            wal_write: Arc::new(RwLock::new(file)),
            wal_path,
            port,
        })
    }

    /// Append an operation to the shared WAL
    pub async fn append(&self, op: &SledWalOperation) -> Result<u64> {
        let data = bincode::encode_to_vec(op, bincode::config::standard())
                   .context("bincode encode error")?;
        let len = data.len() as u32;

        let mut file = self.wal_write.write().await;

        // current file size == LSN
        let lsn = file.metadata().await?.len();

        // write length prefix + data
        file.write_all(&len.to_le_bytes()).await?;
        file.write_all(&data).await?;
        file.flush().await?;

        println!("===> PORT {} WROTE TO SHARED WAL AT OFFSET {} (len={})",
                 self.port, lsn, len);

        Ok(lsn)
    }

    /// Read all operations since a given offset (concurrent-safe)
    pub async fn read_since(&self, since_offset: u64) -> Result<Vec<(u64, SledWalOperation)>> {
        let mut ops = Vec::new();

        // open a **separate** read handle – does not interfere with append
        let mut file = tokio::fs::File::open(&self.wal_path).await?;
        let file_size = file.metadata().await?.len();

        if since_offset >= file_size {
            return Ok(ops);
        }
        file.seek(std::io::SeekFrom::Start(since_offset)).await?;

        let mut current = since_offset;
        let mut len_buf = [0u8; 4];

        loop {
            // read length prefix
            match file.read_exact(&mut len_buf).await {
                Ok(_) => {},
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
            let len = u32::from_le_bytes(len_buf) as usize;

            // read data
            let mut data = vec![0u8; len];
            file.read_exact(&mut data).await?;

            // decode
            if let Ok((op, _)) = bincode::decode_from_slice::<SledWalOperation, _>(
                &data,
                bincode::config::standard()
            ) {
                ops.push((current, op));
            }
            current += 4 + len as u64;
        }
        Ok(ops)
    }

    /// Current file size (LSN) – **independent** read handle
    pub async fn current_lsn(&self) -> Result<u64> {
        tokio::fs::metadata(&self.wal_path)
            .await
            .map(|m| m.len())
            .context("stat WAL file")
    }

    /// No-op for append-only WAL
    pub async fn truncate(&self, _up_to_lsn: u64) -> Result<()> {
        Ok(())
    }
}