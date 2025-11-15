// lib/src/durability/wal.rs
use crate::durability::GraphOp;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write, Read};
use std::path::PathBuf;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
struct WalHeader {
    magic: u32,      // 0xGDBWAL
    version: u8,     // 1
    entry_size: u32, // size of serialized entry
}

const MAGIC: u32 = 0x47444257; // "GDBW"

pub struct Wal {
    file: File,
    path: PathBuf,
    offset: u64, // current write offset
}

impl Wal {
    pub fn open(path: PathBuf) -> Result<Self, Box<dyn std::error::Error>> {
        std::fs::create_dir_all(path.parent().unwrap_or(&path))?;
        let file_path = path.join("graph.wal");
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(&file_path)?;
        let offset = file.seek(SeekFrom::End(0))?;
        Ok(Self { file, path: file_path, offset })
    }

    pub fn append(&mut self, op: GraphOp) -> Result<u64, Box<dyn std::error::Error>> {
        let encoded: Vec<u8> = bincode::serde::encode_to_vec(&op, bincode::config::standard())?;
        
        let header = WalHeader {
            magic: MAGIC,
            version: 1,
            entry_size: encoded.len() as u32,
        };
        
        let header_bytes = bincode::serde::encode_to_vec(&header, bincode::config::standard())?;
        
        let entry_offset = self.offset;
        self.file.write_all(&header_bytes)?;
        self.file.write_all(&encoded)?;
        self.file.flush()?;
        
        self.offset += header_bytes.len() as u64 + encoded.len() as u64;
        Ok(entry_offset)
    }

    pub fn entries_after(&self, start_offset: u64) -> Result<Vec<(u64, GraphOp)>, Box<dyn std::error::Error>> {
        let mut file = File::open(&self.path)?;
        let mut offset = start_offset;
        let mut entries = Vec::new();
        
        file.seek(SeekFrom::Start(start_offset))?;
        
        loop {
            let mut header_bytes = [0u8; 9]; // 4 + 1 + 4
            if file.read_exact(&mut header_bytes).is_err() {
                break;
            }
            
            let (header, _): (WalHeader, _) = match bincode::serde::decode_from_slice(
                &header_bytes, 
                bincode::config::standard()
            ) {
                Ok(h) => h,
                Err(_) => break,
            };
            
            if header.magic != MAGIC || header.version != 1 {
                break;
            }
            
            let mut payload = vec![0u8; header.entry_size as usize];
            if file.read_exact(&mut payload).is_err() {
                break;
            }
            
            if let Ok((op, _)) = bincode::serde::decode_from_slice::<GraphOp, _>(
                &payload,
                bincode::config::standard()
            ) {
                entries.push((offset, op));
            }
            
            offset += 9 + header.entry_size as u64;
        }
        
        Ok(entries)
    }

    pub fn current_offset(&self) -> u64 {
        self.offset
    }

    pub fn truncate(&mut self, offset: u64) -> Result<(), Box<dyn std::error::Error>> {
        self.file.set_len(offset)?;
        self.offset = offset;
        Ok(())
    }
}