extern crate memmap2;

use memmap2::{MmapMut};

use std::convert::TryInto;
use std::fs::{File, OpenOptions};
use std::path::Path;
use std::str::FromStr;
use crate::log::config::Config;

const OFF_WIDTH: usize = 4;
const POS_WIDTH: usize = 8;
pub const ENT_WIDTH: usize = OFF_WIDTH + POS_WIDTH;

pub struct Index {
    file: File,
    path: Box<Path>,
    mmap: MmapMut,
    pub size: usize,
}

pub fn new_index(path: Box<Path>, c: &Config) -> std::io::Result<Index> {
    let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .unwrap();
    let size: usize = f.metadata()?.len().try_into().unwrap();

    f.set_len(c.segment.max_index_bytes).unwrap();

    let file = f.try_clone().unwrap();
    let mmap = unsafe { MmapMut::map_mut(&f)? };

    Ok(Index {
        file,
        path,
        mmap,
        size,
    })
}

impl Index {
    pub fn close(&self) {
        self.mmap.flush().unwrap();
        self.file.sync_all().unwrap();
    }

    pub fn read(&self, i: i64) -> Result<(u32, u64), String> {
        if self.size == 0 {
            return Err("EOF".to_string());
        }

        let rec_i: u32 =
            if i == -1 {
                (self.size / ENT_WIDTH - 1).try_into().unwrap()
            }
            else {
                i.try_into().unwrap()
            };
        
        let rec_idx: usize = rec_i.try_into().unwrap();
        let rec_pos = rec_idx * ENT_WIDTH;

        if self.size < rec_pos + ENT_WIDTH {
            Err("EOF".to_string())
        }
        else {
            Ok((
                u32::from_be_bytes(self.mmap[rec_pos..rec_pos + OFF_WIDTH].try_into().unwrap()),
                u64::from_be_bytes(self.mmap[rec_pos + OFF_WIDTH..rec_pos + ENT_WIDTH].try_into().unwrap())
            ))
        }
    }

    pub fn write(&mut self, off: u32, pos: u64) -> Result<(), String> {
        println!("mmap.len: {}, size: {}", self.mmap.len(), self.size);
        if self.mmap.len() < self.size + ENT_WIDTH {
            Err("EOF".to_string())
        }
        else {
            self.mmap[self.size..self.size + OFF_WIDTH].clone_from_slice(&off.to_be_bytes());
            self.mmap[self.size + OFF_WIDTH..self.size + ENT_WIDTH].clone_from_slice(&pos.to_be_bytes());
            self.size += ENT_WIDTH;
            Ok(())
        }
    }

    pub fn name(&self) -> String {
        String::from_str(self.path.to_str().unwrap()).unwrap()
    }
}
