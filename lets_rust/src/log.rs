use serde::{Deserialize, Serialize};
use std::sync::Mutex;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    value: String,
    offset: usize,
}

pub type Log = Mutex<Vec<Record>>;

pub trait Logger {
    fn create_empty() -> Log;
    fn append(&self, record: Record) -> usize;
    fn read(&self, offset: usize) -> Result<Record, String>;
}

impl Logger for Log {
    fn create_empty() -> Log {
        Mutex::new(Vec::<Record>::new())
    }

    fn append(&self, mut record: Record) -> usize {
        let mut records = self.lock().unwrap();
        let offset = records.len();
        record.offset = offset;
        records.push(record);
        offset
    }

    fn read(&self, offset: usize) -> Result<Record, String> {
        let records = self.lock().unwrap();
        if offset < records.len() {
            Ok(records[offset].clone())
        } else {
            Err("Record Not Found".to_string())
        }
    }
}
