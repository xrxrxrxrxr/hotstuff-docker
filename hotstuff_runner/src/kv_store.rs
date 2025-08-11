// hotstuff_runner/src/kv_store.rs
use hotstuff_rs::block_tree::pluggables::{KVGet, KVStore, WriteBatch};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[derive(Clone)]
pub struct MemoryKVStore {
    data: Arc<RwLock<HashMap<Vec<u8>, Vec<u8>>>>,
}

impl MemoryKVStore {
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl KVGet for MemoryKVStore {
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let data = self.data.read().unwrap();
        data.get(key).cloned()
    }
}

impl KVStore for MemoryKVStore {
    type WriteBatch = MemoryWriteBatch;
    type Snapshot<'a> = MemorySnapshot;

    fn write(&mut self, write_batch: Self::WriteBatch) {
        let mut data = self.data.write().unwrap();
        
        for operation in write_batch.operations {
            match operation {
                WriteOp::Set(key, value) => {
                    data.insert(key, value);
                }
                WriteOp::Delete(key) => {
                    data.remove(&key);
                }
            }
        }
    }

    fn clear(&mut self) {
        let mut data = self.data.write().unwrap();
        data.clear();
    }

    fn snapshot(&self) -> Self::Snapshot<'_> {
        let data = self.data.read().unwrap();
        MemorySnapshot {
            data: data.clone(),
        }
    }
}

pub struct MemoryWriteBatch {
    operations: Vec<WriteOp>,
}

enum WriteOp {
    Set(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
}

impl WriteBatch for MemoryWriteBatch {
    fn new() -> Self {
        Self {
            operations: Vec::new(),
        }
    }

    fn set(&mut self, key: &[u8], value: &[u8]) {
        self.operations.push(WriteOp::Set(key.to_vec(), value.to_vec()));
    }

    fn delete(&mut self, key: &[u8]) {
        self.operations.push(WriteOp::Delete(key.to_vec()));
    }
}

pub struct MemorySnapshot {
    data: HashMap<Vec<u8>, Vec<u8>>,
}

impl KVGet for MemorySnapshot {
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.data.get(key).cloned()
    }
}