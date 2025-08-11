### Test mode: 

- local 4 nodes (dockers) + 1 client (docker) test
- load_tester client supported, customised TPS
- calculate TPS via log
```
 ./run_test.sh
 docker-compose --profile "*" down    
```
### Note: 
Modified `fn block_data(&self, block: &CryptoHash)` in `hotstuff_rs/src/block_tree/pluggables.rs` to support `on_commit_block` log in `hotstuff_runner/src/tcp_node.rs`. 

Key: `find` function in `block_data` consumes the iterator.

From
```
fn block_data(&self, block: &CryptoHash) -> Result<Option<Data>, KVGetError> {
        let data_len = self.block_data_len(block)?;
        match data_len {
            None => Ok(None),
            Some(len) => {
                let mut data = (0..len.int()).map(|i| self.block_datum(block, i));
                if let None = data.find(|datum| datum.is_none()) {
                    Ok(Some(Data::new(data.map(|datum| datum.unwrap()).collect())))
                } else {
                    Err(KVGetError::ValueExpectedButNotFound {
                        key: Key::BlockData {
                            block: block.clone(),
                        },
                    })
                }
            }
        }
    }
```
 to
```
// Revised block_data method to handle None values more gracefully
    fn block_data(&self, block: &CryptoHash) -> Result<Option<Data>, KVGetError> {
        let data_len = self.block_data_len(block)?;
        match data_len {
            None => Ok(None),
            Some(len) => {
                let data_vec: Vec<Option<Datum>> = (0..len.int())
                    .map(|i| self.block_datum(block, i))
                    .collect();
                
                if data_vec.iter().any(|datum| datum.is_none()) {
                    Err(KVGetError::ValueExpectedButNotFound {
                        key: Key::BlockData {
                            block: block.clone(),
                        },
                    })
                } else {
                    let unwrapped_data: Vec<Datum> = data_vec
                        .into_iter()
                        .map(|datum| datum.unwrap())
                        .collect();
                    Ok(Some(Data::new(unwrapped_data)))
                }
            }
        }
    }```
