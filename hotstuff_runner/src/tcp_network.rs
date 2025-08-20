// hotstuff_runner/src/tcp_network.rs
//! Lock-free TCP network implementation for Docker multi-process deployment

use hotstuff_rs::{
    networking::{
        network::Network,
        messages::{Message, ProgressMessage},
    },
    types::{
        validator_set::ValidatorSet,
        update_sets::ValidatorSetUpdates,
        crypto_primitives::VerifyingKey,
    },
};
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use std::collections::HashMap;
use std::net::{TcpListener, TcpStream, SocketAddr};
use std::io::{Read, Write};
use std::thread;
use tracing::{debug, info, error, warn};
use serde::{Serialize, Deserialize};
use borsh::{BorshSerialize, BorshDeserialize};
use hotstuff_rs::block_sync::messages::BlockSyncMessage;
use crossbeam::queue::SegQueue;
use crossbeam::channel::{unbounded, Sender, Receiver};

// Message type enumeration
#[derive(Serialize, Deserialize, Clone, Debug)]
enum MessageType {
    Proposal,
    Vote, 
    NewView,
    Timeout,
    TimeoutCertificate,
    HotStuff,
    Pacemaker,
    BlockSyncAdvertise,
    BlockSyncRequest,
    BlockSyncResponse,
}

// Network message wrapper - direct byte transmission
#[derive(Serialize, Deserialize, Clone)]
struct NetworkMessage {
    from: Vec<u8>,  // VerifyingKey bytes
    message_type: MessageType, // Message type identifier
    message_bytes: Vec<u8>, // Message raw bytes (serialized using other methods)
}

// TCP network configuration
#[derive(Clone)]
pub struct TcpNetworkConfig {
    pub my_addr: SocketAddr,
    pub peer_addrs: HashMap<VerifyingKey, SocketAddr>,
    pub my_key: VerifyingKey,
}

// Lock-free message queue using crossbeam
pub struct LockFreeMessageQueue {
    queue: Arc<SegQueue<(VerifyingKey, Message)>>,
    size_counter: AtomicUsize,
}

impl LockFreeMessageQueue {
    fn new() -> Self {
        Self {
            queue: Arc::new(SegQueue::new()),
            size_counter: AtomicUsize::new(0),
        }
    }
    
    fn push(&self, item: (VerifyingKey, Message)) {
        self.queue.push(item);
        self.size_counter.fetch_add(1, Ordering::Relaxed);
    }
    
    fn pop(&self) -> Option<(VerifyingKey, Message)> {
        if let Some(item) = self.queue.pop() {
            self.size_counter.fetch_sub(1, Ordering::Relaxed);
            Some(item)
        } else {
            None
        }
    }
    
    fn len(&self) -> usize {
        self.size_counter.load(Ordering::Relaxed)
    }
}

// Lock-free connection pool
pub struct LockFreeConnectionPool {
    connections: Arc<SegQueue<(VerifyingKey, TcpStream)>>,
    connection_count: AtomicUsize,
}

impl LockFreeConnectionPool {
    fn new() -> Self {
        Self {
            connections: Arc::new(SegQueue::new()),
            connection_count: AtomicUsize::new(0),
        }
    }
    
    fn add_connection(&self, key: VerifyingKey, stream: TcpStream) {
        self.connections.push((key, stream));
        self.connection_count.fetch_add(1, Ordering::Relaxed);
    }
    
    fn get_connection(&self, key: &VerifyingKey) -> Option<TcpStream> {
        // Try to find existing connection for this key
        let mut temp_connections = Vec::new();
        let mut found_stream = None;
        
        // Extract all connections to check
        while let Some((conn_key, stream)) = self.connections.pop() {
            if conn_key == *key {
                found_stream = Some(stream);
                break;
            } else {
                temp_connections.push((conn_key, stream));
            }
        }
        
        // Put back other connections
        for (conn_key, stream) in temp_connections {
            self.connections.push((conn_key, stream));
        }
        
        if found_stream.is_some() {
            self.connection_count.fetch_sub(1, Ordering::Relaxed);
        }
        
        found_stream
    }
    
    fn get_connection_count(&self) -> usize {
        self.connection_count.load(Ordering::Relaxed)
    }
}

// Lock-free TCP network implementation
pub struct TcpNetwork {
    config: TcpNetworkConfig,
    message_queue: Arc<LockFreeMessageQueue>,
    message_sender: Sender<(VerifyingKey, Message)>, // For self-messages
    connection_pool: Arc<LockFreeConnectionPool>,
    _server_handle: thread::JoinHandle<()>,
}

impl TcpNetwork {
    pub fn new(config: TcpNetworkConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let message_queue = Arc::new(LockFreeMessageQueue::new());
        let connection_pool = Arc::new(LockFreeConnectionPool::new());
        let (message_sender, message_receiver) = unbounded();
        
        // Start TCP server
        let server_config = config.clone();
        let server_queue = Arc::clone(&message_queue);
        let server_handle = thread::spawn(move || {
            if let Err(e) = run_lockfree_tcp_server(server_config, server_queue) {
                error!("TCP server error: {}", e);
            }
        });

        // Start message receiver thread for self-messages
        let queue_clone = Arc::clone(&message_queue);
        thread::spawn(move || {
            while let Ok((key, message)) = message_receiver.recv() {
                queue_clone.push((key, message));
            }
        });

        // Wait for server to start
        thread::sleep(std::time::Duration::from_millis(500));

        // Connect to peer nodes
        let mut network = Self {
            config: config.clone(),
            message_queue,
            message_sender,
            connection_pool,
            _server_handle: server_handle,
        };

        network.connect_to_peers()?;
        
        Ok(network)
    }

    fn connect_to_peers(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        for (peer_key, peer_addr) in &self.config.peer_addrs {
            if *peer_key == self.config.my_key {
                continue;
            }

            info!("Trying to connect to peer node: {:?} -> {}", 
                  peer_key.to_bytes()[0..4].to_vec(), peer_addr);
            
            // Add retry mechanism
            let mut connected = false;
            for attempt in 1..=3 {
                match TcpStream::connect(peer_addr) {
                    Ok(stream) => {
                        info!("Successfully connected to peer node: {} (attempt {})", peer_addr, attempt);
                        self.connection_pool.add_connection(*peer_key, stream);
                        connected = true;
                        break;
                    }
                    Err(e) => {
                        if attempt < 3 {
                            warn!("Connection attempt {}/3 failed {}: {}", attempt, peer_addr, e);
                            thread::sleep(std::time::Duration::from_millis(1000));
                        } else {
                            warn!("All connection attempts failed {}: {} (node may not be started yet)", peer_addr, e);
                        }
                    }
                }
            }
        }
        
        info!("Established {} peer connections", self.connection_pool.get_connection_count());
        Ok(())
    }

    // Convert Message to bytes helper function
    fn message_to_bytes(message: &Message) -> Result<(MessageType, Vec<u8>), Box<dyn std::error::Error>> {
        // Determine MessageType based on message type
        let message_type = match message {
            Message::ProgressMessage(progress_msg) => {
                match progress_msg {
                    ProgressMessage::HotStuffMessage(_) => MessageType::HotStuff,
                    ProgressMessage::PacemakerMessage(_) => MessageType::Pacemaker,
                    ProgressMessage::BlockSyncAdvertiseMessage(_) => MessageType::BlockSyncAdvertise,
                }
            }
            Message::BlockSyncMessage(sync_msg) => {
                match sync_msg {
                    BlockSyncMessage::BlockSyncRequest(_) => MessageType::BlockSyncRequest,
                    BlockSyncMessage::BlockSyncResponse(_) => MessageType::BlockSyncResponse,
                }
            }
        };
        
        // Use Borsh serialization
        let bytes = message.try_to_vec().map_err(|e| {
            error!("Message serialization failed: {}", e);
            format!("Message serialization failed: {}", e)
        })?;
        
        Ok((message_type, bytes))
    }

    // Rebuild Message from bytes helper function
    fn bytes_to_message(_message_type: MessageType, bytes: &[u8]) -> Result<Message, Box<dyn std::error::Error>> {
        // Use BorshDeserialize trait method
        let message = Message::try_from_slice(bytes).map_err(|e| {
            error!("Message deserialization failed: {}", e);
            format!("Message deserialization failed: {}", e)
        })?;

        Ok(message)
    }

    fn send_to_peer_lockfree(&self, peer_key: &VerifyingKey, message: &Message) -> Result<(), Box<dyn std::error::Error>> {
        // Check if sending to self
        if *peer_key == self.config.my_key {
            if let Err(e) = self.message_sender.send((self.config.my_key, message.clone())) {
                error!("Failed to send to self: {}", e);
                return Err(e.into());
            }
            return Ok(());
        }

        // Try to get existing connection from lock-free pool
        let mut stream = self.connection_pool.get_connection(peer_key);
        
        // If no connection available, create new one
        if stream.is_none() {
            if let Some(peer_addr) = self.config.peer_addrs.get(peer_key) {
                match TcpStream::connect(peer_addr) {
                    Ok(new_stream) => {
                        info!("Reconnected to peer node: {}", peer_addr);
                        stream = Some(new_stream);
                    }
                    Err(e) => {
                        error!("Reconnection failed {}: {}", peer_addr, e);
                        return Err(Box::new(e));
                    }
                }
            } else {
                error!("Cannot find peer node address: {:?}", peer_key.to_bytes()[0..4].to_vec());
                return Err("Unknown peer node".into());
            }
        }

        if let Some(mut tcp_stream) = stream {
            let (message_type, message_bytes) = Self::message_to_bytes(message)?;
            
            let net_msg = NetworkMessage {
                from: self.config.my_key.to_bytes().to_vec(),
                message_type,
                message_bytes,
            };
            
            let serialized = bincode::serialize(&net_msg)?;
            let length = serialized.len() as u32;
            
            // Send length prefix
            tcp_stream.write_all(&length.to_be_bytes())?;
            // Send message content
            tcp_stream.write_all(&serialized)?;
            tcp_stream.flush()?;
            
            // Put connection back in pool for reuse
            self.connection_pool.add_connection(*peer_key, tcp_stream);
            
            Ok(())
        } else {
            Err("No available connection".into())
        }
    }
}

impl Clone for TcpNetwork {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            message_queue: self.message_queue.clone(),  // Share the same receive queue
            message_sender: self.message_sender.clone(),
            connection_pool: self.connection_pool.clone(),
            _server_handle: thread::spawn(|| {}), // Note: This is not a real clone, but satisfies type requirements
        }
    }
}

impl Network for TcpNetwork {
    fn init_validator_set(&mut self, validator_set: ValidatorSet) {
        info!("TCP node {:?} initializing validator set: {} validators", 
              self.config.my_key.to_bytes()[0..4].to_vec(),
              validator_set.len());
    }

    fn update_validator_set(&mut self, _updates: ValidatorSetUpdates) {
        info!("TCP node {:?} updating validator set", 
              self.config.my_key.to_bytes()[0..4].to_vec());
    }

    fn broadcast(&mut self, message: Message) {
        let total_nodes = self.config.peer_addrs.len();
        
        let mut success_count = 0;
        
        // Send to all nodes, including self
        for peer_key in self.config.peer_addrs.keys() {
            if let Err(e) = self.send_to_peer_lockfree(peer_key, &message) {
                error!("Broadcast send failed to {:?}: {}", peer_key.to_bytes()[0..4].to_vec(), e);
            } else {
                success_count += 1;
            }
        }
        
        debug!("Successfully broadcast to {}/{} nodes", success_count, total_nodes);
    }

    fn send(&mut self, peer: VerifyingKey, message: Message) {
        if let Err(e) = self.send_to_peer_lockfree(&peer, &message) {
            error!("Failed to send to {:?}: {}", peer.to_bytes()[0..4].to_vec(), e);
        }
    }

    fn recv(&mut self) -> Option<(VerifyingKey, Message)> {
        // Use lock-free queue for message reception
        if let Some((sender_key, message)) = self.message_queue.pop() {
            let sender_id = format!("{:?}", &sender_key.to_bytes()[0..4]);
            
            // Check Message content
            match &message {
                Message::ProgressMessage(progress_msg) => {
                    match progress_msg {
                        ProgressMessage::HotStuffMessage(_hotstuff_msg) => {
                            // Check if this message contains block data
                            // This needs to be implemented based on specific HotStuffMessage structure
                        },
                        ProgressMessage::PacemakerMessage(_) => {
                            // Handle pacemaker message
                        },
                        ProgressMessage::BlockSyncAdvertiseMessage(_) => {
                            // Handle block sync advertise message
                        }
                    }
                },
                Message::BlockSyncMessage(sync_msg) => {
                    match sync_msg {
                        BlockSyncMessage::BlockSyncRequest(_) => {
                            // Handle block sync request
                        },
                        BlockSyncMessage::BlockSyncResponse(_) => {
                            // Handle block sync response
                        }
                    }
                }
            }
            
            Some((sender_key, message))
        } else {
            None
        }
    }
}

// Lock-free TCP server running function
fn run_lockfree_tcp_server(
    config: TcpNetworkConfig,
    message_queue: Arc<LockFreeMessageQueue>,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(config.my_addr)?;
    info!("Lock-free TCP server listening: {}", config.my_addr);
    
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let queue = Arc::clone(&message_queue);
                thread::spawn(move || {
                    if let Err(e) = handle_lockfree_client(stream, queue) {
                        error!("Error handling client connection: {}", e);
                    }
                });
            }
            Err(e) => {
                error!("Error accepting connection: {}", e);
            }
        }
    }
    
    Ok(())
}

// Handle client connections lock-free
fn handle_lockfree_client(
    mut stream: TcpStream,
    message_queue: Arc<LockFreeMessageQueue>,
) -> Result<(), Box<dyn std::error::Error>> {
    let peer_addr = stream.peer_addr()?;
    debug!("New connection from: {}", peer_addr);
    
    loop {
        // Read message length
        let mut length_buf = [0u8; 4];
        match stream.read_exact(&mut length_buf) {
            Ok(_) => {},
            Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                debug!("Connection closed normally: {}", peer_addr);
                break;
            }
            Err(e) => {
                error!("Failed to read length from {}: {}", peer_addr, e);
                break;
            }
        }
        
        let length = u32::from_be_bytes(length_buf) as usize;
        
        // Prevent oversized messages
        if length > 10 * 1024 * 1024 { // 10MB limit
            error!("Message too large: {} bytes from {}", length, peer_addr);
            break;
        }
        
        if length == 0 {
            debug!("Received empty message from {}", peer_addr);
            continue;
        }
        
        // Read message content
        let mut message_buf = vec![0u8; length];
        match stream.read_exact(&mut message_buf) {
            Ok(_) => {},
            Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                debug!("Connection closed while reading message: {}", peer_addr);
                break;
            }
            Err(e) => {
                error!("Failed to read message content from {}: {}", peer_addr, e);
                break;
            }
        }
        
        // Deserialize network message
        match bincode::deserialize::<NetworkMessage>(&message_buf) {
            Ok(net_msg) => {
                // Reconstruct VerifyingKey from bytes
                let sender_key: VerifyingKey = match net_msg.from.try_into() {
                    Ok(bytes_array) => match VerifyingKey::from_bytes(&bytes_array) {
                        Ok(key) => key,
                        Err(_) => {
                            error!("Cannot construct VerifyingKey from bytes");
                            continue;
                        }
                    },
                    Err(_) => {
                        error!("Incorrect byte array length");
                        continue;
                    }
                };
                
                // Deserialize HotStuff message
                match TcpNetwork::bytes_to_message(net_msg.message_type, &net_msg.message_bytes) {
                    Ok(hotstuff_message) => {
                        // Send to lock-free message queue
                        message_queue.push((sender_key, hotstuff_message));
                    }
                    Err(e) => {
                        error!("Failed to deserialize HotStuff message: {}", e);
                        // Continue processing next message, don't exit
                    }
                }
            }
            Err(e) => {
                error!("Failed to deserialize network message from {}: {}", peer_addr, e);
                // Continue processing next message, don't exit
            }
        }
    }
    
    Ok(())
}