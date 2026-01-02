//! Driver runtime: multi-threaded driver for client communication and network I/O.
//!
//! # Architecture
//!
//! The driver spawns three threads:
//! - **Control thread**: Manages client sessions, accepts connections, routes channel commands.
//! - **TX thread**: Drains client→driver SPSC queues and sends to network endpoints.
//! - **RX thread**: Receives from network and pushes to driver→client SPSC queues.
//!
//! # Driver-to-Driver Protocol
//!
//! This section documents the inter-driver protocol for pub/sub communication.
//! The design is informed by [Aeron](https://github.com/aeron-io/aeron/wiki/Transport-Protocol-Specification)
//! and [DDS RTPS](https://www.omg.org/spec/DDSI-RTPS) but optimized for our use case.
//!
//! ## Identification Model
//!
//! ```text
//! (RemoteEndpoint, ChannelId) → unique data stream
//! TypeId                      → wire format validation (reject on mismatch)
//! SessionId                   → connection instance correlation
//! ```
//!
//! - **`ChannelId`**: Application-defined stream identifier (like Aeron's streamId).
//!   Same type can have multiple channels (e.g., two price feeds for different exchanges).
//! - **`TypeId`**: Structural hash of the message type. Used for validation, not identification.
//!   Publisher NAKs subscription if type hash doesn't match.
//! - **`SessionId`**: Random per-subscription, used to correlate SETUP/ACK and subsequent messages.
//!
//! ## Handshake Flow (Unicast)
//!
//! ```text
//! Subscriber (D2)                        Publisher (D1)
//!      │                                       │
//!      │  SETUP(session, channel, type_hash,   │
//!      │        rx_window, mtu)                │
//!      │──────────────────────────────────────>│
//!      │                                       │
//!      │         [validate type_hash matches   │
//!      │          channel's published type]    │
//!      │                                       │
//!      │  SETUP_ACK(session, pub_session, mtu) │
//!      │<──────────────────────────────────────│
//!      │            or                         │
//!      │  SETUP_NAK(session, reason)           │
//!      │<──────────────────────────────────────│
//!      │                                       │
//!      │  DATA(channel, seq, payload...)       │
//!      │<──────────────────────────────────────│
//!      │                                       │
//!      │  SM(session, consumed, rx_window)     │
//!      │──────────────────────────────────────>│
//! ```
//!
//! ## Frame Types
//!
//! | Type | Direction | Purpose |
//! |------|-----------|---------|
//! | SETUP | Sub → Pub | Initiate subscription with type validation |
//! | `SETUP_ACK` | Pub → Sub | Accept subscription, begin data flow |
//! | `SETUP_NAK` | Pub → Sub | Reject (type mismatch, channel not found, etc.) |
//! | DATA | Pub → Sub | Application payload |
//! | SM (Status Message) | Sub → Pub | Flow control: consumption progress + receiver window |
//! | TEARDOWN | Either | Close subscription gracefully |
//!
//! ## Transport Modes
//!
//! We support three transport modes, matching Aeron's architecture:
//!
//! ### Unicast (Initial Implementation)
//! - Point-to-point: sender → single receiver
//! - SETUP/SM travel directly between drivers
//! - Simplest, works everywhere, firewall-friendly
//!
//! ### Multicast (Future)
//! - One packet → network duplicates to multicast group
//! - DATA goes to multicast address (e.g., 224.10.9.7)
//! - SM/NAK go to control multicast (next even address, e.g., 224.10.9.8)
//! - Flow control strategies: min (slowest receiver), max (fastest), tagged (critical receivers)
//!
//! ### MDC - Multi-Destination Cast (Future)
//! - Sender individually sends to each receiver (no network multicast required)
//! - Same flow control semantics as multicast
//! - Higher sender CPU cost, but works without multicast infrastructure
//!
//! ## Flow Control
//!
//! Receiver-driven, inspired by Aeron:
//! - Subscriber advertises `receiver_window` in SETUP and SM
//! - Publisher tracks `consumption_offset` from SM
//! - Publisher will not send beyond `consumption_offset + receiver_window`
//! - Back-pressure propagates to client's SPSC queue when window exhausted
//!
//! ## Design Decisions
//!
//! 1. **No discovery protocol**: Endpoints are statically configured. Application knows
//!    "I want channel 42 from 192.168.1.100:9000". Matches Aeron's model.
//!
//! 2. **Single RTT setup**: Like Aeron (SETUP → SM → streaming). Minimal latency.
//!
//! 3. **Type validation on SETUP only**: Zero per-message type checking overhead.
//!    The `TypeId` hash ensures wire format compatibility before data flows.
//!
//! 4. **Minimal header overhead**: 8-byte base header vs Aeron's ~32 bytes.
//!
//! 5. **Session IDs for correlation**: Fast hash-map lookup, no complex addressing.
//!
//! # Example
//!
//! ```ignore
//! use titan::runtime::driver::{Driver, DriverConfig};
//! use titan::net::Endpoint;
//!
//! let config = DriverConfig {
//!     bind_addr: Endpoint::any(9000),
//!     endpoints: vec![Endpoint::new_v4(192, 168, 1, 100, 9000)],
//!     session_timeout: Duration::from_secs(30),
//! };
//!
//! let driver = Driver::spawn(config)?;
//!
//! // ... application runs ...
//!
//! driver.shutdown();
//! ```

mod commands;
pub mod control_thread;
pub mod protocol;
pub mod rx_thread;
pub mod tx_thread;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::control::types::ConnectionError;
use crate::ipc::shmem::ShmPath;
use crate::net::{Endpoint, UdpSocket};
use crate::sync::spsc;
use crate::trace::{debug, error, info};

use commands::{COMMAND_QUEUE_CAPACITY, RxCommand, RxToControlEvent, RxToTxEvent, TxCommand};
use control_thread::ControlThread;
use rx_thread::RxThread;
use tx_thread::TxThread;

/// Configuration for the driver.
pub struct DriverConfig {
    /// Address to bind the UDP socket to.
    pub bind_addr: Endpoint,
    /// Default network endpoints for channel fan-out.
    pub endpoints: Vec<Endpoint>,
    /// Session timeout duration (clients must heartbeat within this period).
    pub session_timeout: Duration,
    /// Path for the driver's inbox (for client connections).
    /// If `None`, uses the default path `/titan-driver-inbox`.
    pub inbox_path: Option<ShmPath>,
}

impl Default for DriverConfig {
    fn default() -> Self {
        Self {
            bind_addr: Endpoint::any(0),
            endpoints: Vec::new(),
            session_timeout: Duration::from_secs(30),
            inbox_path: None,
        }
    }
}

/// Error spawning the driver.
#[derive(Debug, thiserror::Error)]
pub enum DriverError {
    /// Failed to bind UDP socket.
    #[error("failed to bind socket: {0}")]
    Bind(std::io::Error),
    /// Failed to create driver inbox.
    #[error("connection error: {0}")]
    Connection(#[from] ConnectionError),
}

/// Handle to a running driver.
///
/// Dropping the handle will signal shutdown but not wait for threads to exit.
/// Use [`Driver::shutdown`] for graceful shutdown with join.
pub struct Driver {
    /// Shutdown flag shared with control thread.
    shutdown_flag: Arc<AtomicBool>,
    control_handle: Option<JoinHandle<()>>,
    tx_handle: Option<JoinHandle<()>>,
    rx_handle: Option<JoinHandle<()>>,
}

impl Driver {
    /// Spawns the driver threads.
    ///
    /// # Errors
    /// Returns an error if:
    /// - The UDP socket cannot be bound
    /// - The driver inbox already exists (another driver is running)
    ///
    /// # Panics
    /// Panics if thread spawning fails.
    pub fn spawn(config: DriverConfig) -> Result<Self, DriverError> {
        info!(
            bind_addr = %config.bind_addr,
            endpoints = ?config.endpoints,
            session_timeout_ms = config.session_timeout.as_millis() as u64,
            inbox_path = ?config.inbox_path,
            "driver starting"
        );

        // Create a single UDP socket for both TX and RX
        // UDP sockets are thread-safe for concurrent send/recv
        let socket = Arc::new(UdpSocket::bind(config.bind_addr).map_err(|e| {
            error!(bind_addr = %config.bind_addr, error = %e, "failed to bind UDP socket");
            DriverError::Bind(e)
        })?);
        let tx_socket = Arc::clone(&socket);
        let rx_socket = socket;

        // Create inter-thread command channels
        let (tx_cmd_producer, tx_cmd_consumer) =
            spsc::channel::<TxCommand, COMMAND_QUEUE_CAPACITY>();
        let (rx_cmd_producer, rx_cmd_consumer) =
            spsc::channel::<RxCommand, COMMAND_QUEUE_CAPACITY>();
        let (rx_tx_producer, rx_tx_consumer) =
            spsc::channel::<RxToTxEvent, COMMAND_QUEUE_CAPACITY>();
        let (rx_ctrl_producer, rx_ctrl_consumer) =
            spsc::channel::<RxToControlEvent, COMMAND_QUEUE_CAPACITY>();

        // Create shutdown flag
        let shutdown_flag = Arc::new(AtomicBool::new(false));

        // Create control thread
        let control_thread = ControlThread::new(
            tx_cmd_producer,
            rx_cmd_producer,
            rx_ctrl_consumer,
            config.endpoints.clone(),
            config.session_timeout,
            Arc::clone(&shutdown_flag),
            config.inbox_path,
        )?;

        // Spawn TX thread
        debug!("spawning TX thread");
        let tx_handle = thread::Builder::new()
            .name("titan-tx".into())
            .spawn(move || {
                info!("TX thread started");
                let mut tx = TxThread::new(tx_socket, tx_cmd_consumer, rx_tx_consumer);
                tx.run();
                info!("TX thread exiting");
            })
            .expect("failed to spawn TX thread");

        // Spawn RX thread
        debug!("spawning RX thread");
        let rx_handle = thread::Builder::new()
            .name("titan-rx".into())
            .spawn(move || {
                info!("RX thread started");
                let mut rx =
                    RxThread::new(rx_socket, rx_cmd_consumer, rx_tx_producer, rx_ctrl_producer);
                rx.run();
                info!("RX thread exiting");
            })
            .expect("failed to spawn RX thread");

        // Spawn control thread
        debug!("spawning control thread");
        let control_handle = thread::Builder::new()
            .name("titan-control".into())
            .spawn(move || {
                info!("control thread started");
                let mut control = control_thread;
                control.run();
                info!("control thread exiting");
            })
            .expect("failed to spawn control thread");

        info!("driver started successfully");

        Ok(Self {
            shutdown_flag,
            control_handle: Some(control_handle),
            tx_handle: Some(tx_handle),
            rx_handle: Some(rx_handle),
        })
    }

    /// Initiates graceful shutdown and waits for all threads to exit.
    ///
    /// This method:
    /// 1. Sets the shutdown flag (control thread will notify clients and TX/RX)
    /// 2. Waits for control thread to exit
    /// 3. Waits for TX/RX threads to exit
    pub fn shutdown(mut self) {
        info!("driver shutdown initiated");

        // Signal shutdown
        self.shutdown_flag.store(true, Ordering::Relaxed);

        // Wait for control thread (it will send shutdown to TX/RX)
        if let Some(handle) = self.control_handle.take() {
            debug!("waiting for control thread to exit");
            let _ = handle.join();
        }

        // Wait for TX thread
        if let Some(handle) = self.tx_handle.take() {
            debug!("waiting for TX thread to exit");
            let _ = handle.join();
        }

        // Wait for RX thread
        if let Some(handle) = self.rx_handle.take() {
            debug!("waiting for RX thread to exit");
            let _ = handle.join();
        }

        info!("driver shutdown complete");
    }

    /// Returns a clone of the shutdown flag for external signal handling.
    #[must_use]
    pub fn shutdown_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.shutdown_flag)
    }
}

impl Drop for Driver {
    fn drop(&mut self) {
        // Signal shutdown if not already done
        self.shutdown_flag.store(true, Ordering::Relaxed);

        // Best-effort join - don't block indefinitely
        // In normal usage, shutdown() should be called explicitly
    }
}
