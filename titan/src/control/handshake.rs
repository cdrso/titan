//! Handshake helpers for control connections (Hello/Welcome).

use std::time::Duration;

use crate::control::types::{
    CONTROL_QUEUE_CAPACITY, ClientHello, ClientId, ClientMessage, ConnectionError,
    ControlConnection, DRIVER_INBOX_CAPACITY, DriverMessage, control_channel_paths,
};
use crate::ipc::shmem::{Creator, Opener, ShmPath};
use crate::ipc::spsc::{Consumer, Producer, Timeout};

/// Timeout for receiving Hello/Welcome during handshake.
const HELLO_TIMEOUT: Duration = Duration::from_millis(500);

type ClientHandshakeConn = ControlConnection<
    Producer<ClientMessage, CONTROL_QUEUE_CAPACITY, Creator>,
    Consumer<DriverMessage, CONTROL_QUEUE_CAPACITY, Creator>,
>;

type DriverHandshakeConn = ControlConnection<
    Producer<DriverMessage, CONTROL_QUEUE_CAPACITY, Opener>,
    Consumer<ClientMessage, CONTROL_QUEUE_CAPACITY, Opener>,
>;

/// Client-side handshake: create control queues, send Hello, await Welcome.
pub fn client_handshake(
    driver_inbox_path: ShmPath,
    timeout: Timeout,
) -> Result<ClientHandshakeConn, ConnectionError> {
    let id = ClientId::generate();
    let (tx_path, rx_path) = control_channel_paths(&id);

    let tx = Producer::create(tx_path)?;
    let rx = Consumer::create(rx_path)?;

    tx.push(ClientMessage::Hello(ClientHello { id }))
        .map_err(|_| ConnectionError::QueueFull)?;

    let driver_inbox =
        Producer::<ClientId, DRIVER_INBOX_CAPACITY, Opener>::open(driver_inbox_path)?;
    driver_inbox
        .push(id)
        .map_err(|_| ConnectionError::QueueFull)?;

    match rx.pop_blocking(timeout) {
        Some(DriverMessage::Welcome) => Ok(ControlConnection { id, tx, rx }),
        Some(_) => Err(ConnectionError::ProtocolViolation),
        None => Err(ConnectionError::Timeout),
    }
}

/// Driver-side handshake: accept a client ID and validate Hello.
pub fn driver_accept(client_id: ClientId) -> Result<DriverHandshakeConn, ConnectionError> {
    let (client_tx_path, client_rx_path) = control_channel_paths(&client_id);

    let tx = Producer::<DriverMessage, CONTROL_QUEUE_CAPACITY, Opener>::open(client_rx_path)?;
    let rx = Consumer::<ClientMessage, CONTROL_QUEUE_CAPACITY, Opener>::open(client_tx_path)?;

    // Parse: Expect Hello message
    match rx
        .pop_blocking(Timeout::Duration(HELLO_TIMEOUT))
        .map(ClientHello::try_from)
    {
        Some(Ok(hello)) if hello.id == client_id => {}
        Some(_) => return Err(ConnectionError::ProtocolViolation),
        None => return Err(ConnectionError::Timeout),
    }

    tx.push(DriverMessage::Welcome)
        .map_err(|_| ConnectionError::QueueFull)?;

    Ok(ControlConnection {
        id: client_id,
        tx,
        rx,
    })
}
