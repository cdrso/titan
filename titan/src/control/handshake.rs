//! Handshake helpers for control connections (Hello/Welcome).

use crate::control::types::{
    CONTROL_QUEUE_CAPACITY, ClientHello, ClientId, ClientMessage, ClientRole, ConnectionError,
    ControlConnection, DRIVER_INBOX_CAPACITY, DriverMessage, DriverRole, HELLO_TIMEOUT,
    control_channel_paths,
};
use crate::ipc::shmem::{Opener, ShmPath};
use crate::ipc::spsc::{Consumer, Producer, Timeout};

type ClientConn = ControlConnection<ClientRole>;
type DriverConn = ControlConnection<DriverRole>;

/// Client-side handshake: create control queues, send Hello, await Welcome.
///
/// # Errors
/// - [`ConnectionError::QueueFull`] if control queue is full during Hello or registration
/// - [`ConnectionError::Timeout`] if Welcome is not received before the deadline
/// - [`ConnectionError::ProtocolViolation`] if the driver responds with an unexpected message
/// - [`ConnectionError::Shm`] for shared memory creation/open failures
pub fn client_connect(
    driver_inbox_path: ShmPath,
    timeout: Timeout,
) -> Result<ClientConn, ConnectionError> {
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
///
/// # Errors
/// - [`ConnectionError::Timeout`] if Hello is not received before the deadline
/// - [`ConnectionError::ProtocolViolation`] if the Hello contains the wrong ID or message type
/// - [`ConnectionError::QueueFull`] if the Welcome cannot be enqueued
/// - [`ConnectionError::Shm`] for shared memory open failures
pub fn driver_accept(client_id: ClientId) -> Result<DriverConn, ConnectionError> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::types::driver_inbox_path;
    use crate::ipc::shmem::Creator;
    use serial_test::serial;
    use std::thread;
    use std::time::Duration;

    fn with_clean_inbox<F, R>(f: F) -> R
    where
        F: FnOnce(ShmPath) -> R,
    {
        let path = driver_inbox_path();
        let _ = rustix::shm::unlink(path.as_ref());
        f(path)
    }

    #[test]
    #[serial]
    fn handshake_success() {
        with_clean_inbox(|inbox_path| {
            // Driver creates inbox
            let driver_inbox =
                Consumer::<ClientId, DRIVER_INBOX_CAPACITY, Creator>::create(inbox_path.clone())
                    .unwrap();

            let client_thread = thread::spawn(move || {
                client_connect(inbox_path, Timeout::Duration(Duration::from_secs(1)))
            });

            // Driver accepts
            thread::sleep(Duration::from_millis(10)); // Let client register
            let client_id = driver_inbox.pop().expect("client should have registered");
            let driver_conn = driver_accept(client_id).unwrap();

            // Client should complete
            let client_conn = client_thread.join().unwrap().unwrap();

            assert_eq!(client_conn.id, driver_conn.id);
        });
    }

    #[test]
    #[serial]
    fn handshake_client_timeout_no_driver() {
        with_clean_inbox(|inbox_path| {
            // Create inbox but don't accept
            let _driver_inbox =
                Consumer::<ClientId, DRIVER_INBOX_CAPACITY, Creator>::create(inbox_path.clone())
                    .unwrap();

            let result = client_connect(inbox_path, Timeout::Duration(Duration::from_millis(50)));

            assert!(matches!(result, Err(ConnectionError::Timeout)));
        });
    }

    #[test]
    #[serial]
    fn handshake_no_inbox_fails() {
        with_clean_inbox(|inbox_path| {
            // Don't create inbox - should fail to open
            let result = client_connect(inbox_path, Timeout::Duration(Duration::from_millis(50)));

            assert!(matches!(result, Err(ConnectionError::Shm(_))));
        });
    }

    #[test]
    #[serial]
    fn driver_accept_timeout_no_hello() {
        // Create a client ID but don't actually run client handshake
        let client_id = ClientId::generate();
        let (tx_path, rx_path) = control_channel_paths(&client_id);

        // Create the queues manually without sending Hello
        let _tx =
            Producer::<ClientMessage, CONTROL_QUEUE_CAPACITY, Creator>::create(tx_path).unwrap();
        let _rx =
            Consumer::<DriverMessage, CONTROL_QUEUE_CAPACITY, Creator>::create(rx_path).unwrap();

        let result = driver_accept(client_id);
        assert!(matches!(result, Err(ConnectionError::Timeout)));
    }

    #[test]
    #[serial]
    fn handshake_bidirectional_communication() {
        with_clean_inbox(|inbox_path| {
            let driver_inbox =
                Consumer::<ClientId, DRIVER_INBOX_CAPACITY, Creator>::create(inbox_path.clone())
                    .unwrap();

            let client_thread = thread::spawn(move || {
                client_connect(inbox_path, Timeout::Duration(Duration::from_secs(1)))
            });

            thread::sleep(Duration::from_millis(10));
            let client_id = driver_inbox.pop().unwrap();
            let driver_conn = driver_accept(client_id).unwrap();
            let client_conn = client_thread.join().unwrap().unwrap();

            // Test bidirectional communication after handshake
            // Client -> Driver
            client_conn
                .tx
                .push(ClientMessage::Command(
                    crate::control::types::ClientCommand::Heartbeat,
                ))
                .unwrap();

            let msg = driver_conn.rx.pop().unwrap();
            assert!(matches!(
                msg,
                ClientMessage::Command(crate::control::types::ClientCommand::Heartbeat)
            ));

            // Driver -> Client
            driver_conn.tx.push(DriverMessage::Heartbeat).unwrap();
            let msg = client_conn.rx.pop().unwrap();
            assert!(matches!(msg, DriverMessage::Heartbeat));
        });
    }
}
