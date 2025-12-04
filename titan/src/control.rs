pub mod client;
pub mod driver;
pub mod types;

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;

    use crate::control::types::{
        CLIENT_QUEUE_CAPACITY, ClientCommand, ClientHello, ClientId, ClientMessage,
        DRIVER_INBOX_CAPACITY, DriverMessage, channel_paths,
    };
    use crate::ipc::shmem::{Creator, Opener, ShmPath};
    use crate::ipc::spsc::{Consumer, Producer};

    fn unique_inbox_path(suffix: &str) -> String {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        format!(
            "/titan-inbox-{}-{}-{}",
            std::process::id(),
            suffix,
            COUNTER.fetch_add(1, Ordering::Relaxed)
        )
    }

    #[test]
    fn test_protocol_registration_creates_inbox_entry() {
        let inbox_path_str = unique_inbox_path("reg");
        let _ = rustix::shm::unlink(&inbox_path_str);
        let inbox_path = ShmPath::new(inbox_path_str).unwrap();

        // Driver creates inbox
        let inbox =
            Consumer::<ClientId, DRIVER_INBOX_CAPACITY, Creator>::create(inbox_path.clone())
                .expect("inbox creation");

        // Client generates ID and creates its channels
        let client_id = ClientId::generate();
        let (tx_path, rx_path) = channel_paths(&client_id);

        let _client_tx = Producer::<ClientMessage, CLIENT_QUEUE_CAPACITY, Creator>::create(tx_path)
            .expect("client tx");
        let _client_rx = Consumer::<DriverMessage, CLIENT_QUEUE_CAPACITY, Creator>::create(rx_path)
            .expect("client rx");

        // Client pushes ID to driver inbox
        let inbox_producer = Producer::<ClientId, DRIVER_INBOX_CAPACITY, Opener>::open(inbox_path)
            .expect("inbox open");
        inbox_producer.push(client_id).expect("push to inbox");

        // Driver receives the registration
        let received_id = inbox.pop().expect("should receive client ID");
        assert_eq!(received_id, client_id);
    }

    #[test]
    fn test_protocol_channel_rendezvous() {
        // Client creates channels with paths derived from its ID
        let client_id = ClientId::generate();
        let (tx_path, rx_path) = channel_paths(&client_id);

        // Client creates both queues (Creator = owner)
        let client_tx =
            Producer::<ClientMessage, CLIENT_QUEUE_CAPACITY, Creator>::create(tx_path.clone())
                .expect("client tx");
        let client_rx =
            Consumer::<DriverMessage, CLIENT_QUEUE_CAPACITY, Creator>::create(rx_path.clone())
                .expect("client rx");

        // Driver opens the same queues (Opener = user)
        // Note: Driver writes to client's RX, reads from client's TX
        let driver_tx = Producer::<DriverMessage, CLIENT_QUEUE_CAPACITY, Opener>::open(rx_path)
            .expect("driver tx");
        let driver_rx = Consumer::<ClientMessage, CLIENT_QUEUE_CAPACITY, Opener>::open(tx_path)
            .expect("driver rx");

        // Verify bidirectional communication works
        client_tx
            .push(ClientMessage::Command(ClientCommand::Heartbeat))
            .expect("client send");
        assert_eq!(
            driver_rx.pop(),
            Some(ClientMessage::Command(ClientCommand::Heartbeat))
        );

        driver_tx
            .push(DriverMessage::Heartbeat)
            .expect("driver send");
        assert_eq!(client_rx.pop(), Some(DriverMessage::Heartbeat));
    }

    #[test]
    fn test_protocol_handshake_hello_welcome() {
        let client_id = ClientId::generate();
        let (tx_path, rx_path) = channel_paths(&client_id);

        // Client creates channels
        let client_tx =
            Producer::<ClientMessage, CLIENT_QUEUE_CAPACITY, Creator>::create(tx_path.clone())
                .expect("client tx");
        let client_rx =
            Consumer::<DriverMessage, CLIENT_QUEUE_CAPACITY, Creator>::create(rx_path.clone())
                .expect("client rx");

        // Step 1: Client sends Hello with its ID (proof of ownership)
        client_tx
            .push(ClientMessage::Hello(ClientHello { id: client_id }))
            .expect("send Hello");

        // Driver opens channels
        let driver_tx = Producer::<DriverMessage, CLIENT_QUEUE_CAPACITY, Opener>::open(rx_path)
            .expect("driver tx");
        let driver_rx = Consumer::<ClientMessage, CLIENT_QUEUE_CAPACITY, Opener>::open(tx_path)
            .expect("driver rx");

        // Step 2: Driver verifies Hello contains correct ID
        match driver_rx.pop() {
            Some(ClientMessage::Hello(ClientHello { id })) => {
                assert_eq!(id, client_id, "ID in Hello must match registration");
            }
            other => panic!("Expected Hello, got {:?}", other),
        }

        // Step 3: Driver sends Welcome
        driver_tx
            .push(DriverMessage::Welcome)
            .expect("send Welcome");

        // Step 4: Client receives Welcome (connection established)
        assert_eq!(client_rx.pop(), Some(DriverMessage::Welcome));
    }

    #[test]
    fn test_protocol_handshake_wrong_id_rejected() {
        let client_id = ClientId::generate();
        let wrong_id = ClientId::generate();
        let (tx_path, rx_path) = channel_paths(&client_id);

        // Client creates channels
        let client_tx =
            Producer::<ClientMessage, CLIENT_QUEUE_CAPACITY, Creator>::create(tx_path.clone())
                .expect("client tx");
        let _client_rx =
            Consumer::<DriverMessage, CLIENT_QUEUE_CAPACITY, Creator>::create(rx_path.clone())
                .expect("client rx");

        // Client sends Hello with WRONG ID
        client_tx
            .push(ClientMessage::Hello(ClientHello { id: wrong_id }))
            .expect("send Hello");

        // Driver opens and checks
        let driver_rx = Consumer::<ClientMessage, CLIENT_QUEUE_CAPACITY, Opener>::open(tx_path)
            .expect("driver rx");

        match driver_rx.pop() {
            Some(ClientMessage::Hello(ClientHello { id })) => {
                assert_ne!(id, client_id, "Wrong ID should not match");
                // Driver would reject this connection
            }
            other => panic!("Expected Hello, got {:?}", other),
        }
    }

    #[test]
    fn test_protocol_handshake_no_hello_rejected() {
        let client_id = ClientId::generate();
        let (tx_path, rx_path) = channel_paths(&client_id);

        // Client creates channels but sends Heartbeat instead of Hello
        let client_tx =
            Producer::<ClientMessage, CLIENT_QUEUE_CAPACITY, Creator>::create(tx_path.clone())
                .expect("client tx");
        let _client_rx =
            Consumer::<DriverMessage, CLIENT_QUEUE_CAPACITY, Creator>::create(rx_path.clone())
                .expect("client rx");

        client_tx
            .push(ClientMessage::Command(ClientCommand::Heartbeat))
            .expect("send Heartbeat");

        // Driver opens and checks
        let driver_rx = Consumer::<ClientMessage, CLIENT_QUEUE_CAPACITY, Opener>::open(tx_path)
            .expect("driver rx");

        match driver_rx.pop() {
            Some(ClientMessage::Hello(_)) => panic!("Should not be Hello"),
            Some(other) => {
                // Driver would reject - first message must be Hello
                assert_eq!(other, ClientMessage::Command(ClientCommand::Heartbeat));
            }
            None => panic!("Should have message"),
        }
    }

    #[test]
    fn test_protocol_full_handshake_concurrent() {
        let inbox_path_str = unique_inbox_path("handshake");
        let _ = rustix::shm::unlink(&inbox_path_str);
        let inbox_path = ShmPath::new(inbox_path_str).unwrap();

        let driver_ready = Arc::new(AtomicBool::new(false));
        let driver_ready_clone = driver_ready.clone();

        let driver_inbox_path = inbox_path.clone();

        // Driver thread
        let driver_handle = thread::spawn(move || {
            let inbox =
                Consumer::<ClientId, DRIVER_INBOX_CAPACITY, Creator>::create(driver_inbox_path)
                    .expect("inbox");

            driver_ready_clone.store(true, Ordering::Release);

            // Wait for client registration
            let client_id = loop {
                if let Some(id) = inbox.pop() {
                    break id;
                }
                std::hint::spin_loop();
            };

            // Open client's channels
            let (tx_path, rx_path) = channel_paths(&client_id);
            let driver_tx = Producer::<DriverMessage, CLIENT_QUEUE_CAPACITY, Opener>::open(rx_path)
                .expect("driver tx");
            let driver_rx = Consumer::<ClientMessage, CLIENT_QUEUE_CAPACITY, Opener>::open(tx_path)
                .expect("driver rx");

            // Verify Hello
            match driver_rx.pop() {
                Some(ClientMessage::Hello(ClientHello { id })) if id == client_id => {}
                other => panic!("Expected Hello with correct ID, got {:?}", other),
            }

            // Send Welcome
            driver_tx.push(DriverMessage::Welcome).expect("Welcome");

            client_id
        });

        // Wait for driver to be ready
        while !driver_ready.load(Ordering::Acquire) {
            std::hint::spin_loop();
        }

        // Client thread (inline)
        let client_id = ClientId::generate();
        let (tx_path, rx_path) = channel_paths(&client_id);

        let client_tx =
            Producer::<ClientMessage, CLIENT_QUEUE_CAPACITY, Creator>::create(tx_path.clone())
                .expect("client tx");
        let client_rx =
            Consumer::<DriverMessage, CLIENT_QUEUE_CAPACITY, Creator>::create(rx_path.clone())
                .expect("client rx");

        // Send Hello first (proof of ownership)
        client_tx
            .push(ClientMessage::Hello(ClientHello { id: client_id }))
            .expect("Hello");

        // Register with driver
        let inbox_producer =
            Producer::<ClientId, DRIVER_INBOX_CAPACITY, Opener>::open(inbox_path).expect("inbox");
        inbox_producer.push(client_id).expect("register");

        // Wait for Welcome
        let welcome = loop {
            if let Some(msg) = client_rx.pop() {
                break msg;
            }
            std::hint::spin_loop();
        };
        assert_eq!(welcome, DriverMessage::Welcome);

        // Driver should have seen the same client ID
        let driver_saw_id = driver_handle.join().expect("driver thread");
        assert_eq!(driver_saw_id, client_id);
    }
}
