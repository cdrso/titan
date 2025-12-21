//! Type-safe wrappers for SPSC producers and consumers over shared memory.
//!
//! These wrappers encapsulate the serialization/deserialization of user-defined
//! message types (`T`) into and out of `Frame`s
use crate::data::{Frame, FrameError, Wire};
use crate::ipc::shmem::{ShmError, ShmMode};
use crate::ipc::spsc::{Consumer as SpscConsumer, Producer as SpscProducer, Timeout};
use thiserror::Error;

/// Errors that can occur when sending/receiving typed messages.
#[derive(Debug, Error)]
pub enum TypedChannelError {
    /// An error occurred during shared memory operations or SPSC queue.
    #[error("shared memory error: {0}")]
    Shm(ShmError),
    /// An error occurred during message serialization/deserialization.
    #[error("frame serialization error: {0}")]
    Frame(FrameError),
    /// The channel operation timed out.
    #[error("channel operation timed out")]
    Timeout,
    /// Message queue is full.
    #[error("message queue is full")]
    QueueFull,
}

impl From<ShmError> for TypedChannelError {
    fn from(err: ShmError) -> Self {
        Self::Shm(err)
    }
}

impl From<FrameError> for TypedChannelError {
    fn from(err: FrameError) -> Self {
        Self::Frame(err)
    }
}

/// Type-safe producer for a data channel.
///
/// Wraps an SPSC producer for `Frame`s, handling the serialization of messages
/// of type `T` into frames.
pub struct MessageSender<T: Wire, const FRAME_CAP: usize, const QUEUE_CAP: usize, Mode: ShmMode> {
    producer: SpscProducer<Frame<FRAME_CAP>, QUEUE_CAP, Mode>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: Wire, const FRAME_CAP: usize, const QUEUE_CAP: usize, Mode: ShmMode>
    MessageSender<T, FRAME_CAP, QUEUE_CAP, Mode>
{
    /// Creates a new typed producer wrapping an SPSC producer.
    #[must_use]
    pub const fn new(producer: SpscProducer<Frame<FRAME_CAP>, QUEUE_CAP, Mode>) -> Self {
        Self {
            producer,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Attempts to send a message (wait-free).
    ///
    /// # Errors
    ///
    /// Returns `Err(TypedChannelError::Frame)` if serialization fails.
    /// Returns `Err(TypedChannelError::QueueFull)` if the underlying queue is full.
    pub fn send(&self, msg: &T) -> Result<(), TypedChannelError> {
        let mut frame = Frame::new();
        frame.encode(msg).map_err(TypedChannelError::Frame)?;
        self.producer
            .push(frame)
            .map_err(|_| TypedChannelError::QueueFull)
    }

    /// Sends a message, blocking until space is available.
    ///
    /// # Errors
    ///
    /// Returns `Err(TypedChannelError::Frame)` if serialization fails.
    /// Returns `Err(TypedChannelError::Timeout)` on timeout.
    pub fn send_blocking(&self, msg: &T, timeout: Timeout) -> Result<(), TypedChannelError> {
        let mut frame = Frame::new();
        frame.encode(msg).map_err(TypedChannelError::Frame)?;
        self.producer
            .push_blocking(frame, timeout)
            .map_err(|_| TypedChannelError::Timeout)
    }
}

/// Type-safe consumer for a data channel.
///
/// Wraps an SPSC consumer for `Frame`s, handling the deserialization of messages
/// of type `T` from frames.
pub struct MessageReceiver<T: Wire, const FRAME_CAP: usize, const QUEUE_CAP: usize, Mode: ShmMode> {
    consumer: SpscConsumer<Frame<FRAME_CAP>, QUEUE_CAP, Mode>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: Wire, const FRAME_CAP: usize, const QUEUE_CAP: usize, Mode: ShmMode>
    MessageReceiver<T, FRAME_CAP, QUEUE_CAP, Mode>
{
    /// Creates a new typed consumer wrapping an SPSC consumer.
    #[must_use]
    pub const fn new(consumer: SpscConsumer<Frame<FRAME_CAP>, QUEUE_CAP, Mode>) -> Self {
        Self {
            consumer,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Attempts to receive a message (wait-free).
    ///
    /// Returns `None` if the queue is empty.
    /// Returns `Some(Err)` if deserialization from frame fails.
    #[must_use]
    pub fn recv(&self) -> Option<Result<T, TypedChannelError>> {
        self.consumer
            .pop()
            .map(|frame| frame.decode().map_err(TypedChannelError::Frame))
    }

    /// Receives a message, blocking until available.
    ///
    /// Returns `None` on timeout.
    /// Returns `Some(Err)` if deserialization from frame fails.
    #[must_use]
    pub fn recv_blocking(&self, timeout: Timeout) -> Option<Result<T, TypedChannelError>> {
        self.consumer
            .pop_blocking(timeout)
            .map(|frame| frame.decode().map_err(TypedChannelError::Frame))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ipc::shmem::{Creator, ShmPath};
    use crate::ipc::spsc::{Consumer, Producer};
    use serde::{Deserialize, Serialize};
    use std::time::Duration;
    use type_hash::TypeHash;

    const TEST_CAP: usize = 64;
    const TEST_QUEUE: usize = 16;

    #[derive(Serialize, Deserialize, TypeHash, Debug, PartialEq, Clone)]
    struct TestMsg {
        id: u32,
        data: [u8; 8],
    }

    fn unique_path(name: &str) -> ShmPath {
        let nonce: u32 = rand::random();
        ShmPath::new(format!("/test-chan-{name}-{nonce}")).unwrap()
    }

    #[test]
    fn typed_producer_consumer_roundtrip() {
        let path = unique_path("roundtrip");

        let raw_producer =
            Producer::<Frame<TEST_CAP>, TEST_QUEUE, Creator>::create(path.clone()).unwrap();
        let raw_consumer = Consumer::<Frame<TEST_CAP>, TEST_QUEUE, _>::open(path).unwrap();

        let producer: MessageSender<TestMsg, TEST_CAP, TEST_QUEUE, _> =
            MessageSender::new(raw_producer);
        let consumer: MessageReceiver<TestMsg, TEST_CAP, TEST_QUEUE, _> =
            MessageReceiver::new(raw_consumer);

        let msg = TestMsg {
            id: 42,
            data: [1, 2, 3, 4, 5, 6, 7, 8],
        };

        producer.send(&msg).unwrap();

        let received = consumer.recv().unwrap().unwrap();
        assert_eq!(received, msg);
    }

    #[test]
    fn typed_recv_empty_returns_none() {
        let path = unique_path("empty");

        let raw_producer =
            Producer::<Frame<TEST_CAP>, TEST_QUEUE, Creator>::create(path.clone()).unwrap();
        let raw_consumer = Consumer::<Frame<TEST_CAP>, TEST_QUEUE, _>::open(path).unwrap();

        let _producer: MessageSender<TestMsg, TEST_CAP, TEST_QUEUE, _> =
            MessageSender::new(raw_producer);
        let consumer: MessageReceiver<TestMsg, TEST_CAP, TEST_QUEUE, _> =
            MessageReceiver::new(raw_consumer);

        assert!(consumer.recv().is_none());
    }

    #[test]
    fn typed_send_multiple_messages() {
        let path = unique_path("multi");

        let raw_producer =
            Producer::<Frame<TEST_CAP>, TEST_QUEUE, Creator>::create(path.clone()).unwrap();
        let raw_consumer = Consumer::<Frame<TEST_CAP>, TEST_QUEUE, _>::open(path).unwrap();

        let producer: MessageSender<TestMsg, TEST_CAP, TEST_QUEUE, _> =
            MessageSender::new(raw_producer);
        let consumer: MessageReceiver<TestMsg, TEST_CAP, TEST_QUEUE, _> =
            MessageReceiver::new(raw_consumer);

        for i in 0..5 {
            let msg = TestMsg {
                id: i,
                data: [i as u8; 8],
            };
            producer.send(&msg).unwrap();
        }

        for i in 0..5 {
            let received = consumer.recv().unwrap().unwrap();
            assert_eq!(received.id, i);
        }

        assert!(consumer.recv().is_none());
    }

    #[test]
    fn typed_queue_full_error() {
        let path = unique_path("full");

        // Queue capacity of 4
        let raw_producer = Producer::<Frame<TEST_CAP>, 4, Creator>::create(path.clone()).unwrap();
        let _raw_consumer = Consumer::<Frame<TEST_CAP>, 4, _>::open(path).unwrap();

        let producer: MessageSender<TestMsg, TEST_CAP, 4, _> = MessageSender::new(raw_producer);

        let msg = TestMsg {
            id: 0,
            data: [0; 8],
        };

        // Fill the queue (SPSC with capacity N can hold N items)
        for _ in 0..4 {
            producer.send(&msg).unwrap();
        }

        // Next send should fail
        let result = producer.send(&msg);
        assert!(matches!(result, Err(TypedChannelError::QueueFull)));
    }

    #[test]
    fn typed_blocking_recv_timeout() {
        let path = unique_path("timeout");

        let raw_producer =
            Producer::<Frame<TEST_CAP>, TEST_QUEUE, Creator>::create(path.clone()).unwrap();
        let raw_consumer = Consumer::<Frame<TEST_CAP>, TEST_QUEUE, _>::open(path).unwrap();

        let _producer: MessageSender<TestMsg, TEST_CAP, TEST_QUEUE, _> =
            MessageSender::new(raw_producer);
        let consumer: MessageReceiver<TestMsg, TEST_CAP, TEST_QUEUE, _> =
            MessageReceiver::new(raw_consumer);

        let result = consumer.recv_blocking(Timeout::Duration(Duration::from_millis(10)));
        assert!(result.is_none());
    }

    #[test]
    fn typed_error_display() {
        let err = TypedChannelError::Timeout;
        assert_eq!(format!("{err}"), "channel operation timed out");

        let err = TypedChannelError::QueueFull;
        assert_eq!(format!("{err}"), "message queue is full");
    }
}
