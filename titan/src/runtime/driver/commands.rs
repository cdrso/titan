//! Inter-thread command types for driver coordination.
//!
//! These are internal commands passed between the control, TX, and RX threads
//! via heap-backed SPSC queues. They are NOT part of the client-facing protocol.
//!
//! Command flows:
//! - Control → TX: Channel endpoint updates for outbound routing, protocol frames to send
//! - Control → RX: Channel mappings for inbound demuxing
//! - RX → Control: Incoming protocol frames (SETUP, SM, TEARDOWN)
//! - RX → TX: Reliability events (ack/nack) for future retransmission

use crate::control::types::{ChannelId, ClientId, TypeId};
use crate::data::Frame;
use crate::ipc::shmem::Opener;
use crate::ipc::spsc::{Consumer as IpcConsumer, Producer as IpcProducer};
use crate::net::Endpoint;
use crate::runtime::driver::protocol::{NakReason, SessionId};

/// Key for identifying a remote data stream.
/// Combines the remote endpoint with the remote channel ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RemoteStreamKey {
    pub endpoint: Endpoint,
    pub channel: ChannelId,
}

/// Capacity for inter-thread command queues.
///
/// These queues carry control-plane updates (cold path), not data.
/// Sized to handle bursts of channel opens/closes without backpressure.
pub const COMMAND_QUEUE_CAPACITY: usize = 256;

/// Default frame capacity for data channels.
pub const DEFAULT_FRAME_CAP: usize = crate::data::DEFAULT_FRAME_CAP;

/// Default data queue capacity.
pub const DATA_QUEUE_CAPACITY: usize = crate::control::types::DATA_QUEUE_CAPACITY;

/// IPC queue for receiving data from a client (client TX → driver RX).
///
/// The driver opens queues created by clients, so it uses Opener mode.
pub type ClientTxQueue = IpcConsumer<Frame<DEFAULT_FRAME_CAP>, DATA_QUEUE_CAPACITY, Opener>;

/// IPC queue for sending data to a client (driver TX → client RX).
pub type ClientRxQueue = IpcProducer<Frame<DEFAULT_FRAME_CAP>, DATA_QUEUE_CAPACITY, Opener>;

/// Commands from control thread to TX thread.
///
/// These update the TX thread's local routing table for outbound data.
pub enum TxCommand {
    /// Register a new outbound channel.
    ///
    /// The TX thread should add this channel to its routing table and
    /// start draining the client's TX queue.
    AddChannel {
        /// Unique identifier for this channel.
        channel: ChannelId,
        /// The client that owns this channel.
        client: ClientId,
        /// Queue to drain for outbound messages (client → driver).
        queue: ClientTxQueue,
        /// Network endpoints to fan-out to.
        endpoints: Vec<Endpoint>,
    },

    /// Remove a channel from the routing table.
    ///
    /// The TX thread should stop processing this channel and drop its queue.
    RemoveChannel {
        /// Channel to remove.
        channel: ChannelId,
    },

    /// Update the endpoint list for a channel.
    ///
    /// Used when endpoints change (e.g., peer migration, multicast group change).
    UpdateEndpoints {
        /// Channel to update.
        channel: ChannelId,
        /// New endpoint list (replaces existing).
        endpoints: Vec<Endpoint>,
    },

    /// Add a subscriber to a published channel.
    ///
    /// The TX thread should start sending DATA frames for this channel
    /// to the subscriber's endpoint.
    AddSubscriber {
        /// Channel being subscribed to.
        channel: ChannelId,
        /// Subscriber's session ID (for SM correlation).
        session: SessionId,
        /// Subscriber's endpoint.
        endpoint: Endpoint,
        /// Subscriber's initial receiver window.
        receiver_window: u32,
    },

    /// Remove a subscriber from a published channel.
    RemoveSubscriber {
        /// Channel to remove subscriber from.
        channel: ChannelId,
        /// Subscriber's session ID.
        session: SessionId,
    },

    /// Send a protocol frame to a remote endpoint.
    ///
    /// Used for `SETUP_ACK`, `SETUP_NAK`, and outgoing SETUP frames.
    SendProtocolFrame {
        /// Destination endpoint.
        endpoint: Endpoint,
        /// Pre-encoded frame bytes.
        frame_bytes: Vec<u8>,
    },

    /// Shutdown signal - TX thread should exit its loop.
    Shutdown,
}

/// Commands from control thread to RX thread.
///
/// These update the RX thread's local demux table for inbound data.
pub enum RxCommand {
    /// Register a new inbound channel.
    ///
    /// The RX thread should add this channel to its demux table so
    /// incoming packets can be routed to the client's RX queue.
    AddChannel {
        /// Unique identifier for this channel.
        channel: ChannelId,
        /// The client that owns this channel.
        client: ClientId,
        /// Queue to push inbound messages to (driver → client).
        queue: ClientRxQueue,
    },

    /// Remove a channel from the demux table.
    ///
    /// The RX thread should stop routing to this channel and drop its queue.
    RemoveChannel {
        /// Channel to remove.
        channel: ChannelId,
    },

    /// Map a remote stream to a local channel.
    ///
    /// When DATA frames arrive from the specified remote endpoint + channel,
    /// route them to the local channel's client queue.
    AddRemoteMapping {
        /// Remote endpoint + channel that will send DATA.
        remote: RemoteStreamKey,
        /// Local channel to route DATA to.
        local_channel: ChannelId,
    },

    /// Remove a remote stream mapping.
    RemoveRemoteMapping {
        /// Remote stream to stop routing.
        remote: RemoteStreamKey,
    },

    /// Shutdown signal - RX thread should exit its loop.
    Shutdown,
}

/// Events from RX thread to TX thread.
///
/// These carry reliability feedback for future retransmission support.
/// Initially (best-effort mode), these may be ignored or minimally processed.
pub enum RxToTxEvent {
    /// Acknowledgment received for a sequence number.
    Ack {
        /// Channel the ack is for.
        channel: ChannelId,
        /// Acknowledged sequence number.
        seq: u64,
    },

    /// Negative acknowledgment / gap detected.
    Nack {
        /// Channel the nack is for.
        channel: ChannelId,
        /// Missing sequence number.
        seq: u64,
    },

    /// Heartbeat received from peer.
    Heartbeat {
        /// Channel the heartbeat is for.
        channel: ChannelId,
        /// Peer's next expected sequence number.
        next_expected: u64,
    },

    /// Status Message received from a subscriber (flow control update).
    StatusMessage {
        /// Publisher's session ID.
        session: SessionId,
        /// Subscriber's consumption offset.
        consumption_offset: u64,
        /// Subscriber's current receiver window.
        receiver_window: u32,
    },
}

/// Events from RX thread to Control thread.
///
/// These carry incoming protocol frames that require control-plane decisions.
pub enum RxToControlEvent {
    /// Incoming SETUP request from a remote subscriber.
    SetupRequest {
        /// Source endpoint of the subscriber.
        from: Endpoint,
        /// Subscriber's session ID (to echo in response).
        session: SessionId,
        /// Channel being subscribed to.
        channel: ChannelId,
        /// Expected type hash for validation.
        type_id: TypeId,
        /// Subscriber's receiver window.
        receiver_window: u32,
        /// Subscriber's MTU.
        mtu: u16,
    },

    /// Incoming `SETUP_ACK` from a remote publisher (subscription accepted).
    SetupAck {
        /// Source endpoint of the publisher.
        from: Endpoint,
        /// Our session ID (echoed back).
        session: SessionId,
        /// Publisher's session ID (for future SM).
        publisher_session: SessionId,
        /// Negotiated MTU.
        mtu: u16,
    },

    /// Incoming `SETUP_NAK` from a remote publisher (subscription rejected).
    SetupNak {
        /// Source endpoint of the publisher.
        from: Endpoint,
        /// Our session ID (echoed back).
        session: SessionId,
        /// Reason for rejection.
        reason: NakReason,
    },

    /// Incoming TEARDOWN from remote peer.
    Teardown {
        /// Source endpoint.
        from: Endpoint,
        /// Session being torn down.
        session: SessionId,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    // Ensure command types are Send (required for SPSC queues)
    fn _assert_send<T: Send>() {}

    #[test]
    fn commands_are_send() {
        _assert_send::<TxCommand>();
        _assert_send::<RxCommand>();
        _assert_send::<RxToTxEvent>();
        _assert_send::<RxToControlEvent>();
    }
}
