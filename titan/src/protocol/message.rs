/// Message types soported by the protocol, used on the header so the receiver can interpret the
/// contents
#[repr(u8)]
pub enum MessageType {
    /// Data message
    Data,
    /// Ack the subscriber sends in response to a received packet from a publisher
    Ack,
    /// Nack the subscriber sends when it realizes there are missing packets from a publisher
    Nack,
    /// HB for connection monitoring
    HeartBeat,
    /// Round trip time meassurement
    Rttm,
}
