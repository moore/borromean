pub mod wal;
pub use wal::*;

pub mod channel;
pub use channel::*;

use crate::io::IoBackend;

enum Commands<B: IoBackend, const PAYLOAD_MAX: usize, const MEMBER_LIMIT: usize> {
    Channel(ChannelCommand<B, PAYLOAD_MAX, MEMBER_LIMIT>),
}
