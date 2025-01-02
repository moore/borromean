pub mod wal;
pub use wal::*;

pub mod channel;
pub use channel::*;

pub mod map;
pub use map::*;

use crate::io::RegionAddress;

enum Commands<A: RegionAddress, const PAYLOAD_MAX: usize, const MEMBER_LIMIT: usize> {
    Channel(ChannelCommand<A, PAYLOAD_MAX, MEMBER_LIMIT>),
}
