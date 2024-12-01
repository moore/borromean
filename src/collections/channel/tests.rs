use super::*;

use crate::io::mem_io::MemIo;

#[test]
fn test_new_channel() {
    let id = CollectionId(1);
    let member = MemberId { id: 1 };

    let channel = Channel::<MemIo<1024, 8, 1>, 1024, 8>::new(id, member);
    assert!(channel.is_ok());

    let channel = channel.unwrap();
    assert_eq!(channel.id, id);
    assert_eq!(channel.next_sequence, ChannelSequence(0));
    assert_eq!(channel.members.len(), 1);
    assert_eq!(channel.members[0].member, member);
    assert_eq!(channel.members[0].last_sequence, ChannelSequence(0));
}

#[test]
fn test_add_member() {
    let id = CollectionId(1);
    let initial_member = MemberId { id: 1 };
    let new_member = MemberId { id: 2 };

    let mut channel = Channel::<MemIo<1024, 8, 1>, 1024, 8>::new(id, initial_member).unwrap();

    let result = channel.add_member(new_member);
    assert!(result.is_ok());

    assert_eq!(channel.members.len(), 2);
    assert!(channel.members.iter().any(|m| m.member == initial_member));
    assert!(channel.members.iter().any(|m| m.member == new_member));
}
#[test]
fn test_add_member_limit() {
    let id = CollectionId(1);
    let initial_member = MemberId { id: 1 };

    // Create channel with small member limit of 2
    let mut channel = Channel::<MemIo<1024, 2, 1>, 1024, 2>::new(id, initial_member).unwrap();

    // Add one member should succeed
    let result = channel.add_member(MemberId { id: 2 });
    assert!(result.is_ok());

    // Adding third member should fail with UserLimitReached
    let result = channel.add_member(MemberId { id: 3 });
    assert!(matches!(result, Err(ChannelError::UserLimitReached)));
}

#[test]
fn test_get_last_sequence() {
    let id = CollectionId(1);
    let member = MemberId { id: 1 };
    let channel = Channel::<MemIo<1024, 8, 1>, 1024, 8>::new(id, member).unwrap();

    // Initial sequence should be 0
    let seq = channel.get_last_sequence(&member);
    assert!(seq.is_ok());
    assert_eq!(seq.unwrap(), ChannelSequence(0));

    // Non-existent member should return error
    let bad_member = MemberId { id: 999 };
    let seq = channel.get_last_sequence(&bad_member);
    assert!(matches!(seq, Err(ChannelError::MemberNotFound(_))));
}

#[test]
fn test_get_next_sequence() {
    let id = CollectionId(1);
    let member = MemberId { id: 1 };
    let mut channel = Channel::<MemIo<1024, 8, 1>, 1024, 8>::new(id, member).unwrap();

    // First call should return 0 and increment internal counter
    assert_eq!(channel.get_next_sequence(), ChannelSequence(0));
    assert_eq!(channel.next_sequence, ChannelSequence(1));

    // Second call should return 1 and increment internal counter
    assert_eq!(channel.get_next_sequence(), ChannelSequence(1));
    assert_eq!(channel.next_sequence, ChannelSequence(2));
}

#[test]
fn test_duplicate_member_add() {
    let id = CollectionId(1);
    let member = MemberId { id: 1 };
    let mut channel = Channel::<MemIo<1024, 8, 1>, 1024, 8>::new(id, member).unwrap();

    // Adding same member again should succeed but not create duplicate
    let result = channel.add_member(member);
    assert!(result.is_ok());
    assert_eq!(channel.members.len(), 1);
    assert_eq!(channel.members[0].member, member);
}
