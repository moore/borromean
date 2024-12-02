use super::*;

use crate::io::mem_io::MemRegionAddress;
use crate::vec_like::VecLikeSlice;

#[test]
fn test_new_channel() {
    let id = CollectionId(1);
    let member = MemberId { id: 1 };

    let mut members_data = [MemberSequence {
        member: MemberId { id: 0 },
        last_sequence: ChannelSequence(0),
    }; 1024];
    let mut members = VecLikeSlice::new(&mut members_data);

    let mut updates_data = [MemberId { id: 0 }; 1024];
    let mut updates = VecLikeSlice::new(&mut updates_data);

    let mut pending_data: [_; 1024]  = core::array::from_fn(|_| AddCommand::<MemRegionAddress, 8> {
        prior: CommandAddress::zero(),
        sender_last: ChannelSequence(0),
        sequence: ChannelSequence(0),
        author: MemberId { id: 0 },
        message_id: MessageId { id: 0 },
        payload: Vec::new(),
    });
    let mut pending = VecLikeSlice::new(&mut pending_data);

    let channel = Channel::<_, _, _, _, 8, 1>::new(
        id, 
        member,
        &mut pending,
        &mut members,
        &mut updates,
    );
    assert!(channel.is_ok());

    let channel = channel.unwrap();
    assert_eq!(channel.id, id);
    assert_eq!(channel.next_sequence, ChannelSequence(0));
    assert_eq!(channel.members.len(), 1);
    assert_eq!(channel.members.get(0).unwrap().member, member);
    assert_eq!(channel.members.get(0).unwrap().last_sequence, ChannelSequence(0));
}

#[test]
fn test_add_member() {
    let id = CollectionId(1);
    let initial_member = MemberId { id: 1 };
    let new_member = MemberId { id: 2 };

    let mut members_data = [MemberSequence {
        member: MemberId { id: 0 },
        last_sequence: ChannelSequence(0),
    }; 2];
    let mut members = VecLikeSlice::new(&mut members_data);

    let mut updates_data = [MemberId { id: 0 }; 1];
    let mut updates = VecLikeSlice::new(&mut updates_data);

    let mut pending_data = [AddCommand::<MemRegionAddress, 8> {
        prior: CommandAddress::zero(),
        sender_last: ChannelSequence(0),
        sequence: ChannelSequence(0),
        author: MemberId { id: 0 },
        message_id: MessageId { id: 0 },
        payload: Vec::new(),
    }; 1];
    let mut pending = VecLikeSlice::new(&mut pending_data);

    let mut channel = Channel::<_, _, _, _, 8, 2>::new(
        id,
        initial_member,
        &mut pending,
        &mut members,
        &mut updates,
    ).unwrap();

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

    let mut members_data = [MemberSequence {
        member: MemberId { id: 0 },
        last_sequence: ChannelSequence(0),
    }; 2];
    let mut members = VecLikeSlice::new(&mut members_data);

    let mut updates_data = [MemberId { id: 0 }; 1];
    let mut updates = VecLikeSlice::new(&mut updates_data);

    let mut pending_data = [AddCommand::<MemRegionAddress, 8> {
        prior: CommandAddress::zero(),
        sender_last: ChannelSequence(0),
        sequence: ChannelSequence(0),
        author: MemberId { id: 0 },
        message_id: MessageId { id: 0 },
        payload: Vec::new(),
    }; 1];
    let mut pending = VecLikeSlice::new(&mut pending_data);

    let mut channel = Channel::<_, _, _, _, 8, 2>::new(
        id,
        initial_member,
        &mut pending,
        &mut members,
        &mut updates,
    ).unwrap();

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
    let initial_member = MemberId { id: 1 };

    let mut members_data = [MemberSequence {
        member: MemberId { id: 0 },
        last_sequence: ChannelSequence(0),
    }; 1];
    let mut members = VecLikeSlice::new(&mut members_data);

    let mut updates_data = [MemberId { id: 0 }; 1];
    let mut updates = VecLikeSlice::new(&mut updates_data);

    let mut pending_data = [AddCommand::<MemRegionAddress, 8> {
        prior: CommandAddress::zero(),
        sender_last: ChannelSequence(0),
        sequence: ChannelSequence(0),
        author: MemberId { id: 0 },
        message_id: MessageId { id: 0 },
        payload: Vec::new(),
    }; 1];
    let mut pending = VecLikeSlice::new(&mut pending_data);

    let mut channel = Channel::<_, _, _, _, 8, 1>::new(
        id,
        initial_member,
        &mut pending,
        &mut members,
        &mut updates,
    ).unwrap();

    // Initial sequence should be 0
    let seq = channel.get_last_sequence(&initial_member);
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
    let initial_member = MemberId { id: 1 };

    let mut members_data = [MemberSequence {
        member: MemberId { id: 0 },
        last_sequence: ChannelSequence(0),
    }; 1];
    let mut members = VecLikeSlice::new(&mut members_data);

    let mut updates_data = [MemberId { id: 0 }; 1];
    let mut updates = VecLikeSlice::new(&mut updates_data);

    let mut pending_data = [AddCommand::<MemRegionAddress, 8> {
        prior: CommandAddress::zero(),
        sender_last: ChannelSequence(0),
        sequence: ChannelSequence(0),
        author: MemberId { id: 0 },
        message_id: MessageId { id: 0 },
        payload: Vec::new(),
    }; 1];
    let mut pending = VecLikeSlice::new(&mut pending_data);

    let mut channel = Channel::<_, _, _, _, 8, 1>::new(
        id,
        initial_member,
        &mut pending,
        &mut members,
        &mut updates,
    ).unwrap();

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

    let mut members_data = [MemberSequence {
        member: MemberId { id: 0 },
        last_sequence: ChannelSequence(0),
    }; 1];
    let mut members = VecLikeSlice::new(&mut members_data);

    let mut updates_data = [MemberId { id: 0 }; 1];
    let mut updates = VecLikeSlice::new(&mut updates_data);

    let mut pending_data = [AddCommand::<MemRegionAddress, 8> {
        prior: CommandAddress::zero(),
        sender_last: ChannelSequence(0),
        sequence: ChannelSequence(0),
        author: MemberId { id: 0 },
        message_id: MessageId { id: 0 },
        payload: Vec::new(),
    }; 1];
    let mut pending = VecLikeSlice::new(&mut pending_data);

    let mut channel = Channel::<_, _, _, _, 8, 1>::new(
        id,
        member,
        &mut pending,
        &mut members,
        &mut updates,
    ).unwrap();

    // Adding same member again should succeed but not create duplicate
    let result = channel.add_member(member);
    assert!(result.is_ok());
    assert_eq!(channel.members.len(), 1);
    assert_eq!(channel.members.get(0).unwrap().member, member);
}
