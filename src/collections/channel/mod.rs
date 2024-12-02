use core::marker::PhantomData;

use heapless::Vec;

use crate::io::RegionAddress;
use crate::vec_like::VecLike;
use crate::CollectionId;
#[cfg(test)]
mod tests;

#[derive(Debug)]
pub enum ChannelError {
    UserLimitReached,
    MemberNotFound(MemberId),
    PendingLimitReached,
    NeedsCheckpoint,
}

///////////// basic types /////////////
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChannelSequence(u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemberId {
    id: u128,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageId {
    id: u128,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandAddress<A: RegionAddress> {
    pub region: A,
    pub offset: usize,
}

impl<A: RegionAddress> CommandAddress<A> {
    pub fn zero() -> Self {
        Self {
            region: A::zero(),
            offset: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub struct MemberSequence {
    member: MemberId,
    last_sequence: ChannelSequence,
}

///////////// Protocol /////////////

pub enum ChannelCommand<A: RegionAddress, const PAYLOAD_MAX: usize, const CHECKPOINT_MAX: usize> {
    AddCommand(AddCommand<A, PAYLOAD_MAX>),
    AddMemberCommand(AddMemberCommand<A>),
    CheckPointCommand(CheckPointCommand<A, CHECKPOINT_MAX>),
}

///////////// Add Command /////////////

/// Commands are a partially ordered sequence. Each uses both references
/// to prior commands and sequence numbers to provide ordering.
///
/// The `sender_last` and `sequence` provide a total ordering of commands
/// from a single sender. The sender last is used to detect missing messages
/// from a sender as we jump the `sequence` to be one larger that the largest
/// the sender has seen. This jumping is used to optimize searching and syncing
/// the channel.
///
/// NOTE: I could add a merge filed that would randomly pick another head
/// that this command dose not fallow as an approach to add stronger
/// ordering to the whole channel but I am not sure what problem
// doing so would solve so I am leaving it out for now.
#[derive(Debug, Clone)]
pub struct AddCommand<A: RegionAddress, const PAYLOAD_MAX: usize> {
    /// A command with a sequence one less than
    /// the sequence of this command. It should
    /// be equal to the largest sequence that the
    /// sender has seen.
    prior: CommandAddress<A>,
    /// The last sequence used by the sender.
    /// This is used to detect if we are missing
    /// any commands from the sender.
    sender_last: ChannelSequence,
    /// This command's sequence number. It should
    /// be one greater than any sequence number seen
    /// by the sender and greater then any sequence
    /// number use by the sender previously.
    sequence: ChannelSequence,
    /// The member id of the author of the command.
    author: MemberId,
    /// The message id of the command.
    message_id: MessageId,
    /// The payload of the command.
    payload: Vec<u8, PAYLOAD_MAX>,
}

impl<A: RegionAddress, const PAYLOAD_MAX: usize> AddCommand<A, PAYLOAD_MAX> {
    pub fn new<const MEMBER_LIMIT: usize>(
        prior: CommandAddress<A>,
        sender_last: ChannelSequence,
        sequence: ChannelSequence,
        author: MemberId,
        message_id: MessageId,
        payload: Vec<u8, PAYLOAD_MAX>,
    ) -> ChannelCommand<A, PAYLOAD_MAX, MEMBER_LIMIT> {
        ChannelCommand::AddCommand(Self {
            prior,
            sender_last,
            sequence,
            author,
            message_id,
            payload,
        })
    }
}

///////////// Add Member Command /////////////
pub struct AddMemberCommand<A: RegionAddress> {
    member: MemberId,
    phantom: PhantomData<A>,
}

impl<A: RegionAddress> AddMemberCommand<A> {
    pub fn new<const PAYLOAD_MAX: usize, const MEMBER_LIMIT: usize>(
        member: MemberId,
    ) -> ChannelCommand<A, PAYLOAD_MAX, MEMBER_LIMIT> {
        ChannelCommand::AddMemberCommand(Self {
            member,
            phantom: PhantomData,
        })
    }
}

///////////// CheckPoint Command /////////////

/// A check point is used when a one needs to talk about which devices
/// have sent which commands in a way that allow only describing recent
/// changes.
pub struct CheckPointCommand<A: RegionAddress, const USER_LIMIT: usize> {
    /// The checkpoint that this builds on
    previous_checkpoint: CommandAddress<A>,
    /// This is the total number of commands in the channel on this
    /// device up to this checkpoint.
    command_count: u64,
    /// This should include at least all the changes since
    /// the last checkpoint, but may include other changes
    /// to prevent the search for changes from getting too
    /// deep.
    sequences: Vec<MemberSequence, USER_LIMIT>,
}

impl<A: RegionAddress, const MEMBER_LIMIT: usize> CheckPointCommand<A, MEMBER_LIMIT> {
    pub fn new<const PAYLOAD_MAX: usize>(
        previous_checkpoint: CommandAddress<A>,
        command_count: u64,
        sequences: &Vec<MemberSequence, MEMBER_LIMIT>,
    ) -> ChannelCommand<A, PAYLOAD_MAX, MEMBER_LIMIT> {
        ChannelCommand::CheckPointCommand(Self {
            previous_checkpoint,
            command_count,
            sequences: sequences.clone(),
        })
    }
}

///////////// Channel State /////////////

/// The channel is represented by an ordered set of regions. Each region has a pointer
/// the next and previous region in the channel. The header of the next region is not 
/// written until if is full at which point it becomes the head region of the channel. 
/// Because the header of then next channel is not written prior to becoming the head
/// It will have a lower region sequence number than the current head region and so what ever
/// stale information it contains will be ignored. To track how much of the next region is
/// used the current head also references a WAL to track updates to the next region.
/// 
/// We wright commands in to the next segment instead of the WAL so that they have a
/// stable address.
pub struct Channel<
    'a,
    'b,
    'c,
    A: RegionAddress,
    M: VecLike<MemberSequence>,
    U: VecLike<MemberId>,
    P: VecLike<AddCommand<A, PAYLOAD_MAX>>,
    const PAYLOAD_MAX: usize,
    const CHECKPOINT_MAX: usize,
> {
    id: CollectionId,
    //next_region: A,
    //previous_region: Option<A>,
    //wal: A,
    //next_wal: Option<A>,
    next_sequence: ChannelSequence,
    members: &'a mut M,
    checkpoint: CommandAddress<A>,
    updates: &'b mut U,
    pending: &'c mut P,
}

impl<
        'a,
        'b,
        'c,
        A: RegionAddress,
        M: VecLike<MemberSequence>,
        U: VecLike<MemberId>,
        P: VecLike<AddCommand<A, PAYLOAD_MAX>>,
        const PAYLOAD_MAX: usize,
        const CHECKPOINT_MAX: usize,
    > Channel<'a, 'b, 'c, A, M, U, P, PAYLOAD_MAX, CHECKPOINT_MAX>
{
    pub fn new(
        id: CollectionId,
        initial_member: MemberId,
        pending: &'c mut P,
        members: &'a mut M,
        updates: &'b mut U,
    ) -> Result<Self, ChannelError> {
        let member_sequence = MemberSequence {
            member: initial_member,
            last_sequence: ChannelSequence(0),
        };
        let Ok(_) = members.push(member_sequence) else {
            return Err(ChannelError::UserLimitReached);
        };

        Ok(Self {
            id,
            next_sequence: ChannelSequence(0),
            members,
            checkpoint: CommandAddress::zero(),
            updates,
            pending,
        })
    }

    pub fn add_member(
        &mut self,
        member: MemberId,
    ) -> Result<ChannelCommand<A, PAYLOAD_MAX, CHECKPOINT_MAX>, ChannelError> {
        let command = AddMemberCommand::new(member);
        self.apply_command(&command)?;

        Ok(command)
    }

    pub fn add_command(
        &mut self,
        prior: CommandAddress<A>,
        author: MemberId,
        message_id: MessageId,
        payload: Vec<u8, PAYLOAD_MAX>,
    ) -> Result<ChannelCommand<A, PAYLOAD_MAX, CHECKPOINT_MAX>, ChannelError> {
        let sender_last = self.get_last_sequence(&author)?;
        let sequence = self.get_next_sequence();
        let command = AddCommand::new(prior, sender_last, sequence, author, message_id, payload);
        self.apply_command(&command)?;

        Ok(command)
    }

    pub(crate) fn apply_command(
        &mut self,
        command: &ChannelCommand<A, PAYLOAD_MAX, CHECKPOINT_MAX>,
    ) -> Result<(), ChannelError> {
        match command {
            ChannelCommand::AddMemberCommand(command) => {
                if !self.members.iter().any(|m| m.member == command.member) {
                    let member_sequence = MemberSequence {
                        member: command.member,
                        last_sequence: ChannelSequence(0),
                    };
                    let Ok(_) = self.members.push(member_sequence) else {
                        return Err(ChannelError::UserLimitReached);
                    };
                }

                Ok(())
            }
            ChannelCommand::AddCommand(command) => {

                // TODO: check that all the sequences and such are valid.


                
                let pending_command = command.clone();
                let Ok(_) = self.pending.push(pending_command) else {
                    return Err(ChannelError::PendingLimitReached);
                };

                Ok(())
            }
            ChannelCommand::CheckPointCommand(command) => {
                unimplemented!()
            }
        }
    }

    fn get_last_sequence(&self, member: &MemberId) -> Result<ChannelSequence, ChannelError> {
        let member_sequence = self.members.iter().find(|m| m.member == *member);
        if let Some(member_sequence) = member_sequence {
            Ok(member_sequence.last_sequence)
        } else {
            Err(ChannelError::MemberNotFound(*member))
        }
    }

    fn use_sequence(&mut self, member: &MemberId, sequence: ChannelSequence) -> Result<(), ChannelError> {
        let member_sequence = self.members.iter_mut().find(|m| m.member == *member);
        let Some(member_sequence) = member_sequence else {
            return Err(ChannelError::MemberNotFound(*member));
        };
        member_sequence.last_sequence = sequence;

        let member_in_updates = self.updates.iter().any(|m| m == member);

        if !member_in_updates {
            let Ok(_) = self.updates.push(*member) else {
                return Err(ChannelError::NeedsCheckpoint);
            };
        }

        Ok(())
    }

    fn get_next_sequence(&mut self) -> ChannelSequence {
        let sequence = self.next_sequence;
        self.next_sequence = ChannelSequence(sequence.0 + 1);
        sequence
    }
}
