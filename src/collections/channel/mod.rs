use core::marker::PhantomData;

use heapless::Vec;

use crate::io::{IoBackend, RegionAddress};
use crate::CollectionId;

#[cfg(test)]
mod tests;

#[derive(Debug)]
pub enum ChannelError {
    UserLimitReached,
    MemberNotFound(MemberId),
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
pub struct CommandAddress<B: IoBackend> {
    pub region: B::RegionAddress,
    pub offset: usize,
}

impl<B: IoBackend> CommandAddress<B> {
    pub fn zero() -> Self {
        Self {
            region: B::RegionAddress::zero(),
            offset: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberSequence {
    member: MemberId,
    last_sequence: ChannelSequence,
}

///////////// Protocol /////////////

pub enum ChannelCommand<B: IoBackend, const PAYLOAD_MAX: usize, const MEMBER_LIMIT: usize> {
    AddCommand(AddCommand<B, PAYLOAD_MAX>),
    AddMemberCommand(AddMemberCommand<B>),
    CheckPointCommand(CheckPointCommand<B, MEMBER_LIMIT>),
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
pub struct AddCommand<B: IoBackend, const PAYLOAD_MAX: usize> {
    /// A command with a sequence one less than
    /// the sequence of this command. It should
    /// be equal to the largest sequence that the
    /// sender has seen.
    prior: CommandAddress<B>,
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

impl<B: IoBackend, const PAYLOAD_MAX: usize> AddCommand<B, PAYLOAD_MAX> {
    pub fn new<const MEMBER_LIMIT: usize>(
        prior: CommandAddress<B>,
        sender_last: ChannelSequence,
        sequence: ChannelSequence,
        author: MemberId,
        message_id: MessageId,
        payload: Vec<u8, PAYLOAD_MAX>,
    ) -> ChannelCommand<B, PAYLOAD_MAX, MEMBER_LIMIT> {
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
pub struct AddMemberCommand<B: IoBackend> {
    member: MemberId,
    phantom: PhantomData<B>,
}

impl<B: IoBackend> AddMemberCommand<B> {
    pub fn new<const PAYLOAD_MAX: usize, const MEMBER_LIMIT: usize>(
        member: MemberId,
    ) -> ChannelCommand<B, PAYLOAD_MAX, MEMBER_LIMIT> {
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
pub struct CheckPointCommand<B: IoBackend, const USER_LIMIT: usize> {
    /// The checkpoint that this builds on
    previous_checkpoint: CommandAddress<B>,
    /// This is the total number of commands in the channel on this
    /// device up to this checkpoint.
    command_count: u64,
    /// This should include at least all the changes since
    /// the last checkpoint, but may include other changes
    /// to prevent the search for changes from getting too
    /// deep.
    sequences: Vec<MemberSequence, USER_LIMIT>,
}

impl<B: IoBackend, const MEMBER_LIMIT: usize> CheckPointCommand<B, MEMBER_LIMIT> {
    pub fn new<const PAYLOAD_MAX: usize>(
        previous_checkpoint: CommandAddress<B>,
        command_count: u64,
        sequences: &Vec<MemberSequence, MEMBER_LIMIT>,
    ) -> ChannelCommand<B, PAYLOAD_MAX, MEMBER_LIMIT> {
        ChannelCommand::CheckPointCommand(Self {
            previous_checkpoint,
            command_count,
            sequences: sequences.clone(),
        })
    }
}

///////////// Channel State /////////////

pub struct Channel<B: IoBackend, const PAYLOAD_MAX: usize, const MEMBER_LIMIT: usize> {
    id: CollectionId,
    next_sequence: ChannelSequence,
    members: Vec<MemberSequence, MEMBER_LIMIT>,
    checkpoint: CommandAddress<B>,
    updates: Vec<MemberSequence, MEMBER_LIMIT>,
    phantom: PhantomData<B>,
}

impl<B: IoBackend, const PAYLOAD_MAX: usize, const MEMBER_LIMIT: usize>
    Channel<B, PAYLOAD_MAX, MEMBER_LIMIT>
{
    pub fn new(id: CollectionId, initial_member: MemberId) -> Result<Self, ChannelError> {
        let mut members = Vec::new();
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
            members: members.clone(),
            checkpoint: CommandAddress::zero(),
            updates: Vec::new(),
            phantom: PhantomData,
        })
    }

    pub fn add_member(
        &mut self,
        member: MemberId,
    ) -> Result<ChannelCommand<B, PAYLOAD_MAX, MEMBER_LIMIT>, ChannelError> {
        let command = AddMemberCommand::new(member);
        self.apply_command(&command)?;

        Ok(command)
    }

    pub fn add_command(
        &mut self,
        prior: CommandAddress<B>,
        author: MemberId,
        message_id: MessageId,
        payload: Vec<u8, PAYLOAD_MAX>,
    ) -> Result<ChannelCommand<B, PAYLOAD_MAX, MEMBER_LIMIT>, ChannelError> {
        let sender_last = self.get_last_sequence(&author)?;
        let sequence = self.get_next_sequence();

        let command = AddCommand::new(prior, sender_last, sequence, author, message_id, payload);
        self.apply_command(&command)?;

        Ok(command)
    }

    pub(crate) fn apply_command(
        &mut self,
        command: &ChannelCommand<B, PAYLOAD_MAX, MEMBER_LIMIT>,
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
                unimplemented!()
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

    fn get_next_sequence(&mut self) -> ChannelSequence {
        let sequence = self.next_sequence;
        self.next_sequence = ChannelSequence(sequence.0 + 1);
        sequence
    }
}
