use super::*;
extern crate std;
use crate::io::mem_io::MemIo;
use crate::io::{Io, IoError};
use proptest::prelude::*;
use std::{string::String, vec, vec::Vec};

fn vec_and_indexes() -> impl Strategy<Value = (Vec<u8>, usize, usize)> {
    prop::collection::vec(0..1u8, (ENTRY_REF_SIZE * 2)..(10 * ENTRY_REF_SIZE)).prop_flat_map(
        |vec| {
            let len = vec.len();
            let first = 1..(len / ENTRY_REF_SIZE);
            let second = 1..(len / ENTRY_REF_SIZE);
            (Just(vec), first, second)
        },
    )
}

proptest! {

    #[test]
    fn check_entry_ref((buffer, index1, index2) in vec_and_indexes(), start1 in 0..u32::MAX, end1 in 0..u32::MAX, start2 in 0..u32::MAX, end2 in 0..u32::MAX) {
        if index1 == index2 {
            return Ok(());
        }

        let index1 = RecordIndex(index1);
        let index2 = RecordIndex(index2);
        let start1 = RecordOffset(start1 as usize);
        let end1 = RecordOffset(end1 as usize);
        let start2 = RecordOffset(start2 as usize);
        let end2 = RecordOffset(end2 as usize);

        let mut buffer = buffer;

        EntryRef::write(&mut buffer, index1, start1, end1).unwrap();
        EntryRef::write(&mut buffer, index2, start2, end2).unwrap();
        let entry1 = EntryRef::read(&buffer, index1).unwrap();
        let entry2 = EntryRef::read(&buffer, index2).unwrap();

        assert_eq!(entry1.start, start1.0 as u32);
        assert_eq!(entry1.end, end1.0 as u32);

        assert_eq!(entry2.start, start2.0 as u32);
        assert_eq!(entry2.end, end2.0 as u32);

    }


}

#[test]
fn test_read_write() {
    const BUFFER_SIZE: usize = 2048;
    let mut buffer = vec![0u8; BUFFER_SIZE];
    let id = CollectionId(1);

    const DATA_SIZE: usize = BUFFER_SIZE;
    const MAX_HEADS: usize = 8;
    const REGION_COUNT: usize = 4;

    let mut mem_io =
        MemIo::<DATA_SIZE, MAX_HEADS, REGION_COUNT>::new().expect("Failed to create MemIo");

    let mut io: Io<'_, MemIo<2048, 8, 4>, MAX_HEADS> =
        Io::init(&mut mem_io, DATA_SIZE, REGION_COUNT).expect("Failed to initialize Io");

    let mut map = LsmMap::init::<MAX_HEADS>(&mut io, id, buffer.as_mut_slice())
        .expect("Could not construct LsmMap.");

    let key = 31337;
    let value = 42;
    map.insert(key, value).expect("insert failed");
    let key2 = 31415;
    let value2 = 17;
    map.insert(key2, value2).expect("insert 2 failed");

    let got = map
        .get(&key)
        .expect("could not get key")
        .expect("got None for key");

    assert_eq!(value, got);

    //let got = map.get(&key2).expect("could not get key").expect("got None for key");

    //assert_eq!(value2, got);
}
