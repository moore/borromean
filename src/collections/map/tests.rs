use super::*;
extern crate std;
use proptest::prelude::*;
use std::vec::Vec;

fn vec_and_indexes() -> impl Strategy<Value = (Vec<u8>, usize, usize)> {
    prop::collection::vec(0..1u8, (ENTRY_REF_SIZE*2)..(10 *ENTRY_REF_SIZE)).prop_flat_map(|vec| {
        let len = vec.len();
        let first = 1..(len/ENTRY_REF_SIZE);
        let second = 1..(len/ENTRY_REF_SIZE);
        (Just(vec), first, second)
    })
}

proptest! {

    #[test]
    fn check_entry_ref((buffer, index1, index2) in vec_and_indexes(), start1 in 0..u32::MAX, end1 in 0..u32::MAX, start2 in 0..u32::MAX, end2 in 0..u32::MAX) {
        if index1 == index2 {
            return Ok(());
        }
        
        let index1 = IndexOffset(index1);
        let index2 = IndexOffset(index2);
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
