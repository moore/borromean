use super::*;

//= spec/implementation.md#functional-regression-requirements
//= type=test
//# `RING-IMPL-REGRESSION-121` VecLikeSlice MUST report empty state, length, capacity, and slice
//# contents from its logical items.
#[test]
fn requirement_slice_adapter_reports_len_capacity_and_empty_state() {
    let mut backing = [0u8; 3];
    let mut values = VecLikeSlice::new(&mut backing);

    assert!(values.is_empty());
    assert_eq!(values.len(), 0);
    assert_eq!(values.capacity(), 3);

    values.push(10).unwrap();
    values.push(20).unwrap();
    assert!(!values.is_empty());
    assert_eq!(values.len(), 2);
    assert_eq!(values.capacity(), 3);
    assert_eq!(values.as_slice(), &[10, 20]);
}

//= spec/implementation.md#functional-regression-requirements
//= type=test
//# `RING-IMPL-REGRESSION-122` VecLikeSlice clear MUST remove only logical items, restore empty
//# length, and allow reuse of underlying capacity.
#[test]
fn requirement_slice_adapter_clear_removes_only_logical_items() {
    let mut backing = [0u8; 2];
    let mut values = VecLikeSlice::new(&mut backing);

    values.push(1).unwrap();
    values.push(2).unwrap();
    assert_eq!(values.push(3), Err(3));

    values.clear();
    assert!(values.is_empty());
    assert_eq!(values.len(), 0);
    assert_eq!(values.as_slice(), &[]);

    values.push(4).unwrap();
    assert_eq!(values.as_slice(), &[4]);
}
