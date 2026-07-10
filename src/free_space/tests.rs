use super::*;

fn position(region_index: u32, entry_index: u32) -> FreeQueuePosition {
    FreeQueuePosition {
        region_index,
        entry_index,
    }
}

//= spec/ring/05-disk-format.md#free-space-region-layout
//= type=test
//# `RING-FREE-005` The cursor invariant
//# `allocation_head <= ready_boundary <= append_tail` MUST hold in the
//# logical free-space collection order.
#[test]
fn requirement_free_space_replace_from_parts_validates_cursors_and_membership() {
    let mut state = FreeSpaceState::empty();

    assert_eq!(
        state.replace_from_parts(1, 2, 1, 2, &[10, 11]),
        Err(FreeSpaceError::InvalidCursor)
    );
    assert_eq!(
        state.replace_from_parts(1, 0, 3, 2, &[10, 11]),
        Err(FreeSpaceError::InvalidCursor)
    );
    assert_eq!(
        state.replace_from_parts(1, 0, 2, 3, &[10, 11]),
        Err(FreeSpaceError::InvalidCursor)
    );

    state
        .replace_from_parts(1, 1, 3, 4, &[10, 11, 12, 13])
        .unwrap();
    assert_eq!(state.metadata_region_index(), 1);
    assert_eq!(state.allocation_head(), 1);
    assert_eq!(state.ready_boundary(), 3);
    assert_eq!(state.append_tail(), 4);
    assert_eq!(state.entries(), &[10, 11, 12, 13]);
    assert!(!state.contains_free_region(10));
    assert!(state.contains_free_region(11));
    assert!(state.contains_free_region(12));
    assert!(state.contains_free_region(13));
    assert!(!state.contains_free_region(14));
}

//= spec/ring/05-disk-format.md#free-space-region-layout
//= type=test
//# `RING-FREE-011` Free-space cursor helpers MUST map logical queue
//# positions across every materialized metadata segment and reject
//# positions outside the retained metadata chain.
#[test]
fn requirement_free_space_positions_map_across_metadata_segments() {
    let mut state = FreeSpaceState::empty();
    state
        .replace_from_position_parts(
            &[100, 101, 102],
            2,
            position(100, 1),
            position(101, 1),
            position(102, 0),
            &[10, 11, 12, 13, 14],
        )
        .unwrap();

    assert_eq!(state.metadata_regions(), &[100, 101, 102]);
    assert_eq!(state.allocation_head(), 1);
    assert_eq!(state.ready_boundary(), 3);
    assert_eq!(state.append_tail(), 4);
    assert_eq!(state.allocation_head_position(), position(100, 1));
    assert_eq!(state.ready_boundary_position(), position(101, 1));
    assert_eq!(state.append_tail_position(), position(102, 0));
    assert_eq!(state.position(0), position(100, 0));
    assert_eq!(state.position(2), position(101, 0));
    assert_eq!(state.position(5), position(102, 1));

    assert_eq!(
        FreeSpaceState::index_for_position(&[100], 0, position(100, 0)),
        Err(FreeSpaceError::InvalidCursor)
    );
    assert_eq!(
        FreeSpaceState::index_for_position(&[100], 2, position(101, 0)),
        Err(FreeSpaceError::InvalidCursor)
    );
    assert_eq!(
        FreeSpaceState::index_for_position(&[100], 2, position(100, 3)),
        Err(FreeSpaceError::InvalidCursor)
    );
}

//= spec/ring/07-reclaim.md#erase-free-region-span
//= type=test
//# `RING-ERASE-FREE-001` The erased span MUST begin at the current
//# `ready_boundary` and contain exactly `count` dirty entries.
#[test]
fn requirement_free_space_erase_validates_count_and_boundary_position() {
    let mut state = FreeSpaceState::empty();
    state
        .replace_from_parts(1, 0, 2, 4, &[10, 11, 12, 13])
        .unwrap();

    assert_eq!(
        state.apply_erase(3, position(1, 5)),
        Err(FreeSpaceError::DirtyRangeTooSmall {
            available: 2,
            requested: 3,
        })
    );
    assert_eq!(
        state.apply_erase(1, position(1, 4)),
        Err(FreeSpaceError::InvalidPosition {
            expected: position(1, 3),
            actual: position(1, 4),
        })
    );

    state.apply_erase(1, position(1, 3)).unwrap();
    assert_eq!(state.ready_boundary(), 3);
    assert_eq!(state.ready_count(), 3);
    assert_eq!(state.dirty_count(), 1);
    assert_eq!(state.ready_boundary_position(), position(1, 3));
}
