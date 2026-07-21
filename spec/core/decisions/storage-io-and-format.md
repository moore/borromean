# Storage, I/O, and Format Decisions

Append-only behavior, discovery, logical I/O, and static format rules.

These records preserve reviewed decisions moved from the active
[design queue](../todo.md). Later decisions may explicitly supersede an earlier
record.

## D06 — Append-only scope

Agree that an update appends a new
representation instead of overwriting the previous one, while update and
deletion encodings remain collection-defined. The follow-up patch changes only
that introductory claim.

Decision: Borromean is an append-only store. An update appends a new
representation instead of overwriting the previous one. Each collection
defines how updates and deletions are represented.

Rationale: Append-only describes how an update changes stored state, not how
that update first becomes durable or published. Requiring a delta, replacement
copy, or tombstone would incorrectly make a collection-specific encoding
choice part of the core storage rule.

Patch scope: Replace only the append-only paragraph in section 1 of
`000-system-narrative.md`. Do not change WAL persistence, transaction
publication, retention, reclamation, or collection operation formats.

Verification: Confirm the paragraph prohibits overwriting an earlier
representation, leaves update and deletion encoding to the collection, and
does not describe a WAL or transaction path. Run Markdown and diff checks, and
leave D06 unchecked until this bounded patch has been reviewed.

## D07 — Discovery and logical-root wording

Agree how the fixed database
header, region-header scan, WAL head and tail, and recovered collection roots
relate. The follow-up patch changes only the introductory discovery
explanation.

Decision: Repeatedly updating a fixed database-root location would concentrate
wear, so the main-WAL tail moves through the region area as the database
changes. The fixed database header contains immutable facts, including the
database geometry and physical storage parameters. At startup, those facts
locate the region headers. Borromean scans all region headers to find the
current WAL tail, which points to the retained WAL head. The WAL range from
the retained head through the current tail is the root of the database.
Replaying that range recovers the current collection roots.

Rationale: The immutable header can remain at a fixed location without being
rewritten for each database change. Keeping physical discovery separate from
logical replay also makes clear that geometry helps locate the WAL but is not
itself part of the database root.

Patch scope: Replace only the introductory root-discovery paragraph in section
1 of `000-system-narrative.md`, while retaining the region-size tradeoff as a
separate paragraph. Do not define header fields, WAL validation, tail
selection rules, replay order, or recovery failure handling.

Verification: Confirm that startup scans all region headers, the database root
is only the retained WAL head-to-tail range, and replay recovers the current
collection roots. Confirm that the paragraph does not make geometry part of
the root or introduce WAL-validation mechanics. Run Markdown and diff checks,
and leave D07 unchecked until this bounded patch has been reviewed.

## D08 — Qualified wear claim

Agree the ordering FIFO provides among
free regions and why it does not guarantee equal wear across the whole
device. The follow-up patch changes only the wear-leveling claim and non-goal
note.

Decision: Free regions are kept in a FIFO queue, and allocation takes the
oldest ready entry. No region is returned to use before any free region that
entered the queue earlier. This does not cover regions holding long-lived
data because they are not available for reuse. Borromean does not move live
data solely to balance wear.

Rationale: FIFO guarantees reuse order among free regions, not equal wear
across every region of the device. Describing the boundary in terms of whether
a region is free covers pinned, reserved, bootstrap, and other long-lived uses
without requiring a separate list of special cases.

Patch scope: Replace only the introductory FIFO paragraph and its non-goal
note in section 1 of `000-system-narrative.md`. Do not change free-list
representation, dirty-to-ready maintenance, allocation eligibility, or add
live-data relocation.

Verification: Confirm the paragraph states the FIFO ordering guarantee,
excludes unavailable long-lived regions from that guarantee, and does not
claim equal wear across the whole device. Run Markdown and diff checks, and
leave D08 unchecked until this bounded patch has been reviewed.

## D09 — Logical read API and lifetime

Preserve unaligned logical reads
and define the callback shape, borrowing lifetime, zero-length reads, and
large-range bound. The follow-up patch changes only the `read` contract.

Decision: `read` takes a callback and invokes it once with one contiguous
borrowed slice containing exactly the requested bytes. The slice is valid
only during the callback; the callback may copy or interpret it but cannot
retain it. Reads need not be aligned, must remain within either the fixed
database-header span or one region, and cannot exceed the region size. A
zero-length read at an in-range address performs no device transfer and calls
the callback once with an empty slice. Larger logical values are processed
through multiple reads.

Rationale: A callback lets mapped storage lend its bytes directly while an
embedded backend may use fixed scratch memory. Limiting one read to one
region bounds that scratch memory by the configured region size. Keeping the
borrow inside the callback makes its lifetime explicit and prevents a backend
buffer or mapped view from escaping its valid access period.

Patch scope: Change only the logical `read` signature and its explanatory
paragraph in section 2 of `000-system-narrative.md`. Preserve the existing
continuous-power and post-restart visibility rules. Do not change write,
sync, erase, physical transfer alignment, error types, or exact backend Rust
traits.

Verification: Confirm that successful reads provide exactly one contiguous
slice for the requested range, the slice cannot escape the callback,
zero-length reads require no device transfer, and no read requires more than
one region of scratch memory. Confirm that unaligned reads and the existing
power-loss interpretation remain unchanged. Run Markdown and diff checks, and
leave D09 unchecked until this bounded patch has been reviewed.

## D10 — Logical write and failed-write semantics

Agree the erased-range
precondition, alignment, continuous-power visibility, tear boundary, and
allowed physical effects when `write` returns an error. The follow-up patch
changes only the `write` contract.

Decision: A write address is write-granule aligned, its length is a multiple
of the write granule, and every granule in its range has not been programmed
since its last erase. Alignment, bounds, and erased-range rejection occur
before any device program operation and leave storage unchanged. Once
programming begins, an error may leave the range unchanged, complete, or
torn. A torn write has zero or more complete leading granules, at most one
partly programmed granule, and an erased remainder. Bytes outside the range
remain unchanged. Success makes the complete data visible under continuous
power; a covering sync is still the minimum durability guarantee, though a
backend may make the write durable earlier.

Rationale: Requiring an unused erased range keeps the logical I/O contract
consistent with append-only storage and does not depend on media-specific
in-place bit clearing. Treating the write granule as alignment rather than
atomicity makes the ambiguous effects of a failed device write explicit. A
caller therefore cannot assume that the same range remains erased and safe
to retry after programming has begun.

Patch scope: Replace only the `write` explanation in section 2 of
`000-system-narrative.md`. Do not change the operation signature, read, sync,
erase, exact backend error types, post-error fail-stop policy, or backend
implementations.

Verification: Confirm that precondition rejection performs no program
operation, success is fully read-visible under continuous power, a failed
program can be absent, complete, or torn inside one granule, and no write
changes bytes outside its requested range. Confirm that sync remains the
minimum durability guarantee and earlier durability remains allowed. Run
Markdown and diff checks, and leave D10 unchecked until this bounded patch
has been reviewed.

## D11 — Range-sync and failed-sync semantics

Preserve range sync as the
API and define zero-length behavior, composable partial ranges, widened
durability, and the effects allowed when `sync` returns an error. The
follow-up patch changes only the `sync` contract.

Decision: Sync addresses and lengths are write-granule aligned and in range.
A zero-length sync succeeds without a backend barrier and adds no durability
guarantee. A successful nonempty sync makes each previously written granule
in its requested range durable. Successful sync ranges compose, so several
calls may together make a complete write durable. The requested range is a
minimum guarantee: an implementation may widen it or use a global barrier,
and callers cannot depend on other writes remaining non-durable. Sync changes
durability but not bytes visible under continuous power. Validation rejection
invokes no barrier. After a barrier is attempted, an error may leave none,
some, or all earlier write effects durable, including effects outside the
requested range. Previously durable data remains durable.

Rationale: Defining the guarantee per aligned granule matches a range API and
lets successful partial ranges compose without requiring one call to cover a
whole write. A backend that supports only a global barrier can still satisfy
the contract by widening the operation. A failed barrier cannot roll back
durability or reliably report how much work completed.

Patch scope: Replace only the `sync` explanation in section 2 of
`000-system-narrative.md`. Do not change the operation signature, write, read,
erase, exact backend error types, post-error fail-stop policy, or backend
implementations.

Verification: Confirm that zero length invokes no barrier, successful ranges
compose at write-granule boundaries, a wider or global barrier is allowed,
sync never changes continuously visible bytes, and a barrier error provides
no new durability guarantee while preserving all prior guarantees. Run
Markdown and diff checks, and leave D11 unchecked until this bounded patch
has been reviewed.

## D12 — Format metadata, version, and geometry

Agree immutable metadata
fields, erased byte, format-version compatibility, configured storage range,
geometry validation, and sequence-exhaustion policy. The follow-up patch adds
only the static format contract; bootstrap ordering is D38.

Decision: The fixed database header contains only immutable interpretation
facts: format marker, explicitly supported format version, encoded metadata
length, integrity check, database-header span, erase-block size, region size
and count, logical write granule, and erased byte. It also contains format-time
capacity limits that recovery must know, such as the number of transaction-log
slots, and values needed to recognize encoded data, such as the WAL record
marker. It contains no WAL head or tail, allocator cursor, collection root, or
other mutable state. Runtime tuning settings are excluded because they do not
change the meaning or layout of stored data. The configured database length
is the header span plus region count times region size. The logical range
presented to Borromean matches that length exactly; a larger physical device
may expose the database as a bounded subrange.

Open validates the format marker and integrity before trusting metadata,
selects only an explicitly implemented version decoder, and never guesses
compatibility or silently upgrades. It rejects invalid or overflowing
geometry, range mismatch, erase-alignment violations, an incompatible logical
write granule, erased-byte mismatch, or fixed capacities that do not fit the
region count. Rejection does not modify storage.

Each newly initialized region receives a monotonically increasing sequence
number in its region header. Region sequence numbers never wrap or repeat. An
operation that would need a value beyond the encoding's maximum returns the
permanent `SequenceExhausted` error before media I/O. Existing data remains
readable; additional sequence space requires explicit migration or
reformatting.

Rationale: The fixed header supplies the physical search geometry and stable
interpretation parameters needed before WAL discovery. Keeping mutable roots
out of it avoids repeatedly rewriting a fixed flash location. Exact version
dispatch prevents accidental misinterpretation. Rejecting region-sequence
exhaustion preserves region ordering without reuse or wraparound.

Patch scope: Add only the static database-header, validation, compatibility,
record-recognition, and region-sequence-exhaustion contracts to section 2 of
`000-system-narrative.md`. Do not define the exact byte codec, format
publication order, interrupted-format outcomes, migration procedure, mutable
WAL discovery state, or backend implementation changes. D33 owns exact WAL
framing, D36 owns reserve mechanics, and D38 owns bootstrap publication.

Verification: Confirm every listed header fact is immutable, no mutable root
appears in the header, the configured range has one unambiguous formula,
unsupported versions and geometry mismatches are rejected without
writes, and no region sequence can wrap or repeat. Run Markdown and diff
checks, and leave D12 unchecked until this bounded patch has been reviewed.
