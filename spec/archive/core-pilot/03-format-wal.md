# V3 Format And Append-Only WAL

> Archived design pilot. This document is retained for historical reference and
> is not current design authority.

## Purpose and motivation

The v3 format gives recovery an unambiguous physical vocabulary. The WAL then
provides the ordered durable facts from which runtime state can be rebuilt.
They are specified together because a WAL record is only useful when startup
can find its chain, validate its target purposes, and know which encoded
version it is reading.

V3 is a clean format boundary. It rejects older media instead of carrying v2
branching and migration logic through every recovery transition. Migration, if
provided later, is an external tool that reads an old store and creates a new
v3 store.

## On-media objects

- **metadata** is the final format-publication object. It names format version,
  geometry, immutable bootstrap basis, and initial WAL;
- a **fixed region header** identifies system collection ID `0`, system type
  `WAL`, owning operation, and other fixed discovery fields for a WAL
  candidate. It carries no ordering sequence;
- a **WAL preamble** is the checksummed WAL-specific prefix following the region
  header. It records the WAL sequence, retained WAL head, allocation sequence,
  and allocation head as of the time that WAL region is published;
- a **valid WAL region** has both a valid fixed region header identifying
  collection ID `0` and system type `WAL`, and a valid WAL preamble;
- a **WAL segment** is a valid WAL region with an aligned append area;
- a **WAL record** is an append-framed logical fact with an operation identity,
  record kind, payload length, and individual checksum;
- a **link record** durably records the predecessor-to-successor relationship
  for a prepared WAL target; the target becomes a valid tail only when its WAL
  preamble is published; and
- the **retained boundary** identifies the oldest WAL segment recovery still
  needs, allowing older segments to be reclaimed in order.

System purposes are encoded separately from user collection type identifiers.
Consequently a damaged or stale collection type cannot be mistaken for the
main WAL, a transaction log, or a free-space basis. Format metadata also names
the storage's erased byte, append-record separator, and write granule. The
separator differs from the erased byte.

Collection ID `0` is the system namespace. The `WAL` value in a region header
is a system type, not a user collection type, and user collections cannot use
that ID/type combination.

There is no ordering sequence in the fixed region header. A collection data
region, transaction-log region, free-space-basis region, or other non-WAL
object is identified and ordered by the retained WAL facts and explicit links
that publish it. If one structure needs a sequence, its own preamble or record
stores that sequence with the state it orders. Giving all region headers one
physical publication counter would add no logical ordering information.

WAL preambles have their own monotonically increasing 64-bit sequence. The
initial WAL uses sequence `0`, and each published successor tail uses one more
than the current tail. The largest sequence among valid WAL preambles selects
the tail. Because the current tail is retained until its successor is
published, recovery never depends on preserving a sequence in an unrelated
region header.

Each WAL preamble also contains an allocator checkpoint: the allocation
sequence and allocation head captured from the same allocator state. This pair
defines which allocation facts have already been incorporated into the cursor.
It allows a later retained preamble to preserve the allocator effect of an
allocation fact when WAL reclaim excludes the region that contained that fact.
The checkpoint does not independently prove region ownership or replace an
open transaction's cleanup obligations.

### Format object requirements

1. `CORE-FMT-005` System-region purposes and user collection type identifiers
   MUST occupy distinct encoding namespaces.
2. `CORE-FMT-006` V3 metadata MUST encode the erased byte, append-record
   separator, and write granule, and MUST reject a separator equal to the erased
   byte.
3. `CORE-FMT-007` Collection ID `0` and system type `WAL` MUST identify WAL
   region headers and MUST NOT be available to user collections.
4. `CORE-FMT-008` A fixed region header MUST identify the region's purpose and
   validation fields but MUST NOT carry an ordering sequence; a sequence needed
   by one structure MUST reside in that structure's own preamble or record.
5. `CORE-FMT-009` Every valid WAL preamble MUST carry the WAL sequence number,
   retained WAL head, allocation sequence, and allocation head that were
   captured for that tail publication; the allocation fields MUST be one
   consistent allocator checkpoint.
6. `CORE-FMT-010` WAL sequence numbers MUST form one monotonically increasing,
   non-wrapping 64-bit sequence of valid WAL-tail publications; a successor to
   `u64::MAX` MUST report exhaustion before durable I/O rather than wrap.
7. `CORE-LOG-018` A valid WAL region MUST have both a valid region header with
   collection ID `0` and system type `WAL`, and a valid WAL preamble containing
   its WAL sequence number and retained head.

## Format construction

Formatting is destructive and publishes the store only at the end:

1. validate geometry and compute the bootstrap layout;
2. erase metadata and all data regions, then sync;
3. write the immutable bootstrap free-space basis sequentially and sync;
4. write the initial WAL region header, initial-sequence preamble containing the
   bootstrap allocation-sequence/allocation-head checkpoint, and
   bootstrap-basis selection record, then sync; and
5. encode metadata and sync it as the format publication event.

Until step 5 succeeds, open treats the medium as unformatted. Metadata is not
written first because it would point recovery at bootstrap structures that may
not yet exist after a crash.

### Format publication requirements

1. `CORE-FMT-001` V3 metadata MUST carry an unambiguous format version and the
   device geometry needed to reject an incompatible backing.
2. `CORE-FMT-002` V3 open MUST reject every earlier storage version; no v2
   migration path is part of the core.
3. `CORE-FMT-003` Formatting MUST erase the target, write and sync the immutable
   bootstrap basis, write and sync the initial WAL record that selects that
   basis, and write metadata last as the format publication event.
4. `CORE-FMT-004` A crash before metadata publication MUST be interpreted as
   unformatted media.

## WAL append and rotation

The active WAL tail has one append offset. A logical record is fully encoded in
scratch memory, including length and checksum, then written as one
contiguous aligned span and synced. Only after the sync does runtime advance
its operation identity and append offset.

The WAL is a durability and recovery layout, not the final collection query
layout. It accepts the density cost of append framing and receive-order storage
so a mutation can become durable before the complete contents of its future
data region are known. Replay reconstructs the bounded collection memory
frontier; later collection maintenance sorts, indexes, or packs that frontier
into an immutable region materialization.

### WAL append requirements

1. `CORE-LOG-001` WAL records MUST append at aligned, monotonically increasing
   offsets within a segment.
2. `CORE-LOG-002` Records in one durability phase MUST be encoded as one
   contiguous append batch.

### WAL snapshots

A collection WAL snapshot is the efficient checkpoint between receive-order
updates and a dedicated data-region materialization. The collection serializes
the complete logical state of its bounded memory frontier as one snapshot
payload. The snapshot record itself uses ordinary append framing and one
checksum, while entries inside its bounded payload use the collection's compact
snapshot format rather than separate WAL frames.

After the snapshot record is synced, it becomes the collection's WAL-resident
frontier basis and supersedes that collection's earlier frontier-update
records. It includes any immutable-basis identity needed to interpret the
frontier without those earlier records. Later mutations append normally after
the snapshot. Recovery decodes the newest retained frontier-basis snapshot and
applies only the later updates, so replay work is bounded without prematurely
committing an underfilled data region.

When no committed memory frontier is resident, a snapshot is a candidate
collection root rather than an unconditional winner. Replay compares its WAL
position with the collection's newest `head` record, which publishes an
immutable data-region materialization, and selects whichever record is later.

A snapshot may also make a resident frontier slot clean and evictable. If the
encoded snapshot does not fit the declared WAL record and reserve budget, the
collection writes a normal immutable materialization instead. A snapshot is not
a final collection head merely because it is self-contained: it remains an
interstitial representation in the shared WAL and is later replaced by another
snapshot or a data-region materialization.

### Snapshot requirements

1. `CORE-LOG-015` WAL records required to reconstruct a committed collection
   memory frontier MUST remain retained until a self-contained WAL snapshot or a
   completely written and synced data-region materialization is durably
   published as their replacement.
2. `CORE-LOG-016` A synced collection WAL snapshot MUST become that collection's
   WAL-resident frontier basis and MUST supersede only the earlier updates whose
   complete logical effect it encodes.
3. `CORE-LOG-017` Recovery of a WAL-snapshot-based collection MUST decode the
   newest retained basis snapshot and then apply later retained updates in order
   without requiring the superseded update prefix.
4. `CORE-LOG-023` For a collection without a resident committed frontier, WAL
   replay MUST select the later WAL position between the newest retained
   snapshot and newest retained collection `head` record.

### Allocation facts

An allocation fact contains the allocated region, its reserved purpose, a
globally ordered allocation sequence number, and the resulting
`allocation_head_after` free-queue cursor. Allocation facts in the main WAL and
allocation entries in transaction logs use the same sequence namespace. This
is distinct from the WAL sequence: the WAL sequence orders tail generations,
while the allocation sequence orders allocator pops that may be physically
distributed among several logs.

The allocator is global. One allocation holds its lock from reading the current
head and sequence through appending and syncing the allocation fact, applying
the new head and sequence in memory, and returning. Only then can another
allocation read the advanced state. Consequently, all durable allocation facts
after a basis form one complete order even when their containing logs do not.
Recovery validates and orders those facts by allocation sequence; the fact with
the greatest sequence supplies the recovered `allocation_head_after`.
Transaction commit determines collection visibility, but it does not determine
whether a transaction allocation consumed free space: a durable transaction
allocation fact remains allocator-visible even when the transaction never
commits.

### Append framing

An append stream has no enclosing final length because it remains writable.
Recovery must therefore distinguish an unwritten slot, the start of a record,
and an aligned position covered by a torn record without trusting later bytes
as an arbitrary new start.

Every physical record begins at a write-granule-aligned offset with the
configured one-byte record separator. The logical bytes after that separator,
including the individual checksum, are deterministically byte-stuffed so the
physical body contains neither the storage's erased byte nor an unescaped
record separator. The escape prefix is itself escaped. Padding uses a
non-reserved escape-code byte, and the complete physical extent is rounded up
to a multiple of the write granule.

The physical escape alphabet is derived canonically from metadata. In ascending
byte order, choose the first four values distinct from both the erased byte and
record separator as `escape`, `code_erased`, `code_separator`, and
`code_escape`. Encode a logical erased byte as `escape code_erased`, a logical
separator as `escape code_separator`, and a logical escape byte as
`escape code_escape`; emit every other logical byte unchanged. After decoding
the checksum, fill the remaining physical extent with `code_escape`. This gives
one deterministic encoding for every logical record without reserving values
from collection payloads.

At an aligned candidate position:

- the erased byte means the slot is unwritten;
- the record separator begins a candidate record that must fully decode and
  pass its checksum; and
- any other byte is part of a torn or corrupt physical span, so recovery may
  advance by one write granule when the append-recovery rules permit searching
  for the next candidate.

This framing applies to main WAL records and transaction allocation entries
that are independently appended and recovered. It does not apply separately to
each transaction-private data record or free intent when those entries are
written as one bounded materialization.

A bulk materialization has an enclosing header or seal that states its total
extent and, where required, count and aggregate integrity value. Its entries
are parsed only inside those bounds, the complete materialization is synced
before publication, and recovery never searches its interior for append-record
separators. Consequently each entry of a basis, collection-data region,
manifest, transaction-private data/free-intent materialization, or cleanup list
does not need an individual checksum, separator, escape encoding, or
granule-rounded extent.

### Append-framing requirements

1. `CORE-LOG-007` Every append-framed record MUST carry an individual checksum
   covering its complete logical bytes before physical byte stuffing.
2. `CORE-LOG-008` Every append-framed record MUST start at a write-granule
   boundary, and its complete physical extent including padding MUST be a
   multiple of that write granule.
3. `CORE-LOG-009` An append-framed record MUST begin with the configured record
   separator, which MUST differ from the storage erased byte.
4. `CORE-LOG-010` The escape byte and three escape codes MUST be derived
   canonically from the erased byte and record separator, and encoding MUST
   escape logical erased, separator, and escape bytes with their corresponding
   two-byte sequences.
5. `CORE-LOG-011` After the leading separator, deterministic physical encoding
   and padding MUST exclude both the erased byte and any unescaped record
   separator from the complete encoded body.
6. `CORE-LOG-012` Physical padding after the encoded checksum MUST use the
   canonical non-reserved padding code through the next write-granule boundary.
7. `CORE-LOG-013` Recovery of an append stream MUST interpret an erased byte at
   an aligned candidate position as unwritten space, a separator as a record
   candidate, and any other byte according to the specified torn-span scan rule
   rather than as an implicit record start.
8. `CORE-LOG-014` An entry inside an explicitly bounded materialization MAY omit
   an individual checksum, separator, byte stuffing, and granule-rounded extent
   when its enclosing format defines the bounds and integrity needed for safe
   use.

Rotation is split across maintenance and foreground publication. Maintenance
allocates the oldest prepared region, durably records its WAL reservation,
writes and syncs its fixed region header with collection ID `0`, system type
`WAL`, and its operation identity, but leaves its WAL preamble invalid or
unwritten. The header contains no WAL sequence. This produces a prepared spare
that cannot yet be selected as a valid WAL region. When the active tail needs
rotation, it appends and syncs one link to that spare. The next normal
publication phase captures one consistent allocation-sequence/allocation-head
pair under the global allocator lock, then writes the target's valid WAL
preamble containing the next WAL sequence, current retained head, and captured
allocator checkpoint, together with the first append in the new tail. It then
syncs. Runtime consumes the reservation token and selects the new tail only
after that sync.

If power fails after the old-tail link but before a valid target preamble
exists, the target is not a valid WAL region, so the greatest valid WAL-preamble
sequence still selects the old tail and recovery treats the trailing link as an
incomplete rotation. If the complete new preamble survives, its next WAL
sequence selects the target as the new tail; its predecessor link was already
durable, while its first appended record is recovered under the ordinary
absent, complete, or torn rules.

The current tail always reserves enough room for its successor link. An
ordinary record that would consume that space is rejected before I/O unless a
prepared spare can be linked within the operation's declared budget.

### Rotation requirements

1. `CORE-LOG-003` Rotation MUST reserve a purpose-tagged target, write and sync
   its WAL-purpose region header without a valid preamble, sync its predecessor
   link, then write and sync the valid next-sequence, head-bearing WAL preamble
   before making it the runtime tail.
2. `CORE-LOG-004` Recovery MUST never follow a link to an unprepared target.
3. `CORE-LOG-024` A prepared WAL spare MAY contain its final WAL region header,
   but MUST NOT contain a valid WAL preamble; only tail publication may write the
   head-bearing preamble that makes it a valid WAL region.
4. `CORE-LOG-025` Tail publication MUST capture allocation sequence and
   allocation head from the same global allocator state. A preamble MUST NOT
   claim an allocator state not justified by durable allocation facts or a
   selected allocator checkpoint, and its allocator checkpoint MUST NOT be
   treated as a replacement for ownership or cleanup evidence.

## Recovery and reclaim

The metadata-named initial WAL is the head and tail only for a newly formatted
generation. On later startup, the fixed-header pass identifies regions whose
valid header carries collection ID `0` and system type `WAL`. Startup validates
the WAL preamble of each candidate, then selects the valid WAL region whose
preamble has the largest WAL sequence number as the current tail. Two valid WAL
preambles with the same sequence number cause open to fail with a typed
corruption error, regardless of whether that number is the largest. The
selected tail preamble contains the most recently published retained WAL head
and allocator checkpoint. Recovery follows only the ordered, synced WAL chain
from that head through the selected tail and replays it as the database root.

A prepared region that was never published as part of that chain is not a
second tail. A link whose target header is absent, corrupt, or has the wrong
purpose is invalid; recovery never guesses a replacement target. Collection
heads, free-space basis installations, transaction decisions, cleanup cursors,
and other live roots become authoritative only when encountered during WAL
replay.

Within a segment, recovery validates append-framed records using their aligned
separator, stuffed body, decoded length, and checksum. The append-recovery
state determines whether an incomplete or torn candidate ends the usable tail
or whether scanning may resume at a later aligned separator. Structural
corruption in a retained durable range is reported as a typed error rather than
being skipped as unrelated bytes.

Reclaim does not erase an old WAL segment merely because a newer segment
exists. It first publishes a new tail generation whose preamble names the new
retained head and contains an allocator checkpoint at least as new as every
allocation fact in the excluded prefix. This proves that allocator-head replay
no longer needs those facts. Any ownership, transaction, or cleanup evidence
from the prefix must also have a retained replacement before exclusion.
Excluded segments then enter ordinary ordered dirty-free processing. A
published tail header or preamble is never rewritten in place to advance the
head.

For collection mutations, “no longer needed” means a later retained WAL
snapshot or durably published data-region materialization contains their
complete logical effect. Applying a WAL record to volatile memory is not
sufficient, because a crash still needs that record to reconstruct the
collection memory frontier.

### Recovery and reclaim requirements

1. `CORE-LOG-005` A retained WAL chain MUST have one selected start and one
   traversal direction.
2. `CORE-LOG-019` The valid WAL region whose preamble has the largest WAL
   sequence number MUST be selected as the tail; that preamble MUST supply the
   retained head, and startup MUST replay the ordered WAL from that head through
   the selected tail.
3. `CORE-LOG-020` WAL replay MUST be the only authority that selects live
   collection roots, allocator state, transaction decisions, and recovery
   obligations; the region-header scan MUST NOT independently establish those
   logical states.
4. `CORE-LOG-022` Startup MUST report corruption if any two valid WAL preambles
   carry the same WAL sequence number, and MUST fail open rather than ignore
   either candidate.
5. `CORE-LOG-021` Advancing the retained WAL head MUST publish a new WAL region
   with a larger WAL sequence number in a new preamble, and MUST NOT rewrite an
   already published WAL header or preamble in place.
6. `CORE-LOG-006` Reclaim MUST publish a new tail generation naming the new
   retained head and an allocator checkpoint whose allocation sequence covers
   every allocation fact in the excluded prefix before freeing any segment
   excluded by that head.
