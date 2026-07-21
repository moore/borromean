# Main WAL Decisions

Abstract main-WAL identity, discovery, and retention decisions made while
finishing the system narrative.

These records preserve reviewed decisions moved from the active
[design queue](../todo.md). Later decisions may explicitly supersede an earlier
record.

## D21 — Main-WAL root and retained-boundary model

Agree the main WAL's role as database root, head/tail meanings, WAL sequence
namespace, tail selection, and the facts replay must retain. The follow-up patch
changes only the main-WAL introduction in the system narrative.

Decision: The main WAL is an ordered chain of logical regions and is the root
from which recovery reconstructs shared database state. The retained head is
the first region recovery must replay. Startup validates each main-WAL region
header and prologue and selects the valid prologue with the greatest WAL
sequence as the current tail. A torn or invalid prologue is not a candidate,
and duplicate greatest sequences cause open to fail. The runtime append
position is the first unused record position in the selected tail.

Each main-WAL prologue contains its WAL sequence and a checkpoint of the
retained head current when that region was initialized. A later durable
head-advance record in the selected tail may supersede that checkpoint. The
next WAL region receives the preceding tail prologue's WAL sequence plus one.
WAL sequences increase without repeat or wrap and are separate from the
free-list allocation sequence. Exhaustion is reported before media I/O.

The retained head may advance past a main-WAL region only after every fact
recovery still needs from that region has been superseded or restated at or
after the new head. This includes collection roots and later operations,
allocator state, open transaction-log references, transaction decisions,
unfinished cleanup, and materialization intents. The old prefix becomes
reclaimable only after publication of the new head is durable.

Rationale: A unique greatest valid WAL sequence gives startup one tail without
relying on a mutable database-header root. Separating the retained head, tail,
and append position avoids using one term for replay, region selection, and
runtime append state. Restating every required recovery fact makes head
advancement a safe replacement of the replay basis rather than deletion of
history that may still be needed.

Patch scope: Correct the startup summary and expand only the main-WAL
introduction in `000-system-narrative.md`. Do not create `004-main-wal.md` or
change vocabulary, exact prologue or record codecs, rotation ordering,
head-publication mechanics, tail framing, replay algorithms, implementation,
or models.

Verification: Confirm unique greatest-sequence tail selection, separate head,
tail, and append meanings, an independent non-repeating WAL sequence, and
retention of every listed recovery fact. Run Markdown and diff checks.
