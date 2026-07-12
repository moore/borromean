I think we want to decompose this a little differently. The shape is not all wrong but I think the ordering of ideas and which to stress are a little off. Here is a ruff sequancing of how I would structure it.

1. Borromean is a database that is intended to run without the help of an undelying filesystem. Fallowing from that one of it's core conserns is how to manage storage allocations, reclomation, and the geomitry of the storage.

The primary target for Borromean is flash storage connecte to mrico controlers. This added the complexity that the database must be responsible for ware leveling. To achieve this Boarroman combineds sevral key ideas. 

All updates are proformed as appends. If a record a must be update a delta or new copy of the record is writen that superceeds the curent value. If a record must be deleted a toomstone is writen that marks the record as deleted without touching the current persited value.

The second idea is that free regions are managed in a FIFO (First In First Out) queue where the oldes entry in the free queue is the first removed when neew space is needed. This promotes equal ware across all regions of flash. 

> NOTE: This is not a perfict system as data which is rarly updated can hold on to a allocation for an extended period of time while othere regons of storage are cyceld many times. We will need to model real world useage but I suspect that this will lead to a portion of the storage seeing serveal times the write cycleing then the rest based on different records having very differet life cycles. An aproach to hand this would be to perdiocally move very old allocations to more heavelly used regions of storage but we have chosen not to do that to maintain refererential intergrrty without adding a second level of inderaction.

A second consideration for working with flash is accomadatig the write-erace nature of flash. Once a flash cell has had a value written to it, the cell can not be updated to a new value without first being eraced. So updates to a region of flash must trasaction from written to erased to the new value. This differs from magnetic storage where values can be rewritten with driectly from A -> B. This erase cycle in flash can add siginifigent latency to overwrites of flash regions. This latency can cause unperdictability in write latency where writes to eraced regins are fast and writes to already written regions may see unexabtable delays. To mitigate this Borromean splits the free list. Allocation enter the free list as dirty and move to ready only after they have been eraced. The erasure and transition from dirty to ready is asynrosys writh updates to the records stored in the database so neither free opperations, allocation opperations, or writes to newly allocoated space see the delay of erasure.

Lastly to encorage wareleveling there is no conistent location to look for the root of the database. If we picked one or a few location to conistangly store the root those regions would need to be update reguarlly likely causing them to fail befor the rest of the storage. Instead Borromean devides the avalible storage in to a sequance of equal sized regions, each with a standerd header format. At startup all of these regions headders will be scanned to find the root of the database. To minimise this scan time larger regions are prefered thou as we will see later the size of the regions has a direct impact on memory useage so a traid must be made between start up effechency and required ram.

2. The storage gemometry of a Borromean data base is reasonablly simple. The geomerty is constrained by two four paramaters:

  1. The flash erase block size.
  2. The minimum write size (refered to in Borromean as the write granule).
  3. The totall liniear size allocated to Borromean.
  4. The Region size for the database.

The data base is layed out as:
```
[Database header][Region 0]...[Region n]
```
Each reagion has the format:
```
[Region Header][user data]
```
The database headder and each region must be erase block aligned. The region header must be write granual aligned. A region size which is not a multpule of the erash block is rejected on initilzation of a new database. The database headder is padded to erase block size, and the region headder padded to the region granual size.

Borromean assumes storage has a byte orented interface with four physical opperations:

 1. `write(address, data[u8]) -> Result((),   Error)` 
 2. `sync (address, length)   -> Result((),   Error)`
 3. `read (address, length)   -> Reuslt([u8], Error)`
 4. `erase(address, length)   -> Result((),   Error)`
> TODO: Update these to match the acutal Rust interface.

 The `write` call stages data in the underlying storage system. The staged data will be commit starting at `adderss` location in the storage and span `data[].len()` bytes. The `address` must be write granual aligned and `data[].len()` must be a mutpule of the write greanual. Once `write()` returns later `read()` calls must reflect the data provided for the write. Written data is not garenteed to be durable untill after a sesussiful `sync()` has returend.

 On a non error reulst form a `sync()` call. All previsoly written data in the covered rage starting at `adress` and contining for `length` bytes must be durable a will be retuend by subsuquent `read()` calles even if power is lost to the device between `sync()` and `read()`. The `address` must be write granul aligned, and `length` must be  multuple of the write granual.

 Date is recovered from the storge layer using the `read call()` has no restrictions on alignment but must return data based on the most reasont write assuming continuiouse powered opperation. If power is lost between a write and a read with not sync in between. Reading may return erither the data in write or the provisoly synced data from a previous write.

 Any read or write which would read outside of the storage region, or violate geomerty rules returs an error instead.

3. In Borromean records are grouped in to logical collections. Each collection may own zero or more regions. The core of Borromean is not responsible for the encoding of data past the region header but the region header spesifises the collection that owns the region and the encoding type used in the region. There are three collection types in the core Borromean desing wich are used to manage the allocations and life cycle of regions and records:

  1. The Write Ahead Log (WAL)
  2. Region Free list
  3. Transacton buffers

Othe higher level collection types are degind by consumers of Borromean core, each degfining there own recoreds, opperations, and region formats.

...Eaplain that there is a bounded number collections, and how there heads tracked by storage struct in memory...


4. In Borromean live records may live in three distinct state:

  1. WAL Resident
  2. Snapshot Resident
  3. Region Resident

This set of state is a stratgy to manage the writing of records in an append only namer while still allowing immeadeat durability and fast indexed reads.

The WAL (Write Ahead Log) is a short term durable location for a record to be stored. The WAL stores records in the order they were written. Records are never directly read from the WAL othen that at database startup when all opperations in the WAL are replayed in sequantial order to reconstruct the state of the database when it stopped. After replay any record that is not snapshot or region resident will be in ram. Reads of WAL resident records are always serviced from RAM.

Each open collection maintines a RAM buffer with a size equal to a region. Once a opperation is persted in the WAL is is applied by the collection implmentation to this RAM buffer to update the state of the collection. When the RAM buffer is full it is meterlized in to a region in flash allowing new updates to be held in the RAM buffer.

When a region is not imeadeatly needed but it's RAM buffer is not filled a compact version of the buffer accounting for only the currently consumed buffer space can be stored as a snapshot in the WAL. This allow the collection to be closed, and its RAM buffer returned, without writing a partally filled region whiles still allowing effechent reads from flash.

5. The Region Free List, it's a set of regions, it has a head, a ready pointer, a and a tail. It is a set of lined regions. The tail is managed as WAL records + snapshots until the fill a region at which point a the tail is metrilized to a preallocated region, allog with a link to a new preallocated region that will become the new tail. Only the region or regions immeaditly after the ready region pointer can be eraced and made ready, after wich the ready region pointer will point an the oldest of the newly earaced regions. New allocation are serviced by consuming the head of the list. When advancing the head moves to a new region the region that previsould head the head is freed. (and appended to the tail of the free list) Each allocation record cotains not just the reagion being allocated, but a free list head after field and a monotionc free list squance number. 

```
[Region 1                   ][Region 2                      ]
[stale records][readdy records][dirty records][unsused space]
               ^               ^              ^
            List Head    Ready pointer    List Tail
```

Becouse of transactions being written in parellell. The last allocation record observed in replay may not recover the head of the free list. Instead the head after field of the allocation record with the largets sequanse number is used to reconstruct the head of the free list. Becouse all Free actions happen in the main WAL the tail can be recovred via the last Free record enountered in replay.

> TODO: Work through recursive allocation for internal collections. Transaction-log continuation appears tractable if every segment reserves enough space to allocate, initialize, and link its successor before the transaction finishes. Free-queue growth cannot be nested in an existing caller transaction when rollback of that transaction may itself require the queue capacity being created. It therefore needs an independent reserved internal transaction/maintenance slot. Determine whether that is sufficient, or whether narrowly scoped WAL commands are required to reserve and publish new free-queue materialization or tail regions atomically. Specify the crash cuts, recovery behavior, bootstrap capacity, and minimum ready-region and log-space reserves for both cases.

6. Transaction are required as updates to user collection that require new regions be allocated can not happen in a single atomic step. A region must be allocated and removed from the global free list, and then subsequently made durrably reachable from some record in the curren open collection region. If a crash were to occur between these two steps a the allocated region might be leeked. A parrarell consern exists when a region is freeded from a collection to the free list. If the region is unlinked from the collection befor being added to the free list a crash between the steeps could cause a leak, but if it is freed before being unlinked it could lead to a region being eraced and asigned to a diff use while still having a live link from the collection. Transactions solve this by recording the all steps which must apply atomically and only after they are durable commiting to the state change. 

As an added feature they allow long running multy steps updates to a collection to not block reads of the database or block writes to other collections in the database. A case where this might be required is writing a large file that is being streamed in over a slow network connection.

...explain that there is a fixed number of transctions and that they are held by the storge struct and track the ephermeral state of the transactions each holding a reference to the transaction region the spisific transaction uses to hold segment...

...transaction regions can be freed only when no active wal records reference any segment in the region...

To ensure locality for opperations in a transaction they are stored not directly in the WAL but in a transaction segment which resides in a transaction region. On trasaction begin a begin transactino record is written to the main WAL with a link to the start of the transaction segment. Only one transaction can own a transaction region at any given time but there may be more then one trasaction segment in a single trasnaction owned region. A transaction segment in write granual aligned. 

It contains three sections:
```
[Allocations][Free intents][Optional next segment][collection opperations]
```

Allocations are written with WAL framing and record allocations made inside the transaction. Free intents are a packed list of regions to free at commit. If an addition sement is needed that next segmet contains a referens to it. Collection opperations a packed list of collection opperations.

Before commit opperations in the the transaction are buffered in memory accept for allocations which are imeadiatly appended to the transactino with WAL recored framing. This ensures that even if a crash hapens mid trasaction that no allocated yet uncommited regions will be leaked. 

If the trasaction region becomes full all buffered data is synced to the reagion allong with a link to a new region where trasactino can continume in a additional segment. 

Befor the commit record is writen to the WAL the WAL is locked by the commiting transaction. 

On commit all data is flushed to the transaction segment and once it is durrable a commit record pointing to the transactino segment head, free intent list start, next segment loation, and data start is writen to the wal. 

Immeadatally fallowing the commit record each region with a free intent record is freed, and then a trnsaction finish record is write to the WAL. Once the transactino finished record is written the WAL lock is released.

If on WAL replay one or more trasaction is found to be open and not commited it the WAL is locked and a rolback recored is written containing the same fileds as the commit record. After wich each record in the allocated list is freed. Once all the allocated records are freed the finish record is written and the WAL lock is release.

If Wal replay ends after a commit or role back but before the transction finish, replay finishes the transaction cleanup ans writes the finish record. 

7. The life cycle of a region cycles though the fallowing sates:

```
Ready Free -> Transaction Owned -> Collection Owned -> Transaction Owned -> Dirty Free -> Ready Free
```

The collection owned state my be skiped in the case of a transaction rollback.


Ready Free:
A Ready Free region is a member of the free queue that exits between the free queu head and the Ready pointer inclusive.

The trassition from Ready Free to Transaction owned can only be preformed on the head of the free queu and occurs when a allocation record is made durable in a transaction segmentand the global allocater head is advanced. If a crash occurs at this point a later Rollback and free opperation must move the Transaction Owned region ot the Dirty free state.

Transaction Owned:

A Region is Transactio Owned when it is referenced as allocated by a Alloction record in a transaction and the transaction has not been commited or rolled back.

On When a Commit record is durable in the wall for the transation that owns the region, the region become Collection owned. 

If instead the WAL contains a durrable Roleback Record thee must be a subsequnt free record befor the transaction finish record. The Free Record transsitions the region from Transaction Owned -> Dirty Free.

Collection Owned:

A Region is Collection Owned must be reachable from the collection root thou no part of Borromean core garentees this property. It must be enforced by the collection logic. 

A free intent record in a transaction stages the ownership trasation from Collection Owned to Transactino Owned. When the Transaction is durreably commited in the WAL the region becomes Transaction owned.

Subsuquent to the Commit that transfers ownership from Collection Owned -> Transaction Owned there must be a free record prior to the closing transaction finish record the moves ownership form Transaction Owned -> Dirty Free. 

If instead the Transtion is rolled back the region stayes Collection Owned.

Dirty Free:

A region is Dirty Free owned if it is a member of the free queue between the Ready Pointer (exclisive) and the free queue tail (inclusive)

A Dirty Free region can move to a Ready Free region if the region one step closer to the free queue head is at the Ready Pointer. The transition is a two step process of firs preforming a `erase()` on the region and then advancing the Ready Free pointer by writing a Erase record to the WAL. Once the erase record is durrable in the WAL the region transitions from Dirty Free -> Ready Free. Erase is preformed firs as it is an idempotent opperation, if a crash happens between erase and the WAL write profriming a dupleacate erase on the region is safe. As an optmisation any prefix of the Dirty Free list my be moved to the Ready Free state in a single WAL write under the condition that all regions in the prefix have been erased.

8. A discription of the WAL record framing. Byte stuffing, checksum, record start location after torn records, etc.

9. WAL replay detailed explanation.