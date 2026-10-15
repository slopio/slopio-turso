# runtime
slopio: one thread per core, threads talk over io_uring (MSG_RING). A worker never blocks its thread: anything that waits yields.

# threads
- the DB thread runs `db::serve`: it owns the database and its single writer.
- every worker thread owns a read connection and a reader task: reads never leave the worker.

# reads
- a client's read goes into its worker's queue; the reader runs a batch of them in one read transaction and wakes each task.
- read connections are query-only.
- the reader passes the shared `ReadGate` to start a read transaction; the writer closes it to empty the WAL past `wal_limit`.

# writes
- `Connect` goes over MSG_RING itself, answered with the database and the shared `Inbox`.
- writes and transactions are pushed into the `Inbox` (a lock-free stack); MSG_RING only rings a doorbell, once per emptying.
- the writer batches them and commits at most every `commit_interval`; a write is answered once its batch committed.

# the request
- the worker keeps a clone of the request's `Arc` and touches nothing but the cancelled flag until the wakeup.
- dropped before its answer, the worker flags the request cancelled and ignores the answer.
- the DB thread skips a cancelled request, but always replies: the reply slot stays reserved until a wakeup lands in it.
- the DB thread writes the answer into the body and drops its clone before the wakeup: the worker then takes the body without a lock.

# rules
- no Mutex in shared structures: atomics, OnceLock, lock-free.
