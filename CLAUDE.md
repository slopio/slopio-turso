# runtime
the runtime is thread/core, threads can communicate via io_uring.

# communication
- the client thread sends a message to the DB thread via io_uring and waits a response.
- the client doesn't touch the memory from the ptr that was sent via io_uring, only the cancellation flag can be modified, if it's present, we can ignore the result from the DB.
- the DB can only drop the message if the cancelled flag is true or it was sent as a response via io_uring
