# Rust LanceDb Sync Client

This is a Rust FFI binding for the `lancedb` library. It is intended to be used by the C# side of the project,
but should be compatible with other languages that can use a C ABI (and don't provide direct
helpers like `PyO3`).

The client lazily creates an async environment with Tokio when a connection is first requested.
Subsequent requests are sent into the Tokio environment via a channel. The `event_loop` system
tries to dispatch these to Tokio tasks as fast as possible, avoiding blocking.

Client calls pass a completion handler (an `oneshot` channel) with events, and wait for it to
respond - returning control to the caller. This allows asynchronous operation internally,
while not attempting to expose async/await to the caller.

## Building

To build the project, you'll need to have Rust installed (preferably via `rustup`). From the top-level,
run:

### Debug Mode (Slower, but full strack traces if it goes wrong)

```bash
cd rust
cargo build --package lance_sync_client
```

### Release Mode (Faster, optimized code)

```bash
cd rust
cargo build --release --package lance_sync_client
```

### Using it with C#

In the `rust/target/debug` directory (or `rust/targe/release` if you compiled with optimizations), you'll find the `liblance_sync_client.so` shared library. You
need to put this somewhere the C# project can find: either in the same directory as your target,
or in a directory that's in your `LD_LIBRARY_PATH`.

### Status of the Synchronous API

* `Connection`: all methods implemented.
* `Table`: Implemented except for:
  * `UpdateSql` - there is no Rust equivalent in the API, more research/porting required.
  * `Search` other than the no-parameter version.
  * `Add` with dictionary inputs.
* `QueryBuilder` - implemented other than:
  * The vector variants.
  * The re-ranker, which appears to only be implemented in Python in LanceDB.