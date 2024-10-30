Placeholder.

## Current Status

The initial repo has a few folders:

* `cs/` - C# code
* `rust/` - Rust code
* `vendor/` - contains Git submodule for `lancedb`.

Inside the `rust` folder, you'll find:

* `lance_sync_client` - FFI bindings for the `lancedb` library. See the [README](rust/README.md) for more information.

The `cs` side includes:

* `hello_ffi`, a parent project.
  * `ApiTestbed` - a simple program that calls the sync client and demonstrates usage.
  * `LanceDbClient` - a C# implementation of the sync client.
  * `LanceDbInterface` - the *desired* interface for the sync client. Closely resembles the Python API.
  * `LanceDbClientTests` - a suite of unit tests that exercise the client. You *must* have write access to `/tmp` for these, currently.

At the top-level you'll find a `Dockerfile`. This is a multi-stage build that will build the Rust code and then the C# code,
and invoke the `ApiTestbed` program. It's a demonstration of how to build and run the code in a container.

You can run the demo with:

```bash
docker buildx build --tag lance_test .
docker run lance_test
```
