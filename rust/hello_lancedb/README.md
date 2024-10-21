Note that you need to install `protobuf`:

```bash
sudo apt install -y protobuf-compiler libssl-dev
```

This example is very simple, it just creates a database, inserts some data and
runs a query. It's intended to validate that the Rust side is functional so we don't
build on a foundation of sand.
