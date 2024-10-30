# Use "cargo chef" to improve build times after the first one.
FROM lukemathwalker/cargo-chef:latest-rust-1 AS chef
WORKDIR /app

FROM chef AS planner
COPY rust .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS rust_builder
WORKDIR /usr/src
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY rust .
RUN apt-get update && apt-get install -y libssl-dev pkg-config protobuf-compiler
RUN cargo build --release --package lance_sync_client

### Dotnet layer

FROM mcr.microsoft.com/dotnet/sdk:8.0 AS dotnet_builder
WORKDIR /usr/src
COPY cs .

RUN dotnet build -c Release -o demo hello_ffi/ApiTestbed/ApiTestbed.csproj

### Final layer
FROM mcr.microsoft.com/dotnet/runtime:8.0
WORKDIR /app
COPY --from=rust_builder /usr/src/target/release/liblance_sync_client.so .
COPY --from=dotnet_builder /usr/src/demo .
CMD ["/app/ApiTestbed"]