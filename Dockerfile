FROM rust:1.82.0 AS rust_builder
WORKDIR /usr/src
COPY rust .

# Install the necessary dependencies
RUN apt-get update && apt-get install -y libssl-dev pkg-config protobuf-compiler

# We'll use --release later, let's keep debug info for now
RUN cargo build --package lance_sync_client

### Dotnet layer

FROM mcr.microsoft.com/dotnet/sdk:8.0 AS dotnet_builder
WORKDIR /usr/src
COPY cs .

RUN dotnet build -c Release -o demo hello_ffi/lance_sync_client/lance_sync_client.csproj

### Final layer
FROM mcr.microsoft.com/dotnet/runtime:8.0
WORKDIR /app
COPY --from=rust_builder /usr/src/target/debug/liblance_sync_client.so .
COPY --from=dotnet_builder /usr/src/demo .
CMD ["/app/lance_sync_client"]