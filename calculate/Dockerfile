# 1. This tells docker to use the Rust official image
FROM rust:latest as builder

# 2. Copy the files in your machine to the Docker image
WORKDIR /app
COPY ./src ./src
COPY Cargo.toml .

# Build your program for release
RUN cargo build --release

# Run the binary
CMD ["./target/release/calculate"]


