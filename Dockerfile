FROM rust:1-stretch
RUN apt-get update && apt-get -y install protobuf-compiler
WORKDIR /hawkeye
ADD . .
RUN cargo build --release
