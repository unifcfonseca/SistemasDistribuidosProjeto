FROM ubuntu:24.04

RUN apt-get update && apt-get install -y \
    g++ cmake pkg-config libzmq3-dev nlohmann-json3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY . .

RUN cmake -S . -B build && cmake --build build

CMD ["./build/cliente"]
