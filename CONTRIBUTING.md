# contributing to solana-live-websocket

want to fix a bug or add a new stream filter? welcome aboard bhai, grab a cup of chai first.

## quick setup

- clone the repo and run `cargo test --workspace` to make sure everything is happy on your machine.
- test your changes offline first with `cargo run -p solana-live-websocket -- serve --port 8080 --simulator`.
- run `cargo check --workspace --all-targets --examples` before opening a pull request.

## ground rules

- keep `live-ws-core` focused on fast in-memory slot, fork, and stream types.
- keep `solana-live-websocket` focused on axum http routes, `/ws` fan-out, and cli commands.
- never block the async `tokio` runtime with slow file reads or `std::thread::sleep`.
- if you add a new wire field in `envelope.rs` or `snapshot.rs`, add a real websocket test in `tests/`.
- default server bind host must stay `127.0.0.1` so nobody exposes their local port by accident.
- keep comments short, honest, and easy to read at 3am when prod is acting funny.

## pull request checklist

- `cargo check --workspace --all-targets --examples` passes with zero errors.
- `cargo test --workspace` passes all unit and websocket tests.
- commit messages are short and tell us what changed in plain english.
