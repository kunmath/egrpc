# resilient_subscribe — reconcile-on-reconnect with `egrpc::resilient_stream`

A subscription client that survives server restarts, network drops and
keepalive-detected dead links without missing or duplicating notifications.
It uses only public headers (`<egrpc/...>`, `<grpcpp/...>`) and the unmodified
generated `route_guide` stubs.

## The pattern

```cpp
egrpc::ChannelConfig cfg = egrpc::ChannelConfig::CellularProfile();
cfg.ca_bundle_path = "ca.pem";
auto channel = egrpc::CreateChannel("server.example:443", cfg);
auto stub = routeguide::RouteGuide::NewStub(channel);

int64_t cursor = 0;  // last sequence number processed

egrpc::ResilientStreamOptions opts;
opts.on_reconnected = [&] {
  // Runs on the Read() caller thread once a new stream is established.
  // Reconcile anything that changed while disconnected, e.g. with unary calls.
  ReconcileState(*stub);
};

egrpc::resilient_stream<routeguide::Feature> cmds(opts, [&](grpc::ClientContext& ctx) {
  routeguide::Rectangle req;
  req.mutable_lo()->set_longitude(cursor);  // resume after the cursor
  return stub->ListFeatures(&ctx, req);
});

routeguide::Feature f;
while (cmds.Read(&f)) {
  Handle(f);
  cursor = f.location().latitude();
}
// cmds.status(): terminal status (CANCELLED after cmds.stop()).
```

1. **Cursor in the factory.** The factory is called for every attempt, so it
   always builds the request from the latest cursor; the server resumes after
   it. A message the server sent but the client never read is simply re-sent.
2. **Reconcile in `on_reconnected`.** The callback runs on the caller thread
   (never on the channel's EventThread), so it may make blocking calls on the
   same channel. It must not call `Read()` or `stop()`. Set
   `fire_on_first_connect = true` to also run it on the first connection.
3. **Retry policy.** `DefaultShouldRetry` retries transient codes
   (`UNAVAILABLE`, `DEADLINE_EXCEEDED`, `INTERNAL`, ... and server-originated
   `CANCELLED`); terminal codes such as `PERMISSION_DENIED` end the stream.
   `stop()` (thread-safe) always ends it with `CANCELLED`.

## `ChannelConfig::CellularProfile()` server requirements

The profile sends keepalive PINGs every 60 s (20 s timeout, also without
active calls) to keep NAT mappings alive. The upstream gRPC server must allow
that, or it answers with `GOAWAY(ENHANCE_YOUR_CALM, "too_many_pings")` (egrpc
then doubles `keepalive_time` and logs a `WARN` on the `keepalive` component):

- `GRPC_ARG_HTTP2_MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS` <= 60000
- `GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS` = 1

## Build and run

```sh
cmake -S . -B build && cmake --build build -j
build/examples/resilient_subscribe/egrpc_resilient_subscribe \
    --host localhost --port 50051 --ca ca.pem --cellular --reconcile-unary \
    --stop-on-stdin
```

The binary targets the test server in `tests/server/route_guide_server.py`
(`ListFeatures` mode 7: sequenced notifications after a cursor) and prints
`EVENT`, `RECONNECTED` and summary `KEY=VALUE` lines; see the header comment
of `main.cc` for all flags. `tests/test_resilient_stream.py` drives it through
20 server restarts.
