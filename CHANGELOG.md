## Unreleased

- Fix: an event packet with no data (`2` or `20`) threw in the server. In Dox it logged one error per packet, and a plain `SocketIoServer().listen()` did not catch it. Now only CONNECT and DISCONNECT may have no data. Any other packet with no data is a parse error and closes that client. A JSON `null` counts as data, so `40null` and `1null` are refused too, as in Node.js.
- Security: a CONNECT `auth` nested about 200,000 levels deep parsed fine, then overflowed the stack when the server encoded it again (the `maxAuthSize` check, or `fetchSockets` with the Redis adapter). Now text packets are also limited to 32 levels by default, as binary packets were already. Deeper data closes the client. New `ServerOptions.maxDepth` changes the limit for both. It must be 1 or more, else `SocketIoServer` throws an `ArgumentError`. Node.js has no limit, so an app that sends deeper JSON should raise it. The `maxAuthSize` check also counts auth it can not encode as too big.
- Security: `Broadcast.socketId(req)` gave the `X-Socket-ID` header as it was. That value is the socket to skip, so a room name there (`broadcast:private-user.42`) made `toOthers` skip the whole room. Now it gives null when the value is not a socket id. `Broadcast.event` also drops a bad `toOthers` value, so `BroadcastFake` and the queued payload see null. A broadcast job drops a bad value too.

## 3.2.1

- Fix: `socket.join()` after the socket disconnected kept the socket id in the room store for ever. It now does nothing after the socket closed. A join in a middleware still works. Rooms joined by a socket that never connects (a middleware fails, or the client leaves first) are removed.
- Security: a client could send a binary packet header, then stop before the last part. The parts stayed in memory for as long as the client answered pings, even before it joined a namespace. Now a binary packet before the client joined a namespace closes the client, and a packet that is still not complete at two pings in a row is dropped and closes the client.
- Security: new opt-in `ServerOptions.maxAttachmentBytes` caps all binary parts of one packet. Null (the default) keeps today's behavior.
- Security: the CONNECT `auth` payload (up to `maxHttpBufferSize`) stays in memory for the life of the socket. New opt-in `ServerOptions.maxAuthSize` refuses a bigger one with `connect_error` (`auth payload too large`). Null (the default) means no limit.
- Security: with `maxSessions` null (the default), every GET request makes a session that lives about 45 seconds. The default stays. The server now logs one warning at start, and the docs say to set it in production.
- Security: a 1 MB polling body of separator characters was split into about 500,000 strings before the first one was read. Now packets are read one at a time, and reading stops when the session closes.
- Security: a polling client that stopped polling kept every packet sent to it until the ping timeout. New opt-in `ServerOptions.maxWriteBufferSize` closes the session above that size. Null (the default) means no limit.
- Fix: `socket.emitWithAck` with no timeout waited for ever when the client never answered. New opt-in `ServerOptions.ackTimeout` sets a default timeout. Null (the default) keeps today's behavior.
- Fix: an `emitWithAck` future that the app did not wait for failed on disconnect or timeout as an unhandled error, which can stop the isolate. It is no longer reported as unhandled. `await` still throws.
- Fix: a broadcast `emitWithAck` kept one waiting ack per socket when the server count failed (Redis down), until the socket left. They are now dropped.
- Fix: a broadcast with an ack from another server with no timeout kept its waiting acks for ever. They are now dropped after `requestsTimeout`. That sender (Node.js) has already given up by then.
- New: `socket.waitingAcks`, the number of acks a socket still waits for.
- Speed: a room broadcast with `except` (like `socket.to(room).emit(...)`) copied the room members 3 times on each emit. Now it makes no copy for one room, and one copy for many rooms.
- Speed: with the Redis or isolate adapter, every server decoded every broadcast, also for rooms it does not have. Now a one-room broadcast is not decoded when this server has no socket in that room. A broadcast to the whole namespace is not decoded when this server has no socket in it.
- Speed: a broadcast with the Redis or isolate adapter sent to the local sockets only after the publish to Redis was done. Now the local sockets get it at once. The emit future still completes after the publish, a failed publish is still logged, and messages keep their order.
- Speed: a presence join or leave asked every other server for the whole socket of each member (all headers, cookies, auth). Now Dox servers send only the socket id and the presence data. A Node.js server still sends the whole socket, which works too. New: `PubSubAdapter.fetchSocketsData(nsp, options, dataKey)`.
- Fix: with the Redis or isolate adapter, a namespace like `/a#b` did not get broadcasts from other servers when a namespace `/a` also existed. They went to `/a` and were dropped. Now each matching namespace checks the packet, and only the right one handles it.
- Memory: a socket makes its ack maps and its `data` map only when they are first used.
- Speed: fewer string copies when a packet is decoded, and one socket id needs 4 random calls instead of 15.

- Change: every log and warning line now starts with `[socketio]` (was `dox_socketio:`). Only the prefix changed.

## 3.2.0

- Fix: the warning "Socket.IO does not check the Origin header" also printed in `bin/queue.dart` and `bin/schedule.dart`. Those processes never accept sockets. In a Dox app it now prints only in the web server (`Dox().isWebServer`, needs the dox_core release that has it), at start. A plain server prints it in `attach()` and `listen()`. It still prints once per isolate.
- New: `dox add socketio` (the `socketio:install` command). It adds the config, the provider, the `socketioEnabled` getter and `SOCKETIO_ENABLED`. `--broadcasting` turns on broadcasting. It never overwrites a file. New dependency: `dox_annotation`.
- Fix: a presence rule that returned no `id` made `user_id` the socket id when the socket was logged in. The same user then showed as a new member on each connection. `user_id` is now the user id, as in the HTTP auth route. The socket id is used only when the user has no id.
- Ships an AI agent skill (dox-socketio-guide). Install it in your app with `dart run skills@ get`.
- New: broadcasting, like Laravel. It is off by default. Turn it on with `SocketIoConfig(broadcasting: true)`, or call `Broadcast.listen(io.sockets)` on a plain server.
  - `Broadcast.channel('orders.{id}', rule)` adds a rule for private and presence channels. The rule gets the user and the `{name}` values.
  - `Channel`, `PrivateChannel` and `PresenceChannel` make the channel names (`news`, `private-...`, `presence-...`).
  - Clients emit `subscribe` with `{channel, auth?}` and `unsubscribe` with `{channel}`. Both answer an ack: `{ok: true}` or `{ok: false, error}`.
  - A private or presence channel needs a logged-in socket (`SocketAuth`) and a passing rule, or a token from the HTTP auth endpoint.
  - `Broadcast.routes(path:, middleware:)` adds `POST /broadcasting/auth`. It runs the rule with `req.auth` and answers `{auth: token}`, or 403. The token is signed with HMAC-SHA256 and a key made from `APP_KEY`, and is valid for one socket, one channel and `Broadcast.tokenLifetime` (1 minute).
  - Presence channels send `presence:subscribed`, `presence:joining` and `presence:leaving`, like Laravel Echo. A user with many sockets is one member. It works across isolates and servers with the adapters. When a server does not answer, a presence `subscribe` is refused and a leave still sends `presence:leaving`.
  - `ShouldBroadcast` (`broadcastOn`, `broadcastAs`, `broadcastWith`), `Broadcast.event(event, toOthers:)` and `broadcast(event)` send events to the channels. `Broadcast.socketId(req)` reads `X-Socket-ID`. With no Socket.IO server (a queue worker), set `Broadcast.emitter` to a `SocketIoEmitter`.
  - `Broadcast.fake()` keeps the events for tests, with `assertBroadcasted`, `assertBroadcastedOn`, `assertBroadcastedTimes`, `assertNotBroadcasted` and `assertNothingBroadcasted`.
  - Limits: `Broadcast.maxChannelLength` (200), `Broadcast.maxChannelsPerSocket` (100) and `Broadcast.presenceTimeout` (2 seconds, how long a presence join or leave waits for the other servers). An `unsubscribe` cancels a `subscribe` of the same channel that still runs. The auth endpoint answers 403 for a `socket_id` that is not a Socket.IO id.
  - `Broadcast.reset()` forgets the rules, the send target and the emitter, for tests.
  - The event names follow Laravel Echo. The auth does not: the client sends the token from the auth endpoint in its own `subscribe`.
- New: queued broadcasting, with no dependency on `dox_queue`. It is opt-in.
  - Mark an event with `ShouldBroadcastQueued` (in place of `ShouldBroadcast`). Set a hook with `Broadcast.useQueue(push)`. Then `Broadcast.event` calls `push(payload)` and sends nothing. Your queue job calls `Broadcast.deliver(BroadcastPayload.fromJson(json))` to send it.
  - `BroadcastPayload` is JSON data: `channels`, `event`, `data` and `except` (the `toOthers` socket id). `toJson()` and `fromJson()`.
  - With no hook, a `ShouldBroadcastQueued` event is sent at once. Normal `ShouldBroadcast` events never use the hook.
  - `deliver` in a worker process needs the Redis adapter or `Broadcast.emitter`. It throws a `StateError` with no way to send, and warns once when it sends through an in-memory adapter with no clients.
  - `BroadcastFake` keeps queued events (`BroadcastedEvent.queued`, `assertQueued<T>()`) and the payloads of `deliver` (`delivered`).
  - `Broadcast.reset()` also removes the hook.
- New: whisper, client-to-client events on private and presence channels. It is off by default. Turn it on with `SocketIoConfig(broadcasting: true, whisper: true)` (or `Broadcast.whisper = true` on a plain server).
  - A client emits `client event` with `{channel, event, data}`. The event name must start with `client-`. The other sockets in the channel get `<event>` with `(channel, data)`, on all servers. The sender does not get it.
  - Only a socket that subscribed to the channel can whisper. Public channels are refused. Data bigger than `Broadcast.maxWhisperSize` (10 KB of JSON) is dropped. An ack, when the client asks for one, is `{ok: true}` or `{ok: false, error}`.
  - With whisper off, a `client event` is ignored, like before.
- `crypto` is now a direct dependency (it was already used through dox_core).
- New: `ServerOptions(allowRequest:)`, a hook like `allowRequest` of the Node `engine.io` server. It runs for every handshake and websocket upgrade and gives true or false (sync or async). With false, the client gets 403 with the Engine.IO error code 4 (`Forbidden`). A hook that throws refuses the request too. Null (the default) allows all, like before.
- New: `ServerOptions(maxSessions:)`, the most open Engine.IO sessions per isolate. At the limit, a new handshake gets 503 (`Too many sessions`). Open sessions keep working. Null (the default) means no limit, like before.
- Fix: a closed `IsolateAdapter` kept its bus listen until the isolate stopped. An app that opened and closed many servers in one isolate piled up listens, and each old one still got and answered messages. Now a closed adapter stops listening when another adapter of the isolate listens. Only the last closed one keeps answering, so the other isolates do not wait for the timeout.
- Fix: a closed `IsolateAdapter` still answers the other isolates until its isolate leaves the bus. Before, during a shutdown, `fetchSockets` and `serverSideEmitWithAck` in another isolate could wait 5 seconds for it and then throw.
- `RedisFormat` and `RedisRequestType` are now in `pub_sub_adapter.dart`, so it no longer imports the Redis and isolate adapters. The package exports them as before.

## 3.1.1

- New: `IsolateAdapter` shares broadcasts, `socketsJoin`, `fetchSockets`, `serverSideEmit` and broadcast acks between the isolates of one Dox app, with no Redis. It uses `IsolateBus` of dox_core. Needs dox_core 3.1.3.
- With many isolates and no `adapter`, `SocketIoProvider` now uses the `IsolateAdapter`. Before, it used the in-memory adapter, and a broadcast only reached the clients of the same isolate.
- New: `PubSubAdapter`, the shared logic of the Redis and isolate adapters. Extend it to share over something else.
- The adapter log lines are now `dox_socketio: publish failed` and `dox_socketio: bad adapter message` (were `redis publish failed` and `bad redis message`).

## 3.1.0

- Fix: `handshake.token` reads the `Authorization` header with `bearer` in any case (`Bearer`, `bearer`, `BEARER`), like dox_auth. Before, only `Bearer` worked.
- New: `SocketAuth`, a middleware that logs a socket in at connect time. Socket.IO runs before the Dox router, so session and auth middleware do not run for it. Your function gets the handshake and gives the user, or null. The user is kept in the new `socket.user`. With null, the client gets a `connect_error` (`Unauthorized`).
- New: `handshake.token` gives `auth.token`, else the `Authorization: Bearer` header.
- New: `handshake.cookie(name)` reads a cookie of the handshake, decrypted with the APP_KEY like `req.cookie(name)`.
- New: `await handshake.session()` loads the Dox session of the client from its session cookie, with `SessionMiddleware().fromCookie` of dox_core. The checks are the same as for a request: a changed, foreign or expired cookie gives an empty session. With dox_auth, `Auth(guard: 'web').userFromSession(session)` gives the user of the web login, and `Auth(guard: 'api').userFromToken(handshake.token)` checks a JWT. Needs the next dox_core. With no `ServerOptions.allowedOrigins`, or with `'*'` in it, it prints a warning once, because any website could then open a socket with the session cookie.
- `handshake.cookie(name)` now decrypts with `DoxCookie.decrypt` of dox_core, the same code as `req.cookie(name)`.
- New: `ServerOptions(allowedOrigins:)` turns on an origin check, with the same rule as dox_core. `[]` allows the same host only, a list also allows those sites, `['*']` allows all. Then a websocket or polling request from another origin gets 403. The default (`null`) keeps the 3.0.0 rule.
- With no `allowedOrigins`, the server prints one warning at start that any site can connect.
- Origin matching for `cors` now ignores case and a trailing `/`.

## 3.0.0

- The first stable release.

## 3.0.0-rc.1

- Uses the `Logger` that `dox_core` now exports, so it needs dox_core 3.0.0-rc.1.

## 3.0.0-alpha.15

- Works with `ioredis` 3, so it installs next to `dox_core` 3.0.0-alpha.17. Before, `dart pub add dox_socketio` failed in a new app.

## 3.0.0-alpha.14

- `SocketIoProvider` (was a DoxService): extend it in your app, `shutdown()` is optional.

## 3.0.0-alpha.13

- The Redis adapter now sends requests to the other servers, like `@socket.io/redis-adapter`. Before, it only answered them. Dox and Node.js servers on one Redis work together, in both directions. Both msgpack and JSON work.
- Breaking: `fetchSockets()` returns `Future<List<RemoteSocket>>`. It has the sockets of all servers. Sockets of this server are `Socket`s. Before, it returned `List<Socket>` of this server only.
- Breaking: `socketsJoin`, `socketsLeave` and `disconnectSockets` return `Future<void>` and act on all servers. Before, they returned `void` and acted on this server only.
- New: `RemoteSocket` with `id`, `handshake`, `rooms`, `data`, `emit`, `emitArgs`, `join`, `leave` and `disconnect`. `Socket` implements it.
- New: `serverSideEmit(event, args)` and `serverSideEmitWithAck(event, args)` on the server and on namespaces. Listen with `onServerSideEmit(event, handler)`. The handler gets a `ServerSideEvent`; answer with `event.ack(value)`. Clients do not get these events.
- New: broadcasts with an ack: `io.timeout(Duration(seconds: 5)).emitWithAck('event', data)` and `emitArgsWithAck`. Acks come from clients on all servers. A timeout is needed.
- Fix: our clients now ack broadcasts from other servers (Node.js or Dox), with the right client count. Before, the server always answered "0 clients".
- New: `RedisAdapter(requestsTimeout: ...)`, 5 seconds by default, like Node.js. `fetchSockets` throws a `TimeoutException` and `serverSideEmitWithAck` throws an `AckTimeoutException` (with the answers so far) when a server does not answer in time.
- New: `RedisAdapter(publishOnSpecificResponseChannel: true)`, same as the Node.js option. The adapter now also listens on the response channels.
- New: `AckTimeoutException`, with the `responses` that came before the timeout.
- New: `Namespace.socketsJoin`, `socketsLeave` and `timeout`. `SocketIoServer.fetchSockets`, `socketsJoin`, `socketsLeave`, `disconnectSockets` and `timeout`.
- New: `Handshake.fromJson`, for the handshake of a socket on another server.
- New: `Adapter` has new methods: `broadcastWithAck`, `serverCount`, `fetchSockets`, `addSockets`, `delSockets`, `disconnectSockets`, `serverSideEmit` and `serverSideEmitWithAck`. A custom adapter must add them. `InMemoryAdapter` acts on this server only.
- Breaking: `SocketIoEmitter.serverSideEmit(event, args)` takes the event name first. Before, it took one list. Dox servers now get these events.
- `RedisAdapter.close()` now unsubscribes its channels. It does not close the Redis connections.
- Ack ids are now shared by all sockets of a namespace, like Node.js.

## 3.0.0-alpha.12

- The package now lives in its own repo: [dartondox/dox-socketio](https://github.com/dartondox/dox-socketio).
- Breaking: the Dox glue moved here from `dox_core`. Add `dox_socketio` to your app.
  - New `SocketIoService`. Extend it to turn Socket.IO on: `class AppSocketIoService extends SocketIoService { AppSocketIoService() : super(socketIoConfig); }`. It replaces `AppConfig.socketIo`.
  - `SocketIoConfig` is now in this package. `enabled` is now `true` by default.
  - `Dox().socketIo` is now `SocketIoService.server`.

## 3.0.0-alpha.11

- Version bump only, to match the other packages.

## 3.0.0-alpha.10

- Version bump only, to match the other packages.

## 3.0.0-alpha.9

- Version bump only, to match the other packages.

## 3.0.0-alpha.8

- New: `ServerOptions(maxAttachments: 10)`. It is the most binary attachments one packet may have. The default is the same as before (10). A packet with more closes that client.

## 3.0.0-alpha.7

- Uses `ioredis` 2.0.0-alpha.3, which can send and get binary messages. `RedisAdapter` and `SocketIoEmitter` take `ioredis` clients.
- `RedisAdapter` reads msgpack and JSON messages. Before, it read only JSON, so the Socket.IO emitters (Node.js, PHP, Python, Go) did not work. They use msgpack.
- Breaking: `RedisAdapter` writes msgpack by default, like `@socket.io/redis-adapter`. Use `format: RedisFormat.json` for Node.js servers with a JSON parser.
- Fix: binary data in JSON Redis messages is written as a Node.js Buffer (`{"type":"Buffer","data":[...]}`). Before, it was written as a plain list of numbers.
- `RedisAdapter` handles the requests on `socket.io-request#<nsp>#`: `socketsJoin`, `socketsLeave` and `disconnectSockets` from emitters and other servers. It answers `fetchSockets`, `allRooms` and broadcasts with acks from Node.js servers, so they do not wait for a timeout.
- New `SocketIoEmitter(redis)`: sends events, `socketsJoin`, `socketsLeave` and `disconnectSockets` through Redis without a Socket.IO server. Same format as `@socket.io/redis-emitter`.
- New `MsgPack` encoder and decoder.
- Performance: a broadcast builds the websocket text frame and its UTF-8 bytes once and shares them with every client (new `SharedText`). Before, each client built and encoded its own copy.
- Performance: a broadcast with no `except` and at most one room goes over the room members directly. Before, it copied all socket ids into new sets each time.
- Breaking: `SocketIoServer.listen()` starts a native server (`dox_http_native`), not a `dart:io` one. The websocket transport uses the native WebSocket upgrade. Long-polling works the same. `attach()` still takes any `HttpServer`, also a `dart:io` one.
- Security: on the native server, a websocket message over `maxHttpBufferSize` bytes is closed with 1009 in Rust, before Dart reads it.
- Security: a binary packet nested deeper than 32 levels is a parse error (`SocketDecoder(maxDepth: ...)`). Before, a deep packet overflowed the stack and could stop the server. Any decode error now closes only that one connection.
- Security: many CONNECT packets for one namespace at once make only one socket. Before, each one made a socket that stayed in memory.
- Security: websocket compression is off. A small compressed message could grow very big in memory.
- Security: when `CorsOptions.origins` lists origins, a websocket from another origin gets 403. With no `cors`, or with `*`, any origin is still allowed.
- Breaking: `CorsOptions(origins: ['*'], credentials: true)` throws an `ArgumentError`. Before, it sent back any origin with credentials.
- Security: a middleware error that is not a `SocketIoException` sends `{"message": "Internal error"}` to the client and writes the real error to stderr.
- Security: a client whose CONNECT is refused is closed after `connectTimeout`. Before, it could stay connected forever.

- Security: a binary packet may have at most 10 attachments (`SocketDecoder(maxAttachments: ...)`). More closes the connection with a parse error. Before, a client could send `51000000-[...]` before any auth and the server kept every binary frame after it in memory.

## 3.0.0-alpha.6

- Version bump only, to match the other packages.

## 3.0.0-alpha.5

- Version bump only, to match the other packages.

## 3.0.0-alpha.4

- Version bump only, to match the other packages.

## 3.0.0-alpha.3

- Version bump only, to match the other packages.

## 3.0.0-alpha.2

- Needs Dart 3.13 or newer.
- All dependencies updated to their latest versions.

## 3.0.0-alpha.1

- Uses `ioredis` 2.
- `RedisAdapter` listens with `PSUBSCRIBE`, so it also gets the room channels that the Node.js redis adapter uses for one room. `onRedisMessage` now takes `(pattern, channel, message)`.
- First version. Socket.IO v4 server (Engine.IO v4, Socket.IO protocol v5).
- Polling and websocket transports, upgrade, heartbeat, CORS.
- Namespaces, middleware, rooms, broadcast, acks, binary.
- In-memory adapter and Redis adapter.
