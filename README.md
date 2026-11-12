# dox_socketio

Socket.IO v4 server for Dart. It works with the Dox framework and with plain `dart:io`.

[![pub package](https://img.shields.io/pub/v/dox_socketio.svg)](https://pub.dev/packages/dox_socketio)
[![license: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/dartondox/dox-socketio/blob/main/LICENSE)

Part of [Dox](https://dartondox.dev), a web backend framework for Dart.
Source code: [github.com/dartondox/dox-socketio](https://github.com/dartondox/dox-socketio).

## Install

```bash
dart pub add dox_socketio
```

To see what is ready and what may still change, read [Status and roadmap](https://dartondox.dev/latest/concepts/status-and-roadmap/).

## Use it

```dart
import 'package:dox_socketio/dox_socketio.dart';

Future<void> main() async {
  SocketIoServer io = SocketIoServer();
  io.on('connection', onConnection);
  await io.listen(3001);
}

void onConnection(Socket socket) {
  socket.emit('welcome', 'hello');
  socket.join('lobby');
  socket.on('chat', onChat);
}

void onChat(SocketEvent event) {
  event.socket.to('lobby').emit('chat', event.data); // everyone in lobby but me
  event.ack('ok'); // answer the client callback
}
```

The official clients connect with no changes: `socket.io-client` (JavaScript v4) and the Dart `socket_io_client` package.
In a Dox app, extend `SocketIoProvider` in `lib/`. The generated container finds it, and Socket.IO runs on the app port:

```dart
import 'package:dox_socketio/dox_socketio.dart';

class AppSocketIoProvider extends SocketIoProvider {
  AppSocketIoProvider()
    : super(SocketIoConfig(namespaces: <NamespaceHandler>[ChatNamespace()]));
}
```

`SocketIoProvider.server` is the server of this isolate: `SocketIoProvider.server?.to('room').emit('news', data)`.

## Log in a socket

Socket.IO gets its requests before the Dox router, so route middleware (session, auth) does not run for it.
Use `SocketAuth` as a namespace middleware. Your function gets the handshake and gives the user, or null.

```dart
class ChatNamespace extends NamespaceHandler {
  @override
  String get path => '/chat';

  @override
  List<Middleware> get middleware => <Middleware>[SocketAuth(findUser).handle];

  static Future<User?> findUser(Handshake handshake) async {
    String? token = handshake.token;
    if (token == null) return null;
    return User().where('api_token', token).getFirst();
  }

  @override
  void onConnection(Socket socket) {
    User user = socket.user! as User;
    socket.join('user:${user.id}');
  }
}
```

- With a user, it is kept in `socket.user`.
- With null, the client gets a `connect_error` with the message `Unauthorized`, and `onConnection` does not run. Change it with `SocketAuth(findUser, message: '...')`.
- `handshake.token` is `auth.token` from the client (`io(url, { auth: { token } })`), else the `Authorization: Bearer` header.
- `handshake.cookie('name')` reads a cookie and decrypts it with the APP_KEY, like `req.cookie('name')`. Use `decrypt: false` for a plain cookie.
- `await handshake.session()` gives the Dox session of the client, from its session cookie. It has the same checks as `SessionMiddleware`. A changed, foreign or expired cookie gives an empty session. It is read only. With no `allowedOrigins`, or with `'*'` in it, it prints a warning once.
- `io.use(SocketAuth(findUser).handle)` does the same for the default namespace.

With dox_auth, check a JWT or the web login session:

```dart
static Future<User?> findUser(Handshake handshake) async {
  String? token = handshake.token;
  if (token != null) return Auth(guard: 'api').userFromToken(token);
  return Auth(guard: 'web').userFromSession(await handshake.session());
}
```

## Features

- Engine.IO protocol v4 and Socket.IO protocol v5 (the protocols of Socket.IO v4).
- Polling and websocket transports, upgrade, heartbeat and CORS.
- Namespaces (also as classes), middleware, rooms, broadcast, acks and binary data.
- In-memory adapter, an isolate adapter for the isolates of one Dox app (no Redis, the default with many isolates), and a Redis adapter for many servers.
- The Redis adapter uses the message format of `@socket.io/redis-adapter`. Dox and Node.js servers can share one Redis.
- `fetchSockets`, `socketsJoin`, `socketsLeave`, `disconnectSockets`, `serverSideEmit` and broadcast acks work across all servers.
- `SocketIoEmitter` sends events from any Dart program, like `@socket.io/redis-emitter`.
- Broadcasting with public, private and presence channels (opt-in).

## Origins

By default, the server does not check the `Origin` header of polling requests. It checks a websocket only against `cors`, when `cors` is set. So a browser page on another site can connect, with the cookies of your users. The server logs one warning about this when it starts.

To turn on the check, set `allowedOrigins`:

```dart
ServerOptions(allowedOrigins: <String>['https://app.example.com'])
```

- `[]`: only pages of the same host can connect.
- A list: the same host and the listed sites can connect.
- `['*']`: every site can connect.

Then a websocket or polling request from another origin gets `403`. A client with no `Origin` header (not a browser) is always allowed. Case and a trailing `/` do not matter. `cors` still sets the CORS headers for polling.

## Allow requests and limit sessions

`allowRequest` runs for every handshake and every websocket upgrade. It gets the `EngineRequest` (headers, query, address) and gives `true` or `false`, sync or async. `maxSessions` is the most open sessions.

```dart
ServerOptions(
  allowRequest: (EngineRequest req) => req.query['key'] == 'secret',
  maxSessions: 10000,
)
```

- The hook says `false` or throws: the client gets `403` with `{"code":4,"message":"Forbidden"}`.
- At the limit, a new handshake gets `503` with `{"code":3,"message":"Too many sessions"}`. Open sessions keep working.
- The count is per isolate. With 4 isolates, the app allows 4 times `maxSessions`.
- Keep the hook fast. A slow hook keeps a slot busy longer.

Both are null by default: all requests are allowed and there is no limit.

## Broadcasting

Send server events to channels. It is off by default. Turn it on in the config (or call `Broadcast.listen(io.sockets)` on a plain server):

```dart
SocketIoConfig(broadcasting: true, namespaces: <NamespaceHandler>[ChatNamespace()]);
```

Add rules for private and presence channels, and the auth route, in every isolate (for example in a router):

```dart
Broadcast.channel('orders.{id}', (Object? user, String id) => (user as User).id.toString() == id);
Broadcast.channel('chat.{room}', (Object? user, String room) {
  User u = user as User;
  return <String, Object?>{'id': u.id, 'name': u.name}; // presence member
});
Broadcast.routes(middleware: <dynamic>[AuthMiddleware()]); // POST /broadcasting/auth
```

A rule has no prefix, so `orders.{id}` checks both `private-orders.1` and `presence-orders.1`. When many patterns match, the first one added wins.

Send an event:

```dart
class OrderShipped with ShouldBroadcast {
  OrderShipped(this.order);
  final Order order;

  @override
  List<Channel> broadcastOn() => <Channel>[PrivateChannel('orders.${order.id}')];

  @override
  Map<String, Object?> broadcastWith() => <String, Object?>{'id': order.id};
}

await broadcast(OrderShipped(order));
await Broadcast.event(OrderShipped(order), toOthers: Broadcast.socketId(req));
```

The client:

- `socket.emit('subscribe', {channel: 'private-orders.5', auth?}, ack)`. The ack is `{ok: true}` or `{ok: false, error}`.
- A socket logged in with `SocketAuth` needs no `auth`. Else get a token first: `POST /broadcasting/auth` with `socket_id` and `channel_name` gives `{auth: token}`.
- Events come as `socket.on('OrderShipped', (channel, payload) => ...)`.
- Presence channels also send `presence:subscribed` (channel, members), `presence:joining` and `presence:leaving` (channel, member). A member is `{user_id, user_info}`.
- `socket.emit('unsubscribe', {channel})` leaves.

In a process with no Socket.IO server, like a queue worker, set `Broadcast.emitter = SocketIoEmitter(redis)`. The servers must use the Redis adapter. In tests, `Broadcast.fake()` keeps the events and sends nothing.

The event names and the presence names follow Laravel Echo's Socket.IO connector. The auth does not: Echo sends `auth` as an object with headers, and this server wants a token string. So get the token yourself and send a small `subscribe`, for example with `socket.io-client`:

```js
const socket = io('https://app.example.com', { transports: ['websocket'] });
socket.on('connect', async () => {
  const res = await fetch('/broadcasting/auth', {
    method: 'POST',
    headers: { 'content-type': 'application/json', authorization: `Bearer ${apiToken}` },
    body: JSON.stringify({ socket_id: socket.id, channel_name: 'private-orders.5' }),
  });
  const { auth } = await res.json();
  socket.emit('subscribe', { channel: 'private-orders.5', auth }, (ack) => console.log(ack));
});
socket.on('OrderShipped', (channel, payload) => console.log(channel, payload));
```

Turn broadcasting on for one namespace only. With `listen` on two namespaces, both accept `subscribe`, but `Broadcast.event` sends only through the last one.

Limits:

- `Broadcast.maxChannelLength` (default 200) and `Broadcast.maxChannelsPerSocket` (default 100). Over a limit, the ack is `{ok: false, error: 'channel name is too long'}` or `'too many channels'`.
- The member list uses `fetchSockets`, so it covers all isolates and servers of the adapter. When a server does not answer in time, a presence `subscribe` is refused (`'presence is not available'`), and a leave still sends `presence:leaving`.
- During an outage (a dead Redis node or isolate), each presence join or leave of a channel waits up to `Broadcast.presenceTimeout` (default 2 seconds), one after another.
- Presence joins and leaves of one channel run one after another in each isolate, so one isolate is exact. When two sockets of one user subscribe or leave at the same moment on two isolates or servers, `presence:joining` or `presence:leaving` can be missed or sent twice.

## Many servers

Use the Redis adapter on each server:

```dart
Redis subscriber = Redis(RedisOptions(host: '127.0.0.1', port: 6379));
SocketIoServer io = SocketIoServer(
  adapter: RedisAdapter(
    subscriber: subscriber,
    publisher: subscriber.duplicate(),
  ),
);
```

Then these act on the sockets of all servers:

```dart
List<RemoteSocket> sockets = await io.to('room1').fetchSockets();
await io.to('room1').socketsJoin('vip');
await io.to('vip').socketsLeave('room1');
await io.except('vip').disconnectSockets();

// Acks from clients on all servers. A timeout is needed.
List<Object?> answers = await io
    .timeout(Duration(seconds: 5))
    .emitWithAck('are-you-there');

// Events between servers. Clients do not get them.
io.onServerSideEmit('reload', (ServerSideEvent event) => event.ack('done'));
await io.serverSideEmit('reload', <Object?>['config']);
List<Object?> done = await io.serverSideEmitWithAck('reload');
```

- `fetchSockets()` gives a `Socket` for sockets of this server and a `RemoteSocket` for the others. A `RemoteSocket` has `id`, `handshake`, `rooms` and `data` (copies) and `emit`, `join`, `leave` and `disconnect`, which go through Redis.
- `socketsJoin`, `socketsLeave` and `disconnectSockets` complete when this server is done and the request is sent. They do not wait for the other servers.
- `fetchSockets` and `serverSideEmitWithAck` wait for an answer from each server, for at most `requestsTimeout` (default 5 seconds, like Node.js). When a server does not answer in time, they throw, like Node.js: `fetchSockets` throws a `TimeoutException`, and `serverSideEmitWithAck` throws an `AckTimeoutException` whose `responses` holds the answers that came.
- `emitWithAck` on a broadcast throws an `AckTimeoutException` when not all clients answer in time. Its `responses` holds the acks that came.
- With the in-memory adapter, all of this acts on the local server only.

## Documentation

- [Socket.IO](https://dartondox.dev/latest/packages/socketio/)
- [API reference](https://pub.dev/documentation/dox_socketio/latest/)

## License

MIT. See [LICENSE](https://github.com/dartondox/dox-socketio/blob/main/LICENSE).
