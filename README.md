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
