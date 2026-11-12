---
name: dox-socketio-guide
description: Use when you add realtime to a Dox (Dart) app with dox_socketio. Covers the SocketIoProvider and config, namespaces, events and acks, rooms, socket login with SocketAuth, many isolates or servers (Redis adapter), emitting from a queue job, and broadcasting on public, private and presence channels.
---

# dox_socketio guide

`dox_socketio` is a Socket.IO v4 server for Dart. The official clients (`socket.io-client` for JavaScript, `socket_io_client` for Dart) connect with no change. In a Dox app it runs on the same port as the HTTP routes, at `/socket.io/`. It can also run alone, without Dox.

For routes, controllers and app layout, see the `dox-core-guide` skill.

## Install and register

1. `dart pub add dox_socketio`
2. Create the config in `lib/config/socketio.dart`:

```dart
import 'package:dox_socketio/dox_socketio.dart';
import 'package:my_app/app/socket/chat.namespace.dart';
import 'package:my_app/config/env.dart';

SocketIoConfig socketIoConfig = SocketIoConfig(
  enabled: env.socketioEnabled,
  namespaces: <NamespaceHandler>[ChatNamespace()],
);
```

`SocketIoConfig` options: `enabled` (default `true`), `options` (`ServerOptions()`), `namespaces` (`[]`), `adapter` (null), `broadcasting` (default `false`), `whisper` (default `false`, needs `broadcasting`).

3. Create the provider in `lib/providers/socketio_provider.dart`. The generated container finds it, so run `dart run build_runner build` after you add it.

```dart
import 'package:dox_socketio/dox_socketio.dart';
import 'package:my_app/config/socketio.dart';

class AppSocketIoProvider extends SocketIoProvider {
  AppSocketIoProvider() : super(socketIoConfig);
}
```

4. Add an env getter and key if you use `env.socketioEnabled`: `bool get socketioEnabled => false;` in `lib/config/env.dart`, and `SOCKETIO_ENABLED=true` in `.env`. Then run `dart run build_runner build` to update `lib/config/env.g.dart`.

## Recipes

### 1. A namespace with events and acks

```dart
import 'package:dox_socketio/dox_socketio.dart';

class ChatNamespace extends NamespaceHandler {
  @override
  String get path => '/chat';

  @override
  void onConnection(Socket socket) {
    socket.emit('welcome', <String, dynamic>{'id': socket.id});
    socket.join('lobby');
    socket.on('message', onMessage);
    socket.on('disconnect', onDisconnect);
  }

  void onMessage(SocketEvent event) {
    event.socket.to('lobby').emit('message', event.data); // all in lobby but me
    event.ack('sent'); // answer the client callback
  }

  void onDisconnect(SocketEvent event) {
    print('left: ${event.data}'); // the reason, eg. "transport close"
  }
}
```

Dependencies come in through the constructor: `ChatNamespace(MessageService())`, then `namespaces: <NamespaceHandler>[ChatNamespace(MessageService())]`.

Event data: `event.data` (first argument), `event.args` (all), `event.socket`, `event.ack(x)`. To send several arguments use `socket.emitArgs('e', [a, b])`. Send binary data as a `Uint8List`.

### 2. Rooms and broadcast

```dart
socket.join('lobby');
socket.leave('lobby');
socket.to('lobby').emit('news', data);       // to a room, not to me
socket.broadcast.emit('news', data);          // to everyone, not to me
io.to('room').except('vip').emit('news', data);
```

### 3. Send from a controller, service or job

`SocketIoProvider.server` is the server of this isolate. It is null before the provider started.

```dart
await SocketIoProvider.server?.of('/chat').to('lobby').emit('news', data);
```

In a process with no Socket.IO server (queue worker, cron job), use the emitter. The servers must use the Redis adapter.

```dart
import 'package:dox_socketio/dox_socketio.dart';
import 'package:ioredis/ioredis.dart';

Redis redis = Redis(RedisOptions(host: '127.0.0.1', port: 6379));
SocketIoEmitter emitter = SocketIoEmitter(redis);
await emitter.of('/chat').to('vip').emit('notice', 'for vip');
await redis.disconnect();
```

### 4. Middleware and refusing a client

```dart
@override
List<Middleware> get middleware => <Middleware>[checkToken];

void checkToken(Socket socket) {
  if (socket.handshake.auth['token'] != 'secret') {
    throw SocketIoException('unauthorized', data: <String, dynamic>{'reason': 'bad token'});
  }
}
```

A thrown `SocketIoException` gives the client a `connect_error` with the message and data. Any other error goes to stderr and the client only sees `Internal error`.

### 5. Log in a socket

Socket.IO gets its requests before the Dox router, so route middleware (session, auth) does not run. Use `SocketAuth` as namespace middleware.

```dart
class ChatNamespace extends NamespaceHandler {
  @override
  String get path => '/chat';

  @override
  List<Middleware> get middleware => <Middleware>[SocketAuth(findUser).handle];

  static Future<User?> findUser(Handshake handshake) async {
    String? token = handshake.token; // auth.token, else Authorization: Bearer
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

- A null user means the client gets `connect_error` `Unauthorized` and `onConnection` does not run. Change the text with `SocketAuth(findUser, message: '...')`.
- `handshake.cookie('name')` reads and decrypts a cookie. `await handshake.session()` gives the read-only Dox session.
- With `dox_auth`: `Auth(guard: 'api').userFromToken(token)` or `Auth(guard: 'web').userFromSession(await handshake.session())`.
- For the default namespace use `io.use(SocketAuth(findUser).handle)`.
- Browsers can not set headers on a websocket. The client sends `io(url, { auth: { token } })`.

### 6. Many isolates and many servers

Dox runs one isolate per core, each with its own Socket.IO server.

- With no `adapter` and many isolates, the provider uses `IsolateAdapter`. No Redis needed.
- Many servers need the Redis adapter. Add `dart pub add ioredis` and a factory:

```dart
import 'package:ioredis/ioredis.dart';

class RedisSocketAdapterFactory extends SocketAdapterFactory {
  const RedisSocketAdapterFactory();

  @override
  Adapter create() {
    Redis redis = Redis();
    return RedisAdapter(subscriber: redis.duplicate(), publisher: redis);
  }
}

// in SocketIoConfig: adapter: const RedisSocketAdapterFactory(),
```

The factory makes a new connection in each isolate, because a Redis connection can not move between isolates. Then these reach all servers: `await io.to('room').fetchSockets()`, `socketsJoin`, `socketsLeave`, `disconnectSockets`, `io.timeout(Duration(seconds: 5)).emitWithAck('e')`, `serverSideEmit`, `serverSideEmitWithAck`, `io.onServerSideEmit('e', (ServerSideEvent e) => e.ack('ok'))`.

Clients must use `transports: ['websocket']`. Long-polling sends many requests that can reach different isolates.

### 7. Options

```dart
SocketIoConfig(
  options: ServerOptions(
    allowedOrigins: <String>['https://app.example.com'],
    allowRequest: (EngineRequest req) => req.query['key'] == 'secret',
    maxSessions: 10000,
    transports: <String>['websocket'],
    cors: CorsOptions(origins: <String>['https://app.example.com'], credentials: true),
  ),
);
```

Other options: `path` (`/socket.io/`), `pingInterval` (25 s), `pingTimeout` (20 s), `connectTimeout` (45 s), `maxHttpBufferSize` (1,000,000 bytes), `maxAttachments` (10).

### 8. Broadcasting (public, private, presence channels)

Turn it on with `SocketIoConfig(broadcasting: true, ...)`. It listens on the default `/` namespace only (`io.sockets`). The `subscribe` and `unsubscribe` events work there, not on `/chat` or any other namespace you add. The client must connect to `/`, like `io(url)`, for channels. Add channel rules in every isolate, for example in a `DoxProvider.setup()`:

```dart
Broadcast.channel('orders.{id}', (Object? user, String id) {
  return (user as User).id.toString() == id; // true allows
});
Broadcast.channel('chat.{room}', (Object? user, String room) {
  User u = user as User;
  return <String, Object?>{'id': u.id, 'name': u.name}; // presence member, needs id
});
```

Add the token route in your router:

```dart
Broadcast.routes(middleware: <dynamic>[AuthMiddleware()]); // POST /broadcasting/auth
```

Define and send an event:

```dart
class OrderShipped with ShouldBroadcast {
  OrderShipped(this.order);
  final Order order;

  @override
  List<Channel> broadcastOn() => <Channel>[PrivateChannel('orders.${order.id}')];

  @override
  String broadcastAs() => 'order.shipped'; // default: the class name

  @override
  Map<String, Object?> broadcastWith() => <String, Object?>{'id': order.id};
}

await broadcast(OrderShipped(order));
await Broadcast.event(OrderShipped(order), toOthers: Broadcast.socketId(req));
```

Channels: `Channel('news')`, `PrivateChannel('orders.5')` (name `private-orders.5`), `PresenceChannel('chat.lobby')` (name `presence-chat.lobby`). Rules have no prefix. The first matching rule wins.

Client: on a socket of the `/` namespace (`const socket = io(url)`, not `io(url + "/chat")`), call `socket.emit('subscribe', {channel, auth}, ack)` inside the `connect` handler (a reconnect makes a new socket with no channels). Events arrive as `socket.on('order.shipped', (channel, payload) => ...)`. A socket logged in with `SocketAuth` on the `/` namespace (`io.use(SocketAuth(findUser).handle)`) needs no `auth`. `SocketAuth` on `/chat` (recipe 5) does not log in the `/` socket. Otherwise `POST /broadcasting/auth` with `socket_id` and `channel_name` returns `{auth: token}`. Presence channels also send `presence:subscribed`, `presence:joining`, `presence:leaving`. Leave with `socket.emit('unsubscribe', {channel})`.

In a process with no Socket.IO server, set `Broadcast.emitter = SocketIoEmitter(redis)`.

Whisper (client to client, for example "is typing"): turn it on with `SocketIoConfig(broadcasting: true, whisper: true)`. A socket that subscribed to a private or presence channel sends `socket.emit('client event', {channel: 'presence-chat.lobby', event: 'client-typing', data: {name: 'Alice'}})`. The other sockets in the channel (not the sender) get `socket.on('client-typing', (channel, data) => ...)`, on all servers. The event name must start with `client-`. Public channels, sockets that did not subscribe and data over `Broadcast.maxWhisperSize` (10 KB of JSON) are dropped.

### 9. Test broadcasting

```dart
BroadcastFake fake = Broadcast.fake();
await broadcast(OrderShipped(order));
fake.assertBroadcasted<OrderShipped>();
fake.assertBroadcastedOn<OrderShipped>('private-orders.5');
Broadcast.restore();
```

## Gotchas

- `Broadcast.event` throws a `StateError` when the process has no Socket.IO server and no `Broadcast.emitter`.
- Without a `DoxProvider` that adds the channel rules, every private subscribe gets `Forbidden`.
- With `allowedOrigins` null, any website can open a socket with your users' cookies. The server logs a warning. Set `allowedOrigins` when you log in with cookies.
- `cors` with `*` and `credentials: true` throws an `ArgumentError`.
- `socket.user` stays on its own server. `fetchSockets()` on other servers does not return it.
- `fetchSockets` throws a `TimeoutException`, and `serverSideEmitWithAck` or a broadcast `emitWithAck` throws an `AckTimeoutException` (with `responses`), when a server or client does not answer in time. Default `requestsTimeout` is 5 seconds.
- A namespace added with `io.of('/x')` in only one isolate makes `fetchSockets` wait and throw. Put namespaces in the config so every isolate adds them.
- `maxSessions` counts per isolate. With 4 isolates the app allows 4 times the value.
- Broadcasting runs on one namespace only: `/` with `SocketIoConfig(broadcasting: true)`. `Broadcast.listen(nsp)` on a plain server picks another one, and `Broadcast.event` sends through the last one listened. A `subscribe` on a namespace that is not listened gets no answer.
- Whisper is off by default. With it off, a `client event` gets no answer. Do not name a server event `client-...`.
- Broadcast limits: `Broadcast.maxChannelLength` (200), `Broadcast.maxChannelsPerSocket` (100), `Broadcast.presenceTimeout` (2 seconds).
- Standalone, without Dox: `SocketIoServer io = SocketIoServer(); io.register(ChatNamespace()); await io.listen(3001);`.
