import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:crypto/crypto.dart' show Hmac, sha256;
import 'package:dox_core/dox_core.dart';

import '../adapter/in_memory_adapter.dart';
import '../adapter/pub_sub_adapter.dart';
import '../adapter/socket_io_emitter.dart';
import '../server/broadcast_operator.dart';
import '../server/namespace.dart';
import '../server/remote_socket.dart';
import '../server/socket.dart';
import 'broadcast_fake.dart';

/// A public channel. Any client can subscribe to it.
/// ```
/// Channel('news')
/// ```
class Channel {
  const Channel(this.name);

  /// The full name, with the `private-` or `presence-` prefix.
  final String name;

  @override
  String toString() => name;
}

/// A private channel: `private-<name>`. A client needs a passing
/// [Broadcast.channel] rule to subscribe.
class PrivateChannel extends Channel {
  const PrivateChannel(String name) : super('private-$name');
}

/// A presence channel: `presence-<name>`. Like a private channel, and
/// the members know who else is in it.
class PresenceChannel extends Channel {
  const PresenceChannel(String name) : super('presence-$name');
}

/// An event that is sent to clients on channels. Send it with `Broadcast.event(event)` or
/// `broadcast(event)`.
/// ```
/// class OrderShipped with ShouldBroadcast {
///   OrderShipped(this.order);
///   final Order order;
///
///   @override
///   List<Channel> broadcastOn() => <Channel>[PrivateChannel('orders.${order.id}')];
///
///   @override
///   Map<String, Object?> broadcastWith() => <String, Object?>{'id': order.id};
/// }
/// ```
abstract mixin class ShouldBroadcast {
  /// The channels to send to.
  List<Channel> broadcastOn();

  /// The event name. Default: the class name.
  String broadcastAs() => runtimeType.toString();

  /// The payload. It must be JSON. Default: an empty map.
  Map<String, Object?> broadcastWith() => const <String, Object?>{};
}

/// A [ShouldBroadcast] event that goes to the queue: `Broadcast.event`
/// gives its [BroadcastPayload] to the hook of [Broadcast.useQueue], and
/// a queue job sends it later with [Broadcast.deliver].
///
/// With no hook set, it is sent at once, like any [ShouldBroadcast].
/// ```
/// class OrderShipped with ShouldBroadcastQueued {
///   @override
///   List<Channel> broadcastOn() => <Channel>[PrivateChannel('orders.1')];
/// }
/// ```
abstract mixin class ShouldBroadcastQueued implements ShouldBroadcast {
  @override
  String broadcastAs() => runtimeType.toString();

  @override
  Map<String, Object?> broadcastWith() => const <String, Object?>{};
}

/// One broadcast as JSON data, so a queue can save it: the channel names,
/// the event name, the payload and the `toOthers` socket id.
class BroadcastPayload {
  const BroadcastPayload({
    required this.channels,
    required this.event,
    this.data = const <String, Object?>{},
    this.except,
  });

  /// The payload of [event].
  factory BroadcastPayload.of(ShouldBroadcast event, {String? toOthers}) {
    return BroadcastPayload(
      channels: <String>[
        for (Channel channel in event.broadcastOn()) channel.name,
      ],
      event: event.broadcastAs(),
      data: event.broadcastWith(),
      except: toOthers,
    );
  }

  /// Reads the map of [toJson].
  factory BroadcastPayload.fromJson(Map<String, Object?> json) {
    return BroadcastPayload(
      channels: List<String>.from(json['channels']! as List<Object?>),
      event: json['event']! as String,
      data: Map<String, Object?>.from(json['data']! as Map<Object?, Object?>),
      except: json['except'] as String?,
    );
  }

  /// The channel names, with their `private-` or `presence-` prefix.
  final List<String> channels;

  /// The event name (`broadcastAs`).
  final String event;

  /// The payload (`broadcastWith`). It must be JSON.
  final Map<String, Object?> data;

  /// The socket id that does not get it (`toOthers`), or null.
  final String? except;

  Map<String, Object?> toJson() => <String, Object?>{
    'channels': channels,
    'event': event,
    'data': data,
    'except': except,
  };
}

/// Sends [event] to its channels. Same as [Broadcast.event].
Future<void> broadcast(ShouldBroadcast event, {String? toOthers}) =>
    Broadcast.event(event, toOthers: toOthers);

/// Channel rules, the socket `subscribe` events, the HTTP auth endpoint
/// and the sending of [ShouldBroadcast] events. It works on Socket.IO only.
///
/// The event names follow the Laravel Echo Socket.IO connector, so Echo
/// clients work. The auth does not: the client gets a token from [authenticate] and sends it in
/// `subscribe` itself.
/// - client -> server: `subscribe` with `{channel, auth?}`, and
///   `unsubscribe` with `{channel}`. Both answer an ack with
///   `{ok: true}` or `{ok: false, error}`.
/// - server -> client: `<event name>` with `(channel, payload)`.
/// - presence: `presence:subscribed` with `(channel, members)` to the new
///   socket, `presence:joining` and `presence:leaving` with
///   `(channel, member)` to the others. A member is
///   `{user_id, user_info}`.
/// - whisper (only with [whisper] on): client -> server `client event`
///   with `{channel, event, data}`, then server -> the other members
///   `<event>` with `(channel, data)`. See [whisper].
class Broadcast {
  Broadcast._();

  /// How long a token of [authenticate] is valid.
  static Duration tokenLifetime = const Duration(minutes: 1);

  /// Sends events in a process with no Socket.IO server, like a queue
  /// worker. The servers must use the Redis adapter with the same key.
  /// ```
  /// Broadcast.emitter = SocketIoEmitter(Redis(RedisOptions()));
  /// ```
  /// Use `.of('/name')` when [listen] got a namespace that is not `/`.
  static SocketIoEmitter? emitter;

  /// The longest channel name a socket may subscribe to.
  static int maxChannelLength = 200;

  /// The most channels one socket may be subscribed to.
  static int maxChannelsPerSocket = 100;

  /// How long a presence join or leave waits for the member list of the
  /// other servers. Joins and leaves of one channel wait in line, so keep
  /// it short.
  static Duration presenceTimeout = const Duration(seconds: 2);

  /// Lets a client send an event to the other members of a private or
  /// presence channel it joined (a "whisper", for example "is typing").
  /// Off by default, because it lets clients message each other.
  /// `SocketIoConfig(broadcasting: true, whisper: true)` turns it on.
  ///
  /// The client sends `client event` with `{channel, event, data}`. The
  /// `event` must start with `client-`, so it can not look like a
  /// server event. The server sends `<event>` with `(channel, data)` to
  /// the other sockets in the channel, on all servers, never back to the
  /// sender. It drops (and answers an ack with `{ok: false, error}`) a
  /// whisper on a public channel, from a socket that did not subscribe,
  /// or with data bigger than [maxWhisperSize].
  /// ```js
  /// socket.emit('client event', {
  ///   channel: 'private-chat.1', event: 'client-typing', data: { name: 'Alice' },
  /// });
  /// socket.on('client-typing', (channel, data) => {});
  /// ```
  static bool whisper = false;

  /// The biggest whisper `data`, in bytes of JSON. Default: 10 KB.
  static int maxWhisperSize = 10 * 1024;

  /// The key of the presence members in `socket.data`.
  static const String presenceKey = 'broadcast_presence';

  /// The `APP_KEY` label of the token key.
  static const String _keyLabel = 'dox:broadcast';

  static final List<_Rule> _rules = <_Rule>[];
  static Namespace? _nsp;
  static BroadcastFake? _fake;
  static Future<void> Function(BroadcastPayload payload)? _queue;
  static bool _deliverWarned = false;

  /// Namespaces that [listen] already set up.
  static final Expando<bool> _listening = Expando<bool>();

  /// Channels each socket is subscribing to now (the rule still runs).
  /// True when an `unsubscribe` came in the meantime.
  static final Expando<Map<String, bool>> _pending =
      Expando<Map<String, bool>>();

  /// The last presence join or leave of each channel. They run one after
  /// another in this isolate, so "first socket of this user" is right.
  static final Map<String, Future<void>> _locks = <String, Future<void>>{};

  /// A Socket.IO id: 20 url-safe base64 characters (see `IdGenerator`).
  static final RegExp _socketId = RegExp(r'^[A-Za-z0-9_-]{20}$');

  /// Adds the rule for private and presence channels that match
  /// [pattern]. The pattern has no `private-` or `presence-` prefix. A
  /// `{name}` part matches any text with no dot. [rule] gets the user,
  /// then one string per `{name}` part, in order.
  ///
  /// For a private channel it gives true to allow. For a presence
  /// channel it gives the member info (a map, with an `id`) to allow.
  /// Null, false or a throw refuses. It can be async.
  ///
  /// One rule serves both kinds: `orders.{id}` checks `private-orders.1`
  /// and `presence-orders.1`. When many patterns match, the first one
  /// added wins.
  /// ```
  /// Broadcast.channel('orders.{id}', (Object? user, String id) async {
  ///   Order? order = await Order().find(id);
  ///   return order?.userId == (user as User).id;
  /// });
  /// Broadcast.channel('chat.{room}', (Object? user, String room) {
  ///   User u = user as User;
  ///   return <String, Object?>{'id': u.id, 'name': u.name};
  /// });
  /// ```
  static void channel(String pattern, Function rule) {
    _rules.add(_Rule(pattern, rule));
  }

  /// Turns on `subscribe` and `unsubscribe` for the sockets of [nsp], and
  /// sends [event]s through it. `SocketIoConfig(broadcasting: true)`
  /// calls it for `/`. A second call for the same namespace only makes it
  /// the send target again.
  static void listen(Namespace nsp) {
    _nsp = nsp;
    if (_listening[nsp] == true) return;
    _listening[nsp] = true;
    nsp.onConnection(_onConnection);
  }

  /// For tests: forgets the rules, the send target of [listen], the
  /// [emitter], the fake, the queue hook and turns [whisper] off. Sockets of a namespace that [listen] got can
  /// still subscribe.
  static void reset() {
    _rules.clear();
    _nsp = null;
    emitter = null;
    _fake = null;
    _queue = null;
    whisper = false;
  }

  /// Registers `POST [path]`, the HTTP auth endpoint (see
  /// [authenticate]). Put your auth middleware in [middleware].
  /// ```
  /// Broadcast.routes(middleware: <dynamic>[AuthMiddleware()]);
  /// ```
  static Route routes({
    String path = '/broadcasting/auth',
    List<dynamic> middleware = const <dynamic>[],
  }) {
    return Route.post(path, <dynamic>[...middleware, authenticate]);
  }

  /// The handler of the HTTP auth endpoint. It reads `socket_id` and
  /// `channel_name`, runs the channel rule with the user of the request
  /// (`req.auth`) and answers `{auth: token}`. A refused user, or a
  /// `socket_id` that is not a Socket.IO id, gets 403.
  /// The client sends the token with `subscribe`: `{channel, auth}`.
  static Future<Map<String, String>> authenticate(DoxRequest req) async {
    Object? socketId = req.input('socket_id');
    Object? channel = req.input('channel_name');
    IAuth? auth = req.auth;
    Object? result =
        socketId is! String ||
            !_socketId.hasMatch(socketId) ||
            channel is! String ||
            channel.isEmpty
        ? null
        : await authorize(auth?.user<Object>(), channel);
    if (result == null) throw ForbiddenHttpException();
    Map<String, Object?>? member = result is Map
        ? _member(result, auth?.id())
        : null;
    return <String, String>{
      'auth': authToken(socketId as String, channel as String, member),
    };
  }

  /// The socket id the client sent in the `X-Socket-ID` header, for
  /// `toOthers`.
  static String? socketId(IDoxRequest req) => req.header('x-socket-id');

  /// Runs the rule of [channel] for [user]. Gives null when refused,
  /// true for an allowed public or private channel, and the member info
  /// map for an allowed presence channel. A public channel is always
  /// allowed.
  static Future<Object?> authorize(Object? user, String channel) async {
    bool presence = channel.startsWith('presence-');
    if (!presence && !channel.startsWith('private-')) return true;
    if (user == null) return null;
    String name = channel.substring(channel.indexOf('-') + 1);
    for (_Rule rule in _rules) {
      List<String>? params = rule.match(name);
      if (params == null) continue;
      try {
        Object? result = await Function.apply(rule.callback, <Object?>[
          user,
          ...params,
        ]);
        if (presence) return result is Map ? result : null;
        return result == true ? true : null;
      } catch (error, stack) {
        stderr.writeln('dox_socketio: channel rule error: $error\n$stack');
        return null;
      }
    }
    return null;
  }

  /// A signed token that lets socket [socketId] subscribe to [channel]
  /// until [tokenLifetime] passes. [member] is the presence member.
  /// Needs the `APP_KEY` of the Dox app.
  ///
  /// Format: `<base64url of JSON>.<hex HMAC-SHA256>`. The JSON has
  /// `socket_id`, `channel`, `expires` (seconds since epoch) and
  /// `member`. The key is made from `APP_KEY` for this use only.
  static String authToken(
    String socketId,
    String channel, [
    Map<String, Object?>? member,
  ]) {
    int expires =
        DateTime.now().add(tokenLifetime).millisecondsSinceEpoch ~/ 1000;
    String body = base64Url.encode(
      utf8.encode(
        jsonEncode(<String, Object?>{
          'socket_id': socketId,
          'channel': channel,
          'expires': expires,
          'member': ?member,
        }),
      ),
    );
    return '$body.${_sign(body)}';
  }

  /// Sends [event] to the sockets that subscribed to its channels. With
  /// [toOthers], the socket with that id does not get it.
  ///
  /// It sends through the namespace of [listen]. With no namespace, it
  /// sends through [emitter]. With neither, it throws a [StateError].
  ///
  /// A [ShouldBroadcastQueued] event goes to the hook of [useQueue]
  /// instead. With no hook, it is sent at once.
  static Future<void> event(ShouldBroadcast event, {String? toOthers}) async {
    bool queued = event is ShouldBroadcastQueued;
    BroadcastFake? fake = _fake;
    if (fake != null) {
      fake.broadcasted.add(
        BroadcastedEvent(event, toOthers: toOthers, queued: queued),
      );
      return;
    }
    BroadcastPayload payload = BroadcastPayload.of(event, toOthers: toOthers);
    Future<void> Function(BroadcastPayload payload)? push = _queue;
    if (queued && push != null) return push(payload);
    return _send(payload);
  }

  /// Sets the hook that puts [ShouldBroadcastQueued] events on your
  /// queue. The queue job calls [deliver] with the payload. Null removes
  /// the hook, so those events are sent at once again.
  /// ```
  /// Broadcast.useQueue(queueBroadcast);
  ///
  /// Future<void> queueBroadcast(BroadcastPayload payload) async {
  ///   await Queue.dispatch(BroadcastJob(payload: payload.toJson()));
  /// }
  /// ```
  static void useQueue(Future<void> Function(BroadcastPayload payload)? push) {
    _queue = push;
  }

  /// Sends a [payload] that came from the queue. Call it in the `handle`
  /// of the queue job: `Broadcast.deliver(BroadcastPayload.fromJson(json))`.
  ///
  /// It sends like [event]. A queue worker is often its own process, so
  /// the Socket.IO servers must use the Redis adapter (the worker starts
  /// your providers, so it uses the same adapter), or the worker must set
  /// [emitter]. It prints a warning once when it sends through an
  /// in-memory adapter with no sockets in this process, because then the
  /// clients of the server process do not get it.
  static Future<void> deliver(BroadcastPayload payload) {
    BroadcastFake? fake = _fake;
    if (fake != null) {
      fake.delivered.add(payload);
      return Future<void>.value();
    }
    Namespace? nsp = _nsp;
    // ponytail: a guess. A busy server with no sockets yet warns once too.
    if (!_deliverWarned &&
        nsp != null &&
        nsp.adapter is InMemoryAdapter &&
        nsp.adapter is! PubSubAdapter &&
        nsp.sockets.isEmpty) {
      _deliverWarned = true;
      stderr.writeln(
        'dox_socketio: Broadcast.deliver sent through an in-memory adapter '
        'with no sockets in this process. A queue worker in its own process '
        'can not reach the clients this way: use the Redis adapter '
        '(SocketIoConfig(adapter: ...)) or set Broadcast.emitter.',
      );
    }
    return _send(payload);
  }

  static Future<void> _send(BroadcastPayload payload) async {
    Namespace? nsp = _nsp;
    SocketIoEmitter? out = emitter;
    if (nsp == null && out == null) {
      throw StateError(
        'Broadcast has nowhere to send: turn on broadcasting on the '
        'server, or set Broadcast.emitter (in a queue worker, use the Redis '
        'adapter or an emitter)',
      );
    }
    String name = payload.event;
    String? toOthers = payload.except;
    for (String channel in payload.channels) {
      List<Object?> args = <Object?>[channel, payload.data];
      String room = roomOf(channel);
      if (nsp != null) {
        BroadcastOperator to = nsp.to(room);
        if (toOthers != null) to = to.except(toOthers);
        await to.emitArgs(name, args);
      } else {
        SocketIoEmitterOperator to = out!.to(room);
        if (toOthers != null) to = to.except(toOthers);
        await to.emitArgs(name, args);
      }
    }
  }

  /// Keeps sent events in a [BroadcastFake] and sends nothing. For tests.
  static BroadcastFake fake() => _fake = BroadcastFake();

  /// Turns [fake] off.
  static void restore() => _fake = null;

  /// The room of [channel]. The prefix keeps channels apart from socket
  /// ids and your own rooms.
  static String roomOf(String channel) => 'broadcast:$channel';

  static String _sign(String body) {
    return Hmac(
      sha256,
      KeyDerivation.forAppKey(_keyLabel),
    ).convert(utf8.encode(body)).toString();
  }

  /// The payload of [token] when it is signed, not expired and made for
  /// [socketId] and [channel]. Else null.
  static Map<String, dynamic>? _verify(
    String token,
    String socketId,
    String channel,
  ) {
    int dot = token.lastIndexOf('.');
    if (dot < 0) return null;
    String body = token.substring(0, dot);
    if (!Hash.equals(_sign(body), token.substring(dot + 1))) return null;
    Object? json;
    try {
      json = jsonDecode(utf8.decode(base64Url.decode(body)));
    } on FormatException {
      return null;
    }
    if (json is! Map<String, dynamic>) return null;
    Object? expires = json['expires'];
    int now = DateTime.now().millisecondsSinceEpoch ~/ 1000;
    if (json['socket_id'] != socketId ||
        json['channel'] != channel ||
        expires is! int ||
        expires < now) {
      return null;
    }
    return json;
  }

  static Map<String, Object?> _member(Map<dynamic, dynamic> info, Object? id) {
    return <String, Object?>{'user_id': info['id'] ?? id, 'user_info': info};
  }

  /// The id of a logged-in user: `user['id']` for a map, else the `id`
  /// of the object. Null when it has none, like `req.auth?.id()`.
  static Object? _userId(Object? user) {
    if (user is Map) return user['id'];
    try {
      // ponytail: dynamic call, a user model may have any class
      return (user as dynamic).id;
    } on NoSuchMethodError {
      return null;
    }
  }

  static void _onConnection(Socket socket) {
    socket.on('subscribe', _onSubscribe);
    socket.on('unsubscribe', _onUnsubscribe);
    socket.on('client event', _onWhisper);
    socket.on('disconnect', _onDisconnect);
  }

  static Future<void> _onSubscribe(SocketEvent event) async {
    Object? data = event.data;
    Map<dynamic, dynamic> body = data is Map ? data : const <String, Object>{};
    Object? channel = body['channel'];
    if (channel is! String || channel.isEmpty) {
      event.ack(_error('channel is missing'));
      return;
    }
    if (channel.length > maxChannelLength) {
      event.ack(_error('channel name is too long'));
      return;
    }
    Socket socket = event.socket;
    if (socket.rooms.contains(roomOf(channel))) {
      event.ack(_ok);
      return;
    }
    Map<String, bool> pending = _pending[socket] ??= <String, bool>{};
    if (pending.containsKey(channel)) {
      event.ack(_error('already subscribing'));
      return;
    }
    int count = socket.rooms.where(_isChannelRoom).length + pending.length;
    if (count >= maxChannelsPerSocket) {
      event.ack(_error('too many channels'));
      return;
    }
    pending[channel] = false;
    try {
      event.ack(await _subscribe(socket, channel, body['auth']));
    } finally {
      pending.remove(channel);
    }
  }

  static Future<Map<String, Object>> _subscribe(
    Socket socket,
    String channel,
    Object? token,
  ) async {
    Object? grant = await _grant(socket, channel, token);
    if (grant == null || !socket.connected) return _error('Forbidden');
    if (_cancelled(socket, channel)) return _error('unsubscribed');
    if (grant is Map<String, Object?>) {
      String? error = await _presenceJoin(socket, channel, grant);
      return error == null ? _ok : _error(error);
    }
    socket.join(roomOf(channel));
    return _ok;
  }

  /// True when an `unsubscribe` of [channel] came while it subscribes.
  static bool _cancelled(Socket socket, String channel) =>
      _pending[socket]?[channel] == true;

  static bool _isChannelRoom(String room) => room.startsWith('broadcast:');

  /// True or a presence member when [socket] may subscribe to
  /// [channel]: with its logged-in user and the rule, or with a token.
  static Future<Object?> _grant(
    Socket socket,
    String channel,
    Object? token,
  ) async {
    if (!channel.startsWith('private-') && !channel.startsWith('presence-')) {
      return true;
    }
    if (socket.user != null) {
      Object? result = await authorize(socket.user, channel);
      if (result is Map) {
        return _member(result, _userId(socket.user) ?? socket.id);
      }
      if (result != null) return result;
    }
    if (token is! String) return null;
    Map<String, dynamic>? payload = _verify(token, socket.id, channel);
    if (payload == null) return null;
    Object? member = payload['member'];
    if (!channel.startsWith('presence-')) return true;
    return member is Map<String, dynamic> ? member : null;
  }

  static Future<void> _onUnsubscribe(SocketEvent event) async {
    Object? data = event.data;
    Object? channel = data is Map ? data['channel'] : null;
    if (channel is! String) {
      event.ack(_error('channel is missing'));
      return;
    }
    Map<String, bool>? pending = _pending[event.socket];
    if (pending != null && pending.containsKey(channel)) {
      pending[channel] = true;
    }
    event.socket.leave(roomOf(channel));
    await _presenceLeave(event.socket, channel);
    event.ack(_ok);
  }

  /// Sends a `client-` event to the other members of the channel. Off
  /// unless [whisper] is on: then the event is ignored, as before.
  static Future<void> _onWhisper(SocketEvent event) async {
    if (!whisper) return;
    Object? data = event.data;
    Map<dynamic, dynamic> body = data is Map ? data : const <String, Object>{};
    Object? channel = body['channel'];
    Object? name = body['event'];
    Socket socket = event.socket;
    String? error;
    if (channel is! String || name is! String) {
      error = 'channel or event is missing';
    } else if (!name.startsWith('client-')) {
      error = 'event must start with client-';
    } else if (name.length > maxChannelLength) {
      error = 'event name is too long';
    } else if (!channel.startsWith('private-') &&
        !channel.startsWith('presence-')) {
      error = 'whisper needs a private or presence channel';
    } else if (!socket.rooms.contains(roomOf(channel))) {
      error = 'not subscribed';
    } else if (_size(body['data']) > maxWhisperSize) {
      error = 'data is too big';
    }
    if (error != null) {
      event.ack(_error(error));
      return;
    }
    await socket.nsp.to(roomOf(channel as String)).except(socket.id).emitArgs(
      name as String,
      <Object?>[channel, body['data']],
    );
    event.ack(_ok);
  }

  /// The JSON size of [data] in bytes. Data that is not JSON counts as
  /// too big.
  static int _size(Object? data) {
    try {
      return utf8.encode(jsonEncode(data)).length;
    } on JsonUnsupportedObjectError {
      return maxWhisperSize + 1;
    }
  }

  static Future<void> _onDisconnect(SocketEvent event) async {
    Object? presence = event.socket.data[presenceKey];
    if (presence is! Map) return;
    for (Object? channel in List<Object?>.of(presence.keys)) {
      await _presenceLeave(event.socket, channel.toString());
    }
  }

  /// The members of [socket] in its presence channels, by channel. It is
  /// in `socket.data`, so `fetchSockets` on other servers gets it too.
  static Map<String, Object?> _presence(Socket socket) {
    Object? presence = socket.data[presenceKey];
    if (presence is Map<String, Object?>) return presence;
    Map<String, Object?> created = <String, Object?>{};
    socket.data[presenceKey] = created;
    return created;
  }

  /// Joins the room and tells the members. Gives an error text when the
  /// socket did not join: an `unsubscribe` came, or the members could
  /// not be read or told (a server did not answer, Redis is down).
  static Future<String?> _presenceJoin(
    Socket socket,
    String channel,
    Map<String, Object?> member,
  ) async {
    Completer<void> lock = await _lock(channel);
    try {
      if (!socket.connected) return 'Forbidden';
      if (_cancelled(socket, channel)) return 'unsubscribed';
      socket.join(roomOf(channel));
      _presence(socket)[channel] = member;
      List<Map<dynamic, dynamic>> members;
      try {
        members = await _members(socket, channel);
        // the first socket of this user: tell the others
        if (members.where(_sameUser(member['user_id'])).length == 1) {
          await socket.nsp.to(roomOf(channel)).except(socket.id).emitArgs(
            'presence:joining',
            <Object?>[channel, member],
          );
        }
      } catch (error) {
        stderr.writeln('dox_socketio: presence join of $channel: $error');
        socket.leave(roomOf(channel));
        _presence(socket).remove(channel);
        return 'presence is not available';
      }
      Map<Object?, Map<dynamic, dynamic>> unique =
          <Object?, Map<dynamic, dynamic>>{
            for (Map<dynamic, dynamic> one in members) one['user_id']: one,
          };
      socket.emitArgs('presence:subscribed', <Object?>[
        channel,
        unique.values.toList(),
      ]);
      return null;
    } finally {
      _unlock(channel, lock);
    }
  }

  static Future<void> _presenceLeave(Socket socket, String channel) async {
    Completer<void> lock = await _lock(channel);
    try {
      Object? member = _presence(socket).remove(channel);
      if (member is! Map) return;
      bool last = true;
      try {
        List<Map<dynamic, dynamic>> members = await _members(socket, channel);
        last = !members.any(_sameUser(member['user_id']));
      } catch (error) {
        // best effort: the others still learn that this socket left
        stderr.writeln('dox_socketio: presence members of $channel: $error');
      }
      // the last socket of this user left: tell the others
      if (last) {
        try {
          await socket.nsp.to(roomOf(channel)).emitArgs(
            'presence:leaving',
            <Object?>[channel, member],
          );
        } catch (error) {
          stderr.writeln('dox_socketio: presence leave of $channel: $error');
        }
      }
    } finally {
      _unlock(channel, lock);
    }
  }

  /// Waits for the presence work before it on [channel] in this isolate.
  static Future<Completer<void>> _lock(String channel) async {
    Future<void>? before = _locks[channel];
    Completer<void> mine = Completer<void>();
    _locks[channel] = mine.future;
    if (before != null) await before;
    return mine;
  }

  static void _unlock(String channel, Completer<void> mine) {
    mine.complete();
    if (identical(_locks[channel], mine.future)) _locks.remove(channel);
  }

  /// One member per socket in [channel], on all servers.
  static Future<List<Map<dynamic, dynamic>>> _members(
    Socket socket,
    String channel,
  ) async {
    List<RemoteSocket> sockets = await socket.nsp
        .to(roomOf(channel))
        .fetchSockets()
        .timeout(presenceTimeout);
    List<Map<dynamic, dynamic>> members = <Map<dynamic, dynamic>>[];
    for (RemoteSocket one in sockets) {
      Object? presence = one.data[presenceKey];
      Object? member = presence is Map ? presence[channel] : null;
      if (member is Map) members.add(member);
    }
    return members;
  }

  static bool Function(Map<dynamic, dynamic>) _sameUser(Object? userId) =>
      _SameUser(userId).call;

  static const Map<String, Object> _ok = <String, Object>{'ok': true};

  static Map<String, Object> _error(String message) {
    return <String, Object>{'ok': false, 'error': message};
  }
}

class _SameUser {
  _SameUser(this.userId);

  final Object? userId;

  bool call(Map<dynamic, dynamic> member) => member['user_id'] == userId;
}

/// A channel pattern like `orders.{id}` and its rule.
class _Rule {
  _Rule(String pattern, this.callback)
    : _regex = RegExp(
        '^${pattern.splitMapJoin(_param, onMatch: _group, onNonMatch: RegExp.escape)}\$',
      );

  static final RegExp _param = RegExp(r'\{\w+\}');

  final Function callback;
  final RegExp _regex;

  /// The `{name}` values when [name] matches, else null.
  List<String>? match(String name) {
    RegExpMatch? found = _regex.firstMatch(name);
    if (found == null) return null;
    return <String>[
      for (int i = 1; i <= found.groupCount; i++) found.group(i)!,
    ];
  }

  static String _group(Match _) => r'([^.]+)';
}
