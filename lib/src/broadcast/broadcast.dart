import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:crypto/crypto.dart' show Hmac, sha256;
import 'package:dox_core/dox_core.dart';

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

/// An event that is sent to clients on channels, like `ShouldBroadcast`
/// in Laravel. Send it with `Broadcast.event(event)` or
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

/// Sends [event] to its channels. Same as [Broadcast.event].
Future<void> broadcast(ShouldBroadcast event, {String? toOthers}) =>
    Broadcast.event(event, toOthers: toOthers);

/// Channel rules, the socket `subscribe` events, the HTTP auth endpoint
/// and the sending of [ShouldBroadcast] events. Like `Broadcast` in
/// Laravel, on Socket.IO only.
///
/// The wire protocol fits Laravel Echo's Socket.IO connector:
/// - client -> server: `subscribe` with `{channel, auth?}`, and
///   `unsubscribe` with `{channel}`. Both answer an ack with
///   `{ok: true}` or `{ok: false, error}`.
/// - server -> client: `<event name>` with `(channel, payload)`.
/// - presence: `presence:subscribed` with `(channel, members)` to the new
///   socket, `presence:joining` and `presence:leaving` with
///   `(channel, member)` to the others. A member is
///   `{user_id, user_info}`.
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

  /// The key of the presence members in `socket.data`.
  static const String presenceKey = 'broadcast_presence';

  /// The `APP_KEY` label of the token key.
  static const String _keyLabel = 'dox:broadcast';

  static final List<_Rule> _rules = <_Rule>[];
  static Namespace? _nsp;
  static BroadcastFake? _fake;

  /// Adds the rule for private and presence channels that match
  /// [pattern]. The pattern has no `private-` or `presence-` prefix. A
  /// `{name}` part matches any text with no dot. [rule] gets the user,
  /// then one string per `{name}` part, in order.
  ///
  /// For a private channel it gives true to allow. For a presence
  /// channel it gives the member info (a map, with an `id`) to allow.
  /// Null, false or a throw refuses. It can be async.
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
  /// calls it for `/`. Call it once per isolate.
  static void listen(Namespace nsp) {
    _nsp = nsp;
    nsp.onConnection(_onConnection);
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
  /// (`req.auth`) and answers `{auth: token}`. A refused user gets 403.
  /// The client sends the token with `subscribe`: `{channel, auth}`.
  static Future<Map<String, String>> authenticate(DoxRequest req) async {
    String socketId = '${req.input('socket_id') ?? ''}';
    String channel = '${req.input('channel_name') ?? ''}';
    IAuth? auth = req.auth;
    Object? result = socketId.isEmpty || channel.isEmpty
        ? null
        : await authorize(auth?.user<Object>(), channel);
    if (result == null) throw ForbiddenHttpException();
    Map<String, Object?>? member = result is Map
        ? _member(result, auth?.id())
        : null;
    return <String, String>{'auth': authToken(socketId, channel, member)};
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
  static Future<void> event(ShouldBroadcast event, {String? toOthers}) async {
    BroadcastFake? fake = _fake;
    if (fake != null) {
      fake.broadcasted.add(BroadcastedEvent(event, toOthers: toOthers));
      return;
    }
    String name = event.broadcastAs();
    Map<String, Object?> payload = event.broadcastWith();
    Namespace? nsp = _nsp;
    SocketIoEmitter? out = emitter;
    if (nsp == null && out == null) {
      throw StateError(
        'Broadcast has nowhere to send: turn on broadcasting on the '
        'server, or set Broadcast.emitter',
      );
    }
    for (Channel channel in event.broadcastOn()) {
      List<Object?> args = <Object?>[channel.name, payload];
      String room = roomOf(channel.name);
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

  static void _onConnection(Socket socket) {
    socket.on('subscribe', _onSubscribe);
    socket.on('unsubscribe', _onUnsubscribe);
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
    Socket socket = event.socket;
    String room = roomOf(channel);
    if (socket.rooms.contains(room)) {
      event.ack(_ok);
      return;
    }
    Object? grant = await _grant(socket, channel, body['auth']);
    if (grant == null || !socket.connected) {
      event.ack(_error('Forbidden'));
      return;
    }
    socket.join(room);
    if (grant is Map<String, Object?>) {
      await _presenceJoin(socket, channel, grant);
    }
    event.ack(_ok);
  }

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
      if (result is Map) return _member(result, socket.id);
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
    event.socket.leave(roomOf(channel));
    await _presenceLeave(event.socket, channel);
    event.ack(_ok);
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

  static Future<void> _presenceJoin(
    Socket socket,
    String channel,
    Map<String, Object?> member,
  ) async {
    _presence(socket)[channel] = member;
    List<Map<dynamic, dynamic>> members = await _members(socket, channel);
    Object? userId = member['user_id'];
    // the first socket of this user: tell the others
    if (members.where(_sameUser(userId)).length == 1) {
      await socket.nsp.to(roomOf(channel)).except(socket.id).emitArgs(
        'presence:joining',
        <Object?>[channel, member],
      );
    }
    Map<Object?, Map<dynamic, dynamic>> unique =
        <Object?, Map<dynamic, dynamic>>{
          for (Map<dynamic, dynamic> one in members) one['user_id']: one,
        };
    socket.emitArgs('presence:subscribed', <Object?>[
      channel,
      unique.values.toList(),
    ]);
  }

  static Future<void> _presenceLeave(Socket socket, String channel) async {
    Object? member = _presence(socket).remove(channel);
    if (member is! Map) return;
    List<Map<dynamic, dynamic>> members = await _members(socket, channel);
    // the last socket of this user left: tell the others
    if (!members.any(_sameUser(member['user_id']))) {
      await socket.nsp.to(roomOf(channel)).emitArgs(
        'presence:leaving',
        <Object?>[channel, member],
      );
    }
  }

  /// One member per socket in [channel], on all servers.
  static Future<List<Map<dynamic, dynamic>>> _members(
    Socket socket,
    String channel,
  ) async {
    List<RemoteSocket> sockets = await socket.nsp
        .to(roomOf(channel))
        .fetchSockets();
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
