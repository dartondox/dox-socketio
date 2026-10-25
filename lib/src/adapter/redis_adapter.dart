import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:ioredis/ioredis.dart';

import '../constants/socket_constants.dart';
import '../parser/socket_packet.dart';
import '../server/namespace.dart';
import '../server/remote_socket.dart';
import '../server/socket.dart';
import '../utils/id_generator.dart';
import 'adapter.dart';
import 'in_memory_adapter.dart';
import 'msgpack.dart';

/// How messages are written to Redis.
enum RedisFormat {
  /// MessagePack. The default of `@socket.io/redis-adapter` and of the
  /// emitters (Node.js, PHP, Python, Go).
  msgpack,

  /// JSON, for a Node.js adapter that uses a JSON parser.
  json;

  /// Encodes [value] in this format.
  Uint8List encode(Object? value) {
    if (this == RedisFormat.msgpack) return MsgPack.encode(value);
    return utf8.encode(jsonEncode(_encodeBinary(value), toEncodable: _other));
  }

  /// Decodes a message in either format. JSON starts with `[` or `{`.
  /// In msgpack those bytes are small numbers, never a whole message.
  static Object? decode(Uint8List bytes) {
    if (bytes.isNotEmpty && (bytes[0] == 0x5b || bytes[0] == 0x7b)) {
      return _decodeBinary(jsonDecode(utf8.decode(bytes)));
    }
    return MsgPack.decode(bytes);
  }

  /// In JSON, binary data uses the shape of a Node.js Buffer. jsonEncode
  /// sees a [Uint8List] as a plain list, so it is replaced first.
  static Object? _encodeBinary(Object? value) {
    if (value is Uint8List) {
      return <String, Object>{'type': 'Buffer', 'data': value.toList()};
    }
    if (value is List) return value.map(_encodeBinary).toList();
    if (value is Map) return value.map(_encodeEntry);
    return value;
  }

  static MapEntry<String, Object?> _encodeEntry(Object? k, Object? v) =>
      MapEntry<String, Object?>(k.toString(), _encodeBinary(v));

  static Object? _other(Object? value) {
    if (value is Set) return _encodeBinary(value.toList());
    if (value is DateTime) return value.toIso8601String();
    return _encodeBinary((value as dynamic).toJson());
  }

  static Object? _decodeBinary(Object? value) {
    if (value is List) return value.map(_decodeBinary).toList();
    if (value is Map) {
      if (value['type'] == 'Buffer' && value['data'] is List) {
        return Uint8List.fromList((value['data'] as List<dynamic>).cast<int>());
      }
      return value.map(_decodeEntry);
    }
    return value;
  }

  static MapEntry<String, Object?> _decodeEntry(Object? k, Object? v) =>
      MapEntry<String, Object?>(k.toString(), _decodeBinary(v));
}

/// Request types on the request channel. Same numbers as
/// `@socket.io/redis-adapter`.
class RedisRequestType {
  static const int sockets = 0;
  static const int allRooms = 1;
  static const int remoteJoin = 2;
  static const int remoteLeave = 3;
  static const int remoteDisconnect = 4;
  static const int remoteFetch = 5;
  static const int serverSideEmit = 6;
  static const int broadcast = 7;
  static const int broadcastClientCount = 8;
  static const int broadcastAck = 9;
}

/// Shares broadcasts between servers with Redis pub/sub.
///
/// Each server keeps its own rooms in memory. A broadcast is sent to the
/// local sockets and also published to Redis, so other servers send it to
/// their sockets too.
///
/// It uses the format of `@socket.io/redis-adapter`, so it works next to
/// Node.js servers and with the Socket.IO emitters of other languages:
///
/// - Broadcasts: `[uid, packet, {rooms, except, flags}]` on
///   `socket.io#<namespace>#`, or `socket.io#<namespace>#<room>#` when
///   there is exactly one room. It listens to both with PSUBSCRIBE.
/// - Requests (`socketsJoin`, `socketsLeave`, `disconnectSockets`,
///   `fetchSockets`, `serverSideEmit`, broadcasts with an ack) on
///   `socket.io-request#<namespace>#`. Answers go to
///   `socket.io-response#<namespace>#`.
///
/// A request that waits for answers (`fetchSockets`,
/// `serverSideEmitWithAck`) first asks Redis how many servers listen
/// (`PUBSUB NUMSUB` on the request channel). It waits for that many
/// answers, for at most [requestsTimeout].
///
/// It reads msgpack and JSON. It writes [format].
class RedisAdapter extends InMemoryAdapter {
  RedisAdapter({
    required this.publisher,
    required this.subscriber,
    this.key = defaultRedisKey,
    this.format = RedisFormat.msgpack,
    this.requestsTimeout = const Duration(milliseconds: 5000),
    this.publishOnSpecificResponseChannel = false,
    IdGenerator? ids,
  }) : _ids = ids ?? IdGenerator(),
       uid = (ids ?? IdGenerator()).next();

  /// Connection used to publish. Do not use it for subscribe.
  final Redis publisher;

  /// Connection used to subscribe. Do not use it for other commands.
  final Redis subscriber;

  /// Channel prefix.
  final String key;

  /// Format of the messages this server publishes.
  final RedisFormat format;

  /// How long a request waits for the answers of the other servers.
  /// Same default as Node.js (5 seconds).
  final Duration requestsTimeout;

  /// Send answers to `socket.io-response#<namespace>#<uid>#`, the channel
  /// of the server that asked, instead of the shared response channel.
  /// Same as the Node.js option. It always listens on both.
  final bool publishOnSpecificResponseChannel;

  /// Id of this server. Messages with this id are ignored.
  final String uid;

  final IdGenerator _ids;
  final Map<String, Namespace> _namespaces = <String, Namespace>{};
  final List<Future<void>> _subscribing = <Future<void>>[];
  final Map<String, _Request> _requests = <String, _Request>{};
  final Map<String, _AckRequest> _ackRequests = <String, _AckRequest>{};

  /// Completes when all namespace channels are subscribed.
  Future<void> ready() => Future.wait(_subscribing);

  /// Broadcast channel of [nsp].
  String channelOf(Namespace nsp) => '$key#${nsp.name}#';

  /// Request channel of [nsp].
  String requestChannelOf(Namespace nsp) => '$key-request#${nsp.name}#';

  /// Response channel of [nsp].
  String responseChannelOf(Namespace nsp) => '$key-response#${nsp.name}#';

  /// Response channel of [nsp] for this server only.
  String specificResponseChannelOf(Namespace nsp) =>
      '${responseChannelOf(nsp)}$uid#';

  @override
  void init(Namespace nsp) {
    super.init(nsp);
    _namespaces[nsp.name] = nsp;
    _subscribing.add(_subscribe(nsp));
  }

  Future<void> _subscribe(Namespace nsp) async {
    RedisSubscriber broadcasts = await subscriber.psubscribe(
      _broadcastPattern(nsp),
    );
    broadcasts.onPatternMessageBytes = onRedisMessage;
    RedisSubscriber requests = await subscriber.subscribe(_channels(nsp));
    requests.onMessageBytes = _onRequestMessage;
  }

  String _broadcastPattern(Namespace nsp) => '${_escapeGlob(channelOf(nsp))}*';

  List<String> _channels(Namespace nsp) => <String>[
    requestChannelOf(nsp),
    responseChannelOf(nsp),
    specificResponseChannelOf(nsp),
  ];

  void _onRequestMessage(String channel, Uint8List message) =>
      onRedisMessage(null, channel, message);

  /// Escape glob characters, so a namespace name is matched as plain text.
  static String _escapeGlob(String text) =>
      text.replaceAllMapped(RegExp(r'[*?\[\]\\]'), _escapeMatch);

  static String _escapeMatch(Match match) => '\\${match[0]}';

  @override
  Future<void> broadcast(
    Namespace nsp,
    SocketPacket packet,
    BroadcastOptions options,
  ) async {
    if (!options.local) {
      String channel = channelOf(nsp);
      if (options.rooms.length == 1) channel += '${options.rooms.first}#';
      await _publish(
        channel,
        format.encode(<Object>[uid, packet.toJson(), options.toJson()]),
      );
    }
    await super.broadcast(nsp, packet, options);
  }

  @override
  Future<void> broadcastWithAck(
    Namespace nsp,
    SocketPacket packet,
    BroadcastOptions options,
    void Function(int clientCount) onClientCount,
    void Function(Object? response) onAck,
  ) async {
    if (!options.local) {
      String requestId = _ids.next();
      // The other servers wait for acks with their own packet ids.
      Map<String, Object?> raw = packet.toJson()..remove('id');
      _ackRequests[requestId] = _AckRequest(onClientCount, onAck);
      // There is no way to know when all acks came, so forget the request
      // after the timeout, like Node.js.
      Timer(options.timeout ?? requestsTimeout, () {
        _ackRequests.remove(requestId);
      });
      await _publish(
        requestChannelOf(nsp),
        format.encode(<String, Object?>{
          'uid': uid,
          'requestId': requestId,
          'type': RedisRequestType.broadcast,
          'packet': raw,
          'opts': options.toJson(),
        }),
      );
    }
    await super.broadcastWithAck(nsp, packet, options, onClientCount, onAck);
  }

  @override
  Future<int> serverCount(Namespace nsp) async {
    Object? reply = await publisher.sendCommand(<String>[
      'PUBSUB',
      'NUMSUB',
      requestChannelOf(nsp),
    ]);
    if (reply is List && reply.length >= 2) {
      return int.tryParse(reply[1].toString()) ?? 1;
    }
    return 1;
  }

  @override
  Future<List<RemoteSocket>> fetchSockets(
    Namespace nsp,
    BroadcastOptions options,
  ) async {
    List<RemoteSocket> sockets = <RemoteSocket>[...localSockets(nsp, options)];
    if (options.local) return sockets;
    int numSub = await serverCount(nsp);
    if (numSub <= 1) return sockets;
    List<Object?> remote = await _ask(
      nsp,
      RedisRequestType.remoteFetch,
      numSub,
      <String, Object?>{'opts': _opts(options)},
      'timeout reached while waiting for fetchSockets response',
    );
    for (Object? socket in remote) {
      if (socket is Map<String, dynamic>) {
        sockets.add(RemoteSocket.fromJson(nsp, socket));
      }
    }
    return sockets;
  }

  @override
  Future<void> addSockets(
    Namespace nsp,
    BroadcastOptions options,
    Set<String> rooms,
  ) async {
    await super.addSockets(nsp, options, rooms);
    if (options.local) return;
    await _send(nsp, <String, Object?>{
      'uid': uid,
      'type': RedisRequestType.remoteJoin,
      'opts': _opts(options),
      'rooms': rooms.toList(),
    });
  }

  @override
  Future<void> delSockets(
    Namespace nsp,
    BroadcastOptions options,
    Set<String> rooms,
  ) async {
    await super.delSockets(nsp, options, rooms);
    if (options.local) return;
    await _send(nsp, <String, Object?>{
      'uid': uid,
      'type': RedisRequestType.remoteLeave,
      'opts': _opts(options),
      'rooms': rooms.toList(),
    });
  }

  @override
  Future<void> disconnectSockets(
    Namespace nsp,
    BroadcastOptions options,
    bool close,
  ) async {
    await super.disconnectSockets(nsp, options, close);
    if (options.local) return;
    await _send(nsp, <String, Object?>{
      'uid': uid,
      'type': RedisRequestType.remoteDisconnect,
      'opts': _opts(options),
      'close': close,
    });
  }

  @override
  Future<void> serverSideEmit(Namespace nsp, List<Object?> args) {
    return _send(nsp, <String, Object?>{
      'uid': uid,
      'type': RedisRequestType.serverSideEmit,
      'data': args,
    });
  }

  @override
  Future<List<Object?>> serverSideEmitWithAck(
    Namespace nsp,
    List<Object?> args,
  ) async {
    int numSub = await serverCount(nsp) - 1; // not this server
    if (numSub <= 0) return <Object?>[];
    return _ask(nsp, RedisRequestType.serverSideEmit, numSub, <String, Object?>{
      'data': args,
    }, null);
  }

  /// Sends a request and waits for the answers.
  ///
  /// For `fetchSockets`, [numSub] counts this server too, so it waits for
  /// `numSub - 1` answers, like Node.js. On timeout it throws a
  /// [TimeoutException] with [timeoutMessage]. For `serverSideEmit` it
  /// throws an [AckTimeoutException] with the answers so far.
  Future<List<Object?>> _ask(
    Namespace nsp,
    int type,
    int numSub,
    Map<String, Object?> fields,
    String? timeoutMessage,
  ) async {
    String requestId = _ids.next();
    _Request request = _Request(type, numSub);
    request.timer = Timer(requestsTimeout, () {
      if (_requests.remove(requestId) == null) return;
      request.done.completeError(
        timeoutMessage == null
            ? AckTimeoutException(
                'timeout reached: only ${request.items.length} responses '
                'received out of ${request.numSub}',
                List<Object?>.of(request.items),
              )
            : TimeoutException(timeoutMessage, requestsTimeout),
      );
    });
    _requests[requestId] = request;
    try {
      await publisher.publish(
        requestChannelOf(nsp),
        RedisFormat.json.encode(<String, Object?>{
          'uid': uid,
          'requestId': requestId,
          'type': type,
          ...fields,
        }),
      );
    } catch (error) {
      _requests.remove(requestId);
      request.timer?.cancel();
      rethrow;
    }
    return request.done.future;
  }

  /// Requests that need no answer are JSON, like in Node.js.
  Future<void> _send(Namespace nsp, Map<String, Object?> request) =>
      _publish(requestChannelOf(nsp), RedisFormat.json.encode(request));

  static Map<String, Object> _opts(BroadcastOptions options) {
    return <String, Object>{
      'rooms': options.rooms.toList(),
      'except': options.except.toList(),
    };
  }

  Future<void> _publish(String channel, Uint8List message) async {
    try {
      await publisher.publish(channel, message);
    } catch (error) {
      stderr.writeln('dox_socketio: redis publish failed: $error');
    }
  }

  /// Handles a message from Redis: a broadcast, a request or a response.
  void onRedisMessage(String? pattern, String channel, Uint8List message) {
    try {
      for (Namespace nsp in _namespaces.values) {
        if (channel.startsWith(channelOf(nsp))) {
          _onBroadcast(nsp, RedisFormat.decode(message));
          return;
        }
        if (channel == requestChannelOf(nsp)) {
          _onRequest(nsp, RedisFormat.decode(message));
          return;
        }
        if (channel.startsWith(responseChannelOf(nsp))) {
          _onResponse(RedisFormat.decode(message));
          return;
        }
      }
    } catch (error) {
      stderr.writeln('dox_socketio: bad redis message: $error');
    }
  }

  void _onBroadcast(Namespace nsp, Object? decoded) {
    List<dynamic> parts = decoded as List<dynamic>;
    if (parts.length < 3 || parts[0] == uid) return;
    SocketPacket packet = SocketPacket.fromJson(
      parts[1] as Map<String, dynamic>,
    );
    if (packet.nsp != nsp.name) return;
    BroadcastOptions options = BroadcastOptions.fromJson(
      parts[2] as Map<String, dynamic>,
    );
    unawaited(super.broadcast(nsp, packet, options));
  }

  void _onRequest(Namespace nsp, Object? decoded) {
    Map<String, dynamic> request = decoded as Map<String, dynamic>;
    if (request['uid'] == uid) return;
    BroadcastOptions options = _options(request['opts']);
    switch (request['type']) {
      case RedisRequestType.remoteJoin:
        if (request['opts'] == null) {
          _onSocketRequest(nsp, request, (Socket socket) {
            socket.join(request['room'].toString());
          });
        } else {
          unawaited(super.addSockets(nsp, options, _rooms(request)));
        }
      case RedisRequestType.remoteLeave:
        if (request['opts'] == null) {
          _onSocketRequest(nsp, request, (Socket socket) {
            socket.leave(request['room'].toString());
          });
        } else {
          unawaited(super.delSockets(nsp, options, _rooms(request)));
        }
      case RedisRequestType.remoteDisconnect:
        bool close = request['close'] == true;
        if (request['opts'] == null) {
          _onSocketRequest(nsp, request, (Socket socket) {
            socket.disconnect(close);
          });
        } else {
          unawaited(super.disconnectSockets(nsp, options, close));
        }
      case RedisRequestType.sockets:
        BroadcastOptions options = BroadcastOptions.fromJson(<String, dynamic>{
          'rooms': request['rooms'],
        });
        _respond(nsp, request, <String, Object?>{
          'requestId': request['requestId'],
          'sockets': socketIds(nsp, options).toList(),
        });
      case RedisRequestType.allRooms:
        _respond(nsp, request, <String, Object?>{
          'requestId': request['requestId'],
          'rooms': rooms(nsp).toList(),
        });
      case RedisRequestType.remoteFetch:
        _respond(nsp, request, <String, Object?>{
          'requestId': request['requestId'],
          'sockets': localSockets(nsp, options).map(_socketJson).toList(),
        });
      case RedisRequestType.serverSideEmit:
        _onServerSideEmit(nsp, request);
      case RedisRequestType.broadcast:
        _onBroadcastWithAck(nsp, request);
    }
  }

  /// A request for one socket, by `sid`. Node.js answers it with only the
  /// `requestId`.
  void _onSocketRequest(
    Namespace nsp,
    Map<String, dynamic> request,
    void Function(Socket socket) action,
  ) {
    Socket? socket = nsp.sockets[request['sid']];
    if (socket == null) return;
    action(socket);
    _respond(nsp, request, <String, Object?>{
      'requestId': request['requestId'],
    });
  }

  void _onServerSideEmit(Namespace nsp, Map<String, dynamic> request) {
    Object? data = request['data'];
    if (data is! List) return;
    Object? requestId = request['requestId'];
    if (requestId == null) {
      nsp.onServerSideMessage(data.cast<Object?>());
      return;
    }
    // Node.js always sends this answer on the shared response channel.
    nsp.onServerSideMessage(data.cast<Object?>(), (Object? answer) {
      unawaited(
        _publish(
          responseChannelOf(nsp),
          RedisFormat.json.encode(<String, Object?>{
            'type': RedisRequestType.serverSideEmit,
            'requestId': requestId,
            'data': answer,
          }),
        ),
      );
    });
  }

  /// A broadcast with an ack from another server. We send the number of
  /// our clients that got it, then each ack.
  void _onBroadcastWithAck(Namespace nsp, Map<String, dynamic> request) {
    Map<String, dynamic> raw = Map<String, dynamic>.of(
      request['packet'] as Map<String, dynamic>,
    );
    SocketPacket received = SocketPacket.fromJson(raw);
    SocketPacket packet = SocketPacket(
      type: received.type,
      nsp: nsp.name,
      data: received.data,
      id: nsp.nextAckId(),
    );
    BroadcastOptions options = _options(request['opts']);
    Object? requestId = request['requestId'];
    void onClientCount(int count) {
      _respond(nsp, request, <String, Object?>{
        'type': RedisRequestType.broadcastClientCount,
        'requestId': requestId,
        'clientCount': count,
      });
    }

    void onAck(Object? response) {
      // Node.js writes this answer with its parser, so binary data works.
      _respondBytes(
        nsp,
        request,
        format.encode(<String, Object?>{
          'type': RedisRequestType.broadcastAck,
          'requestId': requestId,
          'packet': response,
        }),
      );
    }

    unawaited(
      super.broadcastWithAck(nsp, packet, options, onClientCount, onAck),
    );
    // The sender stops waiting after its timeout. Do the same, so the
    // waiting acks do not stay in memory.
    Duration? timeout = options.timeout;
    if (timeout != null) {
      Timer(timeout, () {
        for (Socket socket in nsp.sockets.values) {
          socket.dropAck(packet.id!);
        }
      });
    }
  }

  void _onResponse(Object? decoded) {
    if (decoded is! Map) return;
    Object? requestId = decoded['requestId'];
    _AckRequest? ackRequest = _ackRequests[requestId];
    if (ackRequest != null) {
      switch (decoded['type']) {
        case RedisRequestType.broadcastClientCount:
          Object? count = decoded['clientCount'];
          ackRequest.onClientCount(count is num ? count.toInt() : 0);
        case RedisRequestType.broadcastAck:
          ackRequest.onAck(decoded['packet']);
      }
      return;
    }
    _Request? request = _requests[requestId];
    if (request == null) return;
    switch (request.type) {
      case RedisRequestType.remoteFetch:
        request.msgCount++;
        Object? sockets = decoded['sockets'];
        if (sockets is! List) return;
        request.items.addAll(sockets);
        if (request.msgCount == request.numSub) _finish(requestId, request);
      case RedisRequestType.serverSideEmit:
        request.items.add(decoded['data']);
        if (request.items.length == request.numSub) {
          _finish(requestId, request);
        }
    }
  }

  void _finish(Object? requestId, _Request request) {
    _requests.remove(requestId);
    request.timer?.cancel();
    request.done.complete(request.items);
  }

  /// The sockets a request is for: `opts.rooms` minus `opts.except`.
  /// The request comes from another server, so it acts on this one only.
  static BroadcastOptions _options(Object? opts) {
    BroadcastOptions options = opts is Map<String, dynamic>
        ? BroadcastOptions.fromJson(opts)
        : const BroadcastOptions();
    return options.copyWith(local: true);
  }

  static Set<String> _rooms(Map<String, dynamic> request) {
    Object? rooms = request['rooms'] ?? request['room'];
    if (rooms is List) return rooms.map(_toString).toSet();
    return rooms == null ? const <String>{} : <String>{rooms.toString()};
  }

  static String _toString(Object? value) => value.toString();

  /// Same shape as a socket in the Node.js `fetchSockets` answer.
  static Map<String, Object?> _socketJson(Socket socket) {
    return <String, Object?>{
      'id': socket.id,
      'handshake': <String, Object?>{
        'headers': socket.handshake.headers,
        'time': socket.handshake.time.toIso8601String(),
        'address': socket.handshake.address,
        'xdomain': false,
        'secure': socket.handshake.secure,
        'issued': socket.handshake.issued,
        'url': socket.handshake.url,
        'query': socket.handshake.query,
        'auth': socket.handshake.auth,
      },
      'rooms': socket.rooms.toList(),
      'data': socket.data,
    };
  }

  /// Answers are JSON, like in the Node.js adapter.
  void _respond(
    Namespace nsp,
    Map<String, dynamic> request,
    Map<String, Object?> response,
  ) {
    _respondBytes(nsp, request, RedisFormat.json.encode(response));
  }

  void _respondBytes(
    Namespace nsp,
    Map<String, dynamic> request,
    Uint8List response,
  ) {
    String channel = publishOnSpecificResponseChannel
        ? '${responseChannelOf(nsp)}${request['uid']}#'
        : responseChannelOf(nsp);
    unawaited(_publish(channel, response));
  }

  /// Stops listening on Redis and fails the requests that still wait.
  /// It does not close the Redis connections; they are yours.
  @override
  Future<void> close() async {
    for (_Request request in _requests.values) {
      request.timer?.cancel();
      request.done.completeError(StateError('adapter closed'));
    }
    _requests.clear();
    _ackRequests.clear();
    try {
      await ready();
      for (Namespace nsp in _namespaces.values) {
        await subscriber.punsubscribe(_broadcastPattern(nsp));
        await subscriber.unsubscribe(_channels(nsp));
      }
    } catch (error) {
      stderr.writeln('dox_socketio: redis unsubscribe failed: $error');
    }
    await super.close();
  }
}

/// A request that waits for answers from other servers.
class _Request {
  _Request(this.type, this.numSub);

  final int type;
  final int numSub;
  final Completer<List<Object?>> done = Completer<List<Object?>>();

  /// Sockets or answers so far.
  final List<Object?> items = <Object?>[];

  /// Answers so far, this server included (for `fetchSockets`).
  int msgCount = 1;
  Timer? timer;
}

/// A broadcast with an ack that waits for other servers.
class _AckRequest {
  _AckRequest(this.onClientCount, this.onAck);

  final void Function(int clientCount) onClientCount;
  final void Function(Object? response) onAck;
}
