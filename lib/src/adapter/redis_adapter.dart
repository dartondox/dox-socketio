import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

import 'package:ioredis/ioredis.dart';

import '../server/namespace.dart';
import 'msgpack.dart';
import 'pub_sub_adapter.dart';

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
/// It uses the format of `@socket.io/redis-adapter`, so it works next to
/// Node.js servers and with the Socket.IO emitters of other languages.
/// See [PubSubAdapter] for the channels.
///
/// To know how many servers must answer a request, it asks Redis
/// (`PUBSUB NUMSUB` on the request channel).
class RedisAdapter extends PubSubAdapter {
  RedisAdapter({
    required this.publisher,
    required this.subscriber,
    super.key,
    super.format,
    super.requestsTimeout,
    super.publishOnSpecificResponseChannel,
    super.ids,
  });

  /// Connection used to publish. Do not use it for subscribe.
  final Redis publisher;

  /// Connection used to subscribe. Do not use it for other commands.
  final Redis subscriber;

  @override
  Future<void> subscribe(Namespace nsp) async {
    RedisSubscriber broadcasts = await subscriber.psubscribe(
      _broadcastPattern(nsp),
    );
    broadcasts.onPatternMessageBytes = onRedisMessage;
    RedisSubscriber requests = await subscriber.subscribe(_channels(nsp));
    requests.onMessageBytes = onMessage;
  }

  /// It does not close the Redis connections; they are yours.
  @override
  Future<void> unsubscribe(Namespace nsp) async {
    await subscriber.punsubscribe(_broadcastPattern(nsp));
    await subscriber.unsubscribe(_channels(nsp));
  }

  @override
  Future<void> publish(String channel, Uint8List message) =>
      publisher.publish(channel, message);

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

  /// Handles a message from Redis: a broadcast, a request or a response.
  void onRedisMessage(String? pattern, String channel, Uint8List message) =>
      onMessage(channel, message);

  String _broadcastPattern(Namespace nsp) => '${_escapeGlob(channelOf(nsp))}*';

  List<String> _channels(Namespace nsp) => <String>[
    requestChannelOf(nsp),
    responseChannelOf(nsp),
    specificResponseChannelOf(nsp),
  ];

  /// Escape glob characters, so a namespace name is matched as plain text.
  static String _escapeGlob(String text) =>
      text.replaceAllMapped(RegExp(r'[*?\[\]\\]'), _escapeMatch);

  static String _escapeMatch(Match match) => '\\${match[0]}';
}
