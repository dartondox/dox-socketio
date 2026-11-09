import 'dart:async';
import 'dart:typed_data';

import 'package:ioredis/ioredis.dart';

import '../server/namespace.dart';
import 'pub_sub_adapter.dart';

// RedisFormat and RedisRequestType live in pub_sub_adapter.dart, so it
// does not import this file. Exported here too for code that imports it.
export 'pub_sub_adapter.dart' show RedisFormat, RedisRequestType;

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
