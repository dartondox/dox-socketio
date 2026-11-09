import 'dart:async';
import 'dart:typed_data';

import 'package:dox_core/dox_core.dart';

import '../dox/socket_io_config.dart';
import '../server/namespace.dart';
import 'pub_sub_adapter.dart';
import 'redis_adapter.dart';

/// Shares broadcasts between the isolates of one Dox app, with no Redis.
///
/// Each isolate of a Dox app has its own Socket.IO server, and a client
/// is on one of them. This adapter sends broadcasts, `socketsJoin`,
/// `fetchSockets`, `serverSideEmit` and acks to the other isolates through
/// the [IsolateBus] of dox_core. `SocketIoProvider` uses it by default when
/// the app has more than one isolate.
///
/// It works inside one process only. For many servers, use [RedisAdapter].
class IsolateAdapter extends PubSubAdapter {
  IsolateAdapter({IsolateBus? bus, super.requestsTimeout, super.ids})
    : bus = bus ?? IsolateBus.instance;

  /// The bus of this isolate.
  final IsolateBus bus;

  StreamSubscription<Object?>? _listening;

  /// One listen gets the messages of all namespaces; [onMessage] picks the
  /// namespace by the channel.
  @override
  Future<void> subscribe(Namespace nsp) async {
    _listening ??= bus.messages.listen(_onBusMessage);
  }

  @override
  Future<void> unsubscribe(Namespace nsp) async {
    await _listening?.cancel();
    _listening = null;
  }

  @override
  Future<void> publish(String channel, Uint8List message) async {
    bus.publish(<Object>[channel, message]);
  }

  /// The isolates that listen. All isolates of a Dox app register the
  /// same namespaces, so it is the same for each namespace.
  @override
  Future<int> serverCount(Namespace nsp) async => bus.size;

  void _onBusMessage(Object? message) {
    if (message is! List || message.length != 2) return;
    Object? channel = message[0];
    Object? bytes = message[1];
    if (channel is String && bytes is Uint8List) onMessage(channel, bytes);
  }
}

/// Makes an [IsolateAdapter] in each isolate. `SocketIoProvider` uses it
/// when `SocketIoConfig.adapter` is null and the app has many isolates.
class IsolateSocketAdapterFactory extends SocketAdapterFactory {
  const IsolateSocketAdapterFactory();

  @override
  IsolateAdapter create() => IsolateAdapter();
}
