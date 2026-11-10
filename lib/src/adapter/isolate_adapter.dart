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
  /// The [IsolateBus] channel of this adapter. Your own channels never
  /// get its messages.
  static const String busChannel = 'dox_socketio';

  IsolateAdapter({IsolateBus? bus, super.requestsTimeout, super.ids})
    : bus = bus ?? IsolateBus.instance;

  /// The bus of this isolate.
  final IsolateBus bus;

  StreamSubscription<Object?>? _listening;

  /// bus -> the adapters of this isolate that listen and are not closed
  static final Expando<Set<IsolateAdapter>> _live =
      Expando<Set<IsolateAdapter>>();

  /// bus -> the listen of the last closed adapter, if it still answers
  static final Expando<StreamSubscription<Object?>> _closed =
      Expando<StreamSubscription<Object?>>();

  /// One listen gets the messages of all namespaces; [onMessage] picks the
  /// namespace by the channel.
  @override
  Future<void> subscribe(Namespace nsp) async {
    if (_listening != null) return;
    _listening = bus.on(busChannel).listen(_onBusMessage);
    (_live[bus] ??= <IsolateAdapter>{}).add(this);
    // this adapter answers now, so a closed one can stop
    _stopClosed();
  }

  /// The other isolates count this one until it leaves the bus (when the
  /// isolate stops). So the last closed adapter of an isolate keeps
  /// answering them, with no sockets. Else their `fetchSockets` and
  /// `serverSideEmitWithAck` wait for the timeout.
  ///
  /// It stops when another adapter of this isolate listens. So an app
  /// that opens and closes many servers keeps at most one old listen.
  @override
  Future<void> unsubscribe(Namespace nsp) async {
    StreamSubscription<Object?>? listening = _listening;
    if (listening == null || !(_live[bus]?.remove(this) ?? false)) return;
    if (_live[bus]!.isNotEmpty) {
      await listening.cancel();
      return;
    }
    _stopClosed();
    _closed[bus] = listening;
  }

  void _stopClosed() {
    StreamSubscription<Object?>? closed = _closed[bus];
    _closed[bus] = null;
    unawaited(closed?.cancel());
  }

  @override
  Future<void> publish(String channel, Uint8List message) async {
    bus.publish(busChannel, <Object>[channel, message]);
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
