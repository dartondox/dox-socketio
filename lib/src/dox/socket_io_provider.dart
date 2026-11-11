import 'package:dox_core/dox_core.dart';

import '../adapter/isolate_adapter.dart';
import '../broadcast/broadcast.dart';
import '../engine/origin_check.dart';
import '../server/namespace.dart';
import '../server/socket_io_server.dart';
import 'socket_io_config.dart';

/// Mounts Socket.IO on the Dox HTTP port, in every isolate. Extend it in
/// `lib/` so the generated container finds it:
/// ```
/// class AppSocketIoProvider extends SocketIoProvider {
///   AppSocketIoProvider() : super(socketIoConfig);
/// }
/// ```
/// Socket.IO gets the raw requests of its path, before the router.
class SocketIoProvider extends DoxProvider {
  SocketIoProvider(this.config);

  final SocketIoConfig config;

  /// The Socket.IO server of this isolate, after the provider started.
  /// ```
  /// SocketIoProvider.server?.to('room').emit('news', data);
  /// ```
  static SocketIoServer? server;

  @override
  void setup() {
    if (!config.enabled) return;
    // Every isolate runs setup. Print the origin warning from the first one.
    if (Dox().isolateId != 1) {
      originWarningDone = true;
      sessionOriginWarningDone = true;
    }
    int isolates = Dox().isolateCount;
    // Many isolates: share broadcasts between them, like one server.
    SocketAdapterFactory? adapter =
        config.adapter ??
        (isolates > 1 ? const IsolateSocketAdapterFactory() : null);
    SocketIoServer io = SocketIoServer(
      options: config.options,
      adapter: adapter?.create(),
    );
    for (NamespaceHandler handler in config.namespaces) {
      io.register(handler);
    }
    if (config.broadcasting) Broadcast.listen(io.sockets);
    server = io;
    Route.mount(config.options.path, io.handle);
    if (Dox().isolateId == 1 &&
        isolates > 1 &&
        config.options.transports.contains('polling')) {
      Logger.info(
        'Socket.IO runs on $isolates isolates: '
        "clients should use transports: ['websocket'] (long-polling needs "
        'one isolate or sticky sessions).',
      );
    }
  }
}
