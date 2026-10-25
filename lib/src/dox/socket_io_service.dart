import 'package:dox_core/dox_core.dart';
import 'package:dox_core/utils/logger.dart';

import '../server/namespace.dart';
import '../server/socket_io_server.dart';
import 'socket_io_config.dart';

/// Mounts Socket.IO on the Dox HTTP port, in every isolate. Extend it in
/// `lib/` so the generated container finds it:
/// ```
/// class AppSocketIoService extends SocketIoService {
///   AppSocketIoService() : super(socketIoConfig);
/// }
/// ```
/// Socket.IO gets the raw requests of its path, before the router.
class SocketIoService implements DoxService {
  SocketIoService(this.config);

  final SocketIoConfig config;

  /// The Socket.IO server of this isolate, after the service started.
  /// ```
  /// SocketIoService.server?.to('room').emit('news', data);
  /// ```
  static SocketIoServer? server;

  @override
  void setup() {
    if (!config.enabled) return;
    SocketIoServer io = SocketIoServer(
      options: config.options,
      adapter: config.adapter?.create(),
    );
    for (NamespaceHandler handler in config.namespaces) {
      io.register(handler);
    }
    server = io;
    Route.mount(config.options.path, io.handle);
    AppConfig app = Dox().config;
    if (Dox().isolateId == 1 &&
        app.totalIsolate > 1 &&
        config.options.transports.contains('polling')) {
      Logger.info(
        'Socket.IO runs on ${app.totalIsolate} isolates: '
        "clients should use transports: ['websocket'] (long-polling needs "
        'one isolate or sticky sessions), with a Redis adapter.',
      );
    }
  }
}
