import 'dart:async';
import 'dart:io';

import '../adapter/adapter.dart';
import '../constants/socket_constants.dart';
import '../parser/socket_packet.dart';
import '../utils/id_generator.dart';
import 'broadcast_operator.dart';
import 'client.dart';
import 'handler_runner.dart';
import 'handshake.dart';
import 'remote_socket.dart';
import 'socket.dart';
import 'socket_io_exception.dart';

/// Handles a new socket.
typedef ConnectionHandler = FutureOr<void> Function(Socket socket);

/// Runs before a socket connects. Throw [SocketIoException] to refuse it.
typedef Middleware = FutureOr<void> Function(Socket socket);

/// Handles an event from another server (see [Namespace.serverSideEmit]).
typedef ServerSideEventHandler = FutureOr<void> Function(ServerSideEvent event);

/// An event that another server sent with `serverSideEmit`.
class ServerSideEvent {
  ServerSideEvent({required this.name, required this.args, this._ack});

  /// Event name.
  final String name;

  /// All arguments the other server sent.
  final List<Object?> args;

  void Function(Object? data)? _ack;

  /// First argument, or null when there is none.
  Object? get data => args.isEmpty ? null : args.first;

  /// True when the other server waits for an answer.
  bool get hasAck => _ack != null;

  /// Answers the other server. Only the first call is sent. Like in
  /// Node.js, the answer is one value and it must be JSON.
  void ack([Object? data]) {
    void Function(Object? data)? ack = _ack;
    _ack = null;
    ack?.call(data);
  }
}

/// A class-based namespace. Register it with `io.register(handler)`.
///
/// ```dart
/// class ChatNamespace extends NamespaceHandler {
///   @override
///   String get path => '/chat';
///
///   @override
///   void onConnection(Socket socket) => socket.on('message', onMessage);
///
///   void onMessage(SocketEvent event) => event.ack('ok');
/// }
/// ```
abstract class NamespaceHandler {
  /// Namespace name.
  String get path => defaultNamespace;

  /// Middlewares of this namespace, in order.
  List<Middleware> get middleware => const <Middleware>[];

  /// Called for each new socket.
  FutureOr<void> onConnection(Socket socket);
}

/// A namespace, like `/` or `/chat`. It holds its sockets and middlewares.
class Namespace {
  Namespace(
    this.name,
    this.adapter, {
    IdGenerator? ids,
    this._runner = const HandlerRunner(),
  }) : _ids = ids ?? IdGenerator() {
    adapter.init(this);
  }

  /// Namespace name.
  final String name;

  /// Adapter that keeps the rooms and does broadcasts.
  final Adapter adapter;

  /// Connected sockets on this server, by id.
  final Map<String, Socket> sockets = <String, Socket>{};

  final IdGenerator _ids;
  final HandlerRunner _runner;
  final List<Middleware> _middleware = <Middleware>[];
  final List<ConnectionHandler> _connectionHandlers = <ConnectionHandler>[];
  final Map<String, List<ServerSideEventHandler>> _serverSideHandlers =
      <String, List<ServerSideEventHandler>>{};
  int _nextAckId = 0;

  /// A new ack id. Sockets and broadcasts of this namespace share the
  /// counter, so a broadcast can use the same id for every socket.
  int nextAckId() => _nextAckId++;

  /// Adds a middleware. Middlewares run in the order they are added.
  void use(Middleware middleware) => _middleware.add(middleware);

  /// Listens for `connection` (or `connect`).
  void on(String event, ConnectionHandler handler) {
    if (event != connectionEvent && event != 'connect') {
      throw ArgumentError.value(event, 'event', 'only connection is allowed');
    }
    _connectionHandlers.add(handler);
  }

  /// Same as `on('connection', handler)`.
  void onConnection(ConnectionHandler handler) => on(connectionEvent, handler);

  /// Sends to sockets in [room].
  BroadcastOperator to(String room) =>
      BroadcastOperator(this, rooms: <String>{room});

  /// Sends to all sockets except the ones in [room].
  BroadcastOperator except(String room) =>
      BroadcastOperator(this, except: <String>{room});

  /// Sends only to sockets of this server.
  BroadcastOperator get local => BroadcastOperator(this, local: true);

  /// Sets how long `emitWithAck` waits for the acks.
  BroadcastOperator timeout(Duration timeout) =>
      BroadcastOperator(this).timeout(timeout);

  /// Sends [event] to all sockets of this namespace.
  Future<void> emit(String event, [Object? data]) =>
      BroadcastOperator(this).emit(event, data);

  /// Sends [event] with many arguments to all sockets of this namespace.
  Future<void> emitArgs(String event, List<Object?> args) =>
      BroadcastOperator(this).emitArgs(event, args);

  /// All sockets of this namespace, on all servers.
  Future<List<RemoteSocket>> fetchSockets() =>
      BroadcastOperator(this).fetchSockets();

  /// Makes all sockets of this namespace, on all servers, join [room].
  Future<void> socketsJoin(String room) =>
      BroadcastOperator(this).socketsJoin(room);

  /// Makes all sockets of this namespace, on all servers, leave [room].
  Future<void> socketsLeave(String room) =>
      BroadcastOperator(this).socketsLeave(room);

  /// Disconnects all sockets of this namespace, on all servers.
  Future<void> disconnectSockets([bool close = false]) =>
      BroadcastOperator(this).disconnectSockets(close);

  /// Sends [event] to the other servers. Clients do not get it. The other
  /// servers get it with [onServerSideEmit] (Node.js: `io.on(event)`).
  /// The arguments must be JSON.
  Future<void> serverSideEmit(
    String event, [
    List<Object?> args = const <Object?>[],
  ]) {
    _checkEventName(event);
    return adapter.serverSideEmit(this, <Object?>[event, ...args]);
  }

  /// Like [serverSideEmit], and waits for one answer from each other
  /// server. Returns the answers.
  ///
  /// When not all servers answer in time (`requestsTimeout` of the Redis
  /// adapter), it throws [AckTimeoutException]. Its `responses` holds the
  /// answers that came back.
  Future<List<Object?>> serverSideEmitWithAck(
    String event, [
    List<Object?> args = const <Object?>[],
  ]) {
    _checkEventName(event);
    return adapter.serverSideEmitWithAck(this, <Object?>[event, ...args]);
  }

  /// Listens for [event] sent by another server with `serverSideEmit`.
  void onServerSideEmit(String event, ServerSideEventHandler handler) {
    _serverSideHandlers.putIfAbsent(event, _newHandlerList).add(handler);
  }

  /// Stops listening. Without [handler], all handlers of [event] go.
  void offServerSideEmit(String event, [ServerSideEventHandler? handler]) {
    if (handler == null) {
      _serverSideHandlers.remove(event);
    } else {
      _serverSideHandlers[event]?.remove(handler);
    }
  }

  /// Called by the adapter with `[event, ...args]` from another server.
  /// [ack] is set when that server waits for an answer.
  void onServerSideMessage(
    List<Object?> message, [
    void Function(Object? data)? ack,
  ]) {
    if (message.isEmpty) return;
    String name = message.first.toString();
    ServerSideEvent event = ServerSideEvent(
      name: name,
      args: message.sublist(1),
      ack: ack,
    );
    for (ServerSideEventHandler handler in List<ServerSideEventHandler>.of(
      _serverSideHandlers[name] ?? const <ServerSideEventHandler>[],
    )) {
      unawaited(_runner.run(handler, event));
    }
  }

  void _checkEventName(String event) {
    if (reservedEvents.contains(event)) {
      throw ArgumentError.value(event, 'event', 'is a reserved event name');
    }
  }

  static List<ServerSideEventHandler> _newHandlerList() =>
      <ServerSideEventHandler>[];

  /// Runs the middlewares and connects a new socket for [client].
  Future<void> add(Client client, Map<String, dynamic> auth) async {
    Socket socket = Socket(
      id: _ids.next(),
      nsp: this,
      client: client,
      handshake: Handshake(client.conn.request, auth),
    );
    try {
      for (Middleware middleware in _middleware) {
        await middleware(socket);
      }
    } catch (error, stack) {
      // Only a SocketIoException goes to the client. Other errors can hold
      // private text, like SQL or host names, so they are only logged.
      if (error is! SocketIoException) {
        stderr.writeln('dox_socketio: middleware error: $error\n$stack');
      }
      client.sendPacket(
        SocketPacket(
          type: SocketPacketType.connectError,
          nsp: name,
          data: error is SocketIoException
              ? error.toJson()
              : const <String, Object>{'message': 'Internal error'},
        ),
      );
      return;
    }
    if (!client.isOpen) return;
    sockets[socket.id] = socket;
    client.addSocket(socket);
    socket.onConnect();
    for (ConnectionHandler handler in List<ConnectionHandler>.of(
      _connectionHandlers,
    )) {
      await _runner.run(handler, socket);
    }
  }

  /// Forgets a socket that left.
  void remove(Socket socket) => sockets.remove(socket.id);
}
