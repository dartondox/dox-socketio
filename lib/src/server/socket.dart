import 'dart:async';

import '../adapter/adapter.dart';
import '../constants/engine_constants.dart';
import '../constants/socket_constants.dart';
import '../parser/socket_packet.dart';
import 'broadcast_operator.dart';
import 'client.dart';
import 'handler_runner.dart';
import 'handshake.dart';
import 'namespace.dart';
import 'remote_socket.dart';

/// Handles one event from a client.
typedef EventHandler = FutureOr<void> Function(SocketEvent event);

/// One event from a client, with its arguments and optional ack.
class SocketEvent {
  SocketEvent({
    required this.name,
    required this.args,
    required this.socket,
    this._ackId,
  });

  /// Event name.
  final String name;

  /// All arguments the client sent.
  final List<Object?> args;

  /// The socket that sent the event.
  final Socket socket;

  int? _ackId;

  /// First argument, or null when there is none.
  Object? get data => args.isEmpty ? null : args.first;

  /// True when the client waits for an ack.
  bool get hasAck => _ackId != null;

  /// Answers the client ack with one value. Only the first call is sent.
  void ack([Object? data]) {
    ackArgs(data == null ? const <Object?>[] : <Object?>[data]);
  }

  /// Answers the client ack with many values. Only the first call is sent.
  void ackArgs(List<Object?> args) {
    int? id = _ackId;
    if (id == null) return;
    _ackId = null;
    socket.sendPacket(
      SocketPacket(
        type: SocketPacketType.ack,
        nsp: socket.nsp.name,
        id: id,
        data: args,
      ),
    );
  }
}

/// One client connected to one namespace.
class Socket implements RemoteSocket {
  Socket({
    required this.id,
    required this.nsp,
    required this.client,
    required this.handshake,
    this._runner = const HandlerRunner(),
  });

  /// Socket id. It is also a room this socket is in.
  @override
  final String id;

  /// Namespace of this socket.
  @override
  final Namespace nsp;

  /// The connection this socket uses.
  final Client client;

  /// Details of the connection request.
  @override
  final Handshake handshake;

  /// Free storage for your own data, like the logged-in user.
  /// `fetchSockets()` on other servers gets a copy of it, so keep it JSON.
  @override
  final Map<String, Object?> data = <String, Object?>{};

  final HandlerRunner _runner;
  final Map<String, List<EventHandler>> _handlers =
      <String, List<EventHandler>>{};
  final Map<int, Completer<List<Object?>>> _acks =
      <int, Completer<List<Object?>>>{};
  final Map<int, void Function(List<Object?> args)> _broadcastAcks =
      <int, void Function(List<Object?> args)>{};
  bool _connected = false;

  /// True while the socket is connected.
  bool get connected => _connected;

  /// Name of the transport now in use (`polling` or `websocket`).
  String get transport => client.conn.transport.name;

  /// Rooms this socket is in.
  @override
  Set<String> get rooms => nsp.adapter.socketRooms(nsp, id);

  Adapter get _adapter => nsp.adapter;

  /// Listens for [event]. Use `disconnect` or `disconnecting` to know when
  /// the socket leaves. For them, [SocketEvent.data] is the reason.
  void on(String event, EventHandler handler) {
    _handlers.putIfAbsent(event, _newHandlerList).add(handler);
  }

  /// Stops listening. Without [handler], all handlers of [event] go.
  void off(String event, [EventHandler? handler]) {
    if (handler == null) {
      _handlers.remove(event);
    } else {
      _handlers[event]?.remove(handler);
    }
  }

  /// Sends [event] to this client. Null [data] sends no argument.
  @override
  void emit(String event, [Object? data]) {
    emitArgs(event, data == null ? const <Object?>[] : <Object?>[data]);
  }

  /// Sends [event] with many arguments to this client.
  @override
  void emitArgs(String event, List<Object?> args) {
    _checkEventName(event);
    sendPacket(
      SocketPacket(
        type: SocketPacketType.event,
        nsp: nsp.name,
        data: <Object?>[event, ...args],
      ),
    );
  }

  /// Sends [event] and waits for the client ack. Returns the ack arguments.
  ///
  /// Throws [TimeoutException] when [timeout] passes first.
  Future<List<Object?>> emitWithAck(
    String event,
    Object? data, {
    Duration? timeout,
  }) {
    _checkEventName(event);
    int id = nsp.nextAckId();
    Completer<List<Object?>> completer = Completer<List<Object?>>();
    _acks[id] = completer;
    sendPacket(
      SocketPacket(
        type: SocketPacketType.event,
        nsp: nsp.name,
        id: id,
        data: <Object?>[event, ?data],
      ),
    );
    if (timeout == null) return completer.future;
    return completer.future.timeout(
      timeout,
      onTimeout: _AckTimeout(this, id).call,
    );
  }

  /// Joins [room].
  @override
  void join(String room) => _adapter.addAll(nsp, id, <String>{room});

  /// Leaves [room].
  @override
  void leave(String room) => _adapter.del(nsp, id, room);

  /// Sends to everyone in [room] except this socket.
  BroadcastOperator to(String room) {
    return BroadcastOperator(nsp, rooms: <String>{room}, except: <String>{id});
  }

  /// Sends to everyone except this socket and [room].
  BroadcastOperator except(String room) {
    return BroadcastOperator(nsp, except: <String>{id, room});
  }

  /// Sends to everyone in the namespace except this socket.
  BroadcastOperator get broadcast {
    return BroadcastOperator(nsp, except: <String>{id});
  }

  /// Disconnects from this namespace. With [close], the whole connection
  /// is closed.
  @override
  void disconnect([bool close = false]) {
    if (!_connected) return;
    if (close) {
      client.disconnect();
      return;
    }
    sendPacket(SocketPacket(type: SocketPacketType.disconnect, nsp: nsp.name));
    onClose(CloseReason.serverNamespaceDisconnect);
  }

  /// Sends a packet on the connection.
  void sendPacket(SocketPacket packet) {
    if (_connected || packet.type == SocketPacketType.connect) {
      client.sendPacket(packet);
    }
  }

  /// Called by the namespace when the middlewares passed.
  void onConnect() {
    _connected = true;
    join(id);
    sendPacket(
      SocketPacket(
        type: SocketPacketType.connect,
        nsp: nsp.name,
        data: <String, Object>{'sid': id},
      ),
    );
  }

  /// Called by the client for packets of this namespace.
  void onPacket(SocketPacket packet) {
    if (!_connected) return;
    if (packet.isEvent) {
      _onEvent(packet);
    } else if (packet.isAck) {
      _onAck(packet);
    } else if (packet.type == SocketPacketType.disconnect) {
      onClose(CloseReason.clientNamespaceDisconnect);
    }
  }

  /// Called when the socket leaves, with the [reason].
  void onClose(String reason) {
    if (!_connected) return;
    _emitLocal(disconnectingEvent, reason);
    _connected = false;
    _adapter.delAll(nsp, id);
    nsp.remove(this);
    client.remove(this);
    for (Completer<List<Object?>> completer in _acks.values) {
      completer.completeError(StateError('socket disconnected: $reason'));
    }
    _acks.clear();
    _broadcastAcks.clear();
    _emitLocal(disconnectEvent, reason);
  }

  /// Calls [onAck] with the ack arguments when the client answers packet
  /// [id]. Used for broadcasts with an ack.
  void waitForAck(int id, void Function(List<Object?> args) onAck) {
    _broadcastAcks[id] = onAck;
  }

  /// Removes a waiting ack after a timeout.
  void dropAck(int id) {
    _acks.remove(id);
    _broadcastAcks.remove(id);
  }

  void _onEvent(SocketPacket packet) {
    List<Object?> payload = packet.data! as List<Object?>;
    String name = payload.first.toString();
    SocketEvent event = SocketEvent(
      name: name,
      args: payload.sublist(1),
      socket: this,
      ackId: packet.id,
    );
    for (EventHandler handler in List<EventHandler>.of(
      _handlers[name] ?? const <EventHandler>[],
    )) {
      unawaited(_runner.run(handler, event));
    }
  }

  void _onAck(SocketPacket packet) {
    Object? data = packet.data;
    List<Object?> args = data is List ? data.cast<Object?>() : <Object?>[];
    _acks.remove(packet.id)?.complete(args);
    _broadcastAcks.remove(packet.id)?.call(args);
  }

  void _emitLocal(String name, String reason) {
    SocketEvent event = SocketEvent(
      name: name,
      args: <Object?>[reason],
      socket: this,
    );
    for (EventHandler handler in List<EventHandler>.of(
      _handlers[name] ?? const <EventHandler>[],
    )) {
      unawaited(_runner.run(handler, event));
    }
  }

  void _checkEventName(String event) {
    if (reservedEvents.contains(event)) {
      throw ArgumentError.value(event, 'event', 'is a reserved event name');
    }
  }

  static List<EventHandler> _newHandlerList() => <EventHandler>[];
}

/// Cleans up an ack that timed out and throws [TimeoutException].
class _AckTimeout {
  _AckTimeout(this._socket, this._id);

  final Socket _socket;
  final int _id;

  List<Object?> call() {
    _socket.dropAck(_id);
    throw TimeoutException('ack timed out');
  }
}
