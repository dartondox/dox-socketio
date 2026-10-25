import 'dart:async';

import 'broadcast_operator.dart';
import 'handshake.dart';
import 'namespace.dart';

/// A socket from `fetchSockets()`.
///
/// A socket of this server is a [Socket], which implements this class.
/// A socket of another server is a plain [RemoteSocket]. Its [emit],
/// [join], [leave] and [disconnect] go through the adapter (Redis) to the
/// server that holds the socket, like `RemoteSocket` in Node.js.
class RemoteSocket {
  RemoteSocket({
    required this.id,
    required this.nsp,
    required this.handshake,
    required this.rooms,
    required this.data,
  });

  /// Reads a socket from a `fetchSockets` answer of another server.
  factory RemoteSocket.fromJson(Namespace nsp, Map<String, dynamic> json) {
    Object? rooms = json['rooms'];
    Object? data = json['data'];
    Object? handshake = json['handshake'];
    return RemoteSocket(
      id: json['id'].toString(),
      nsp: nsp,
      handshake: Handshake.fromJson(
        handshake is Map<String, dynamic> ? handshake : <String, dynamic>{},
      ),
      rooms: rooms is List ? rooms.map(_toString).toSet() : <String>{},
      data: data is Map<String, dynamic> ? data : <String, Object?>{},
    );
  }

  /// Socket id.
  final String id;

  /// Namespace of this socket.
  final Namespace nsp;

  /// Details of the connection request.
  final Handshake handshake;

  /// Rooms this socket is in. For a remote socket, this is a copy made
  /// when it was fetched.
  final Set<String> rooms;

  /// Your own data on the socket. For a remote socket, this is a copy.
  final Map<String, Object?> data;

  BroadcastOperator get _operator =>
      BroadcastOperator(nsp, rooms: <String>{id});

  /// Sends [event] to this client. Null [data] sends no argument.
  FutureOr<void> emit(String event, [Object? data]) =>
      _operator.emit(event, data);

  /// Sends [event] with many arguments to this client.
  FutureOr<void> emitArgs(String event, List<Object?> args) =>
      _operator.emitArgs(event, args);

  /// Joins [room].
  FutureOr<void> join(String room) => _operator.socketsJoin(room);

  /// Leaves [room].
  FutureOr<void> leave(String room) => _operator.socketsLeave(room);

  /// Disconnects. With [close], the whole connection is closed.
  FutureOr<void> disconnect([bool close = false]) =>
      _operator.disconnectSockets(close);

  static String _toString(Object? value) => value.toString();
}
