import 'dart:async';

import '../parser/socket_packet.dart';
import '../server/namespace.dart';
import '../server/remote_socket.dart';

/// Which sockets a broadcast goes to.
class BroadcastOptions {
  const BroadcastOptions({
    this.rooms = const <String>{},
    this.except = const <String>{},
    this.local = false,
    this.timeout,
  });

  /// Reads options from JSON (same shape as the Node.js redis adapter).
  factory BroadcastOptions.fromJson(Map<String, dynamic> json) {
    Object? flags = json['flags'];
    Object? timeout = flags is Map ? flags['timeout'] : null;
    return BroadcastOptions(
      rooms: _stringSet(json['rooms']),
      except: _stringSet(json['except']),
      local: flags is Map && flags['local'] == true,
      timeout: timeout is num ? Duration(milliseconds: timeout.toInt()) : null,
    );
  }

  /// Send to sockets in any of these rooms. Empty means all sockets.
  final Set<String> rooms;

  /// Skip sockets in any of these rooms.
  final Set<String> except;

  /// Do not send to other servers.
  final bool local;

  /// How long a broadcast with an ack waits for the clients.
  final Duration? timeout;

  BroadcastOptions copyWith({
    Set<String>? rooms,
    Set<String>? except,
    bool? local,
    Duration? timeout,
  }) {
    return BroadcastOptions(
      rooms: rooms ?? this.rooms,
      except: except ?? this.except,
      local: local ?? this.local,
      timeout: timeout ?? this.timeout,
    );
  }

  Map<String, Object> toJson() {
    return <String, Object>{
      'rooms': rooms.toList(),
      'except': except.toList(),
      'flags': <String, Object>{
        if (local) 'local': true,
        if (timeout != null) 'timeout': timeout!.inMilliseconds,
      },
    };
  }

  static Set<String> _stringSet(Object? value) {
    if (value is! List) return <String>{};
    return value.map(_toString).toSet();
  }

  static String _toString(Object? value) => value.toString();
}

/// Keeps rooms and sends broadcasts.
///
/// One adapter serves all namespaces of a server. Replace it to share
/// broadcasts between many servers (see `RedisAdapter`).
abstract class Adapter {
  /// Called once for each new namespace.
  void init(Namespace nsp);

  /// Adds socket [id] to [rooms].
  void addAll(Namespace nsp, String id, Set<String> rooms);

  /// Removes socket [id] from [room].
  void del(Namespace nsp, String id, String room);

  /// Removes socket [id] from all rooms.
  void delAll(Namespace nsp, String id);

  /// Rooms of socket [id].
  Set<String> socketRooms(Namespace nsp, String id);

  /// Ids of the local sockets that match [options].
  Set<String> socketIds(Namespace nsp, BroadcastOptions options);

  /// Sends [packet] to the sockets that match [options].
  Future<void> broadcast(
    Namespace nsp,
    SocketPacket packet,
    BroadcastOptions options,
  );

  /// Sends [packet] to the sockets that match [options] and waits for
  /// their acks.
  ///
  /// [onClientCount] is called once per server, with the number of
  /// clients that got the packet. [onAck] is called with the first ack
  /// argument of each client.
  Future<void> broadcastWithAck(
    Namespace nsp,
    SocketPacket packet,
    BroadcastOptions options,
    void Function(int clientCount) onClientCount,
    void Function(Object? response) onAck,
  );

  /// Number of servers that share this namespace, this one included.
  Future<int> serverCount(Namespace nsp);

  /// Sockets that match [options], on all servers.
  Future<List<RemoteSocket>> fetchSockets(
    Namespace nsp,
    BroadcastOptions options,
  );

  /// Makes the sockets that match [options] join [rooms], on all servers.
  Future<void> addSockets(
    Namespace nsp,
    BroadcastOptions options,
    Set<String> rooms,
  );

  /// Makes the sockets that match [options] leave [rooms], on all servers.
  Future<void> delSockets(
    Namespace nsp,
    BroadcastOptions options,
    Set<String> rooms,
  );

  /// Disconnects the sockets that match [options], on all servers.
  Future<void> disconnectSockets(
    Namespace nsp,
    BroadcastOptions options,
    bool close,
  );

  /// Sends `[event, ...args]` in [args] to the other servers.
  Future<void> serverSideEmit(Namespace nsp, List<Object?> args);

  /// Like [serverSideEmit], and waits for one answer from each other
  /// server.
  Future<List<Object?>> serverSideEmitWithAck(
    Namespace nsp,
    List<Object?> args,
  );

  /// Frees resources.
  Future<void> close();
}

/// Thrown when not all acks came back in time.
///
/// [responses] holds the acks that did come back.
class AckTimeoutException extends TimeoutException {
  AckTimeoutException(super.message, this.responses);

  /// The answers that came back before the timeout.
  final List<Object?> responses;
}
