import 'dart:async';

import '../adapter/adapter.dart';
import '../constants/socket_constants.dart';
import '../parser/socket_packet.dart';
import 'namespace.dart';
import 'remote_socket.dart';
import 'socket.dart';

/// Picks a group of sockets (rooms, except rooms) and sends to them.
///
/// Each method returns a new operator, so you can chain calls:
/// `io.to('a').to('b').except('c').emit('news', data)`.
class BroadcastOperator {
  BroadcastOperator(
    this._nsp, {
    Set<String> rooms = const <String>{},
    Set<String> except = const <String>{},
    bool local = false,
  }) : _options = BroadcastOptions(rooms: rooms, except: except, local: local);

  BroadcastOperator._(this._nsp, this._options);

  final Namespace _nsp;
  final BroadcastOptions _options;

  /// Also sends to sockets in [room].
  BroadcastOperator to(String room) {
    return BroadcastOperator._(
      _nsp,
      _options.copyWith(rooms: <String>{..._options.rooms, room}),
    );
  }

  /// Skips sockets in [room].
  BroadcastOperator except(String room) {
    return BroadcastOperator._(
      _nsp,
      _options.copyWith(except: <String>{..._options.except, room}),
    );
  }

  /// Sends only to sockets of this server, not to other servers.
  BroadcastOperator get local {
    return BroadcastOperator._(_nsp, _options.copyWith(local: true));
  }

  /// Sets how long [emitWithAck] waits for the acks.
  BroadcastOperator timeout(Duration timeout) {
    return BroadcastOperator._(_nsp, _options.copyWith(timeout: timeout));
  }

  /// Sends [event]. Null [data] sends no argument.
  Future<void> emit(String event, [Object? data]) {
    return emitArgs(event, data == null ? const <Object?>[] : <Object?>[data]);
  }

  /// Sends [event] with many arguments.
  Future<void> emitArgs(String event, List<Object?> args) {
    if (reservedEvents.contains(event)) {
      throw ArgumentError.value(event, 'event', 'is a reserved event name');
    }
    SocketPacket packet = SocketPacket(
      type: SocketPacketType.event,
      nsp: _nsp.name,
      data: <Object?>[event, ...args],
    );
    return _nsp.adapter.broadcast(_nsp, packet, _options);
  }

  /// Sends [event] and waits for an ack from every matching client, on
  /// all servers. Returns the first ack argument of each client.
  ///
  /// Set a timeout first: `io.timeout(Duration(seconds: 5))`. When not all
  /// clients answer in time, it throws [AckTimeoutException]. Its
  /// `responses` holds the acks that came back.
  Future<List<Object?>> emitWithAck(String event, [Object? data]) {
    return emitArgsWithAck(
      event,
      data == null ? const <Object?>[] : <Object?>[data],
    );
  }

  /// Like [emitWithAck], with many arguments.
  Future<List<Object?>> emitArgsWithAck(String event, List<Object?> args) {
    if (reservedEvents.contains(event)) {
      throw ArgumentError.value(event, 'event', 'is a reserved event name');
    }
    Duration? timeout = _options.timeout;
    if (timeout == null) {
      throw StateError(
        'a broadcast with an ack needs a timeout: '
        'use timeout(...) first',
      );
    }
    return _BroadcastAck(
      _nsp,
      SocketPacket(
        type: SocketPacketType.event,
        nsp: _nsp.name,
        id: _nsp.nextAckId(),
        data: <Object?>[event, ...args],
      ),
      _options,
      timeout,
    ).run();
  }

  /// Matching sockets on all servers. Sockets of this server are [Socket]s.
  ///
  /// With the Redis adapter it waits for an answer from each server. When
  /// one does not answer in time, it throws a [TimeoutException].
  Future<List<RemoteSocket>> fetchSockets() =>
      _nsp.adapter.fetchSockets(_nsp, _options);

  /// Makes the matching sockets on all servers join [room].
  ///
  /// It completes when this server is done and the request is sent to the
  /// other servers. It does not wait for them.
  Future<void> socketsJoin(String room) =>
      _nsp.adapter.addSockets(_nsp, _options, <String>{room});

  /// Makes the matching sockets on all servers leave [room].
  Future<void> socketsLeave(String room) =>
      _nsp.adapter.delSockets(_nsp, _options, <String>{room});

  /// Disconnects the matching sockets on all servers. With [close], the
  /// whole connection is closed.
  Future<void> disconnectSockets([bool close = false]) =>
      _nsp.adapter.disconnectSockets(_nsp, _options, close);
}

/// Collects the acks of one broadcast, like `emit()` with an ack in the
/// Node.js `BroadcastOperator`.
///
/// It is done when every server sent its client count and every client
/// sent its ack.
class _BroadcastAck {
  _BroadcastAck(this._nsp, this._packet, this._options, this._timeout);

  final Namespace _nsp;
  final SocketPacket _packet;
  final BroadcastOptions _options;
  final Duration _timeout;
  final Completer<List<Object?>> _done = Completer<List<Object?>>();
  final List<Object?> _responses = <Object?>[];
  late final Timer _timer;
  int _expectedServers = -1;
  int _servers = 0;
  int _expectedClients = 0;

  Future<List<Object?>> run() {
    _timer = Timer(_timeout, _onTimeout);
    Adapter adapter = _nsp.adapter;
    unawaited(
      adapter
          .broadcastWithAck(_nsp, _packet, _options, _onClientCount, _onAck)
          .catchError(_fail),
    );
    Future<int> servers = _options.local
        ? Future<int>.value(1)
        : adapter.serverCount(_nsp);
    unawaited(servers.then(_onServerCount).catchError(_fail));
    return _done.future;
  }

  void _onServerCount(int count) {
    _expectedServers = count;
    _check();
  }

  void _onClientCount(int count) {
    _expectedClients += count;
    _servers++;
    _check();
  }

  void _onAck(Object? response) {
    _responses.add(response);
    _check();
  }

  void _check() {
    if (_done.isCompleted) return;
    if (_servers == _expectedServers && _responses.length == _expectedClients) {
      _timer.cancel();
      _done.complete(_responses);
    }
  }

  void _onTimeout() {
    for (Socket socket in _nsp.sockets.values) {
      socket.dropAck(_packet.id!);
    }
    if (_done.isCompleted) return;
    _done.completeError(
      AckTimeoutException(
        'operation has timed out',
        List<Object?>.of(_responses),
      ),
    );
  }

  void _fail(Object error) {
    if (_done.isCompleted) return;
    _timer.cancel();
    _done.completeError(error);
  }
}
