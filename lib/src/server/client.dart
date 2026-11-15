import 'dart:async';
import 'dart:convert';

import '../constants/engine_constants.dart';
import '../constants/socket_constants.dart';
import '../engine/engine_socket.dart';
import '../parser/socket_decoder.dart';
import '../parser/socket_encoder.dart';
import '../parser/socket_packet.dart';
import 'namespace.dart';
import 'socket.dart';
import 'socket_io_server.dart';

/// One Engine.IO connection. It can hold sockets of many namespaces.
class Client implements EngineSocketListener {
  Client(this.server, this.conn, {this._encoder = const SocketEncoder()})
    : _decoder = SocketDecoder(
        maxAttachments: server.options.maxAttachments,
        maxAttachmentBytes: server.options.maxAttachmentBytes,
      ) {
    conn.listener = this;
    _connectTimer = Timer(server.options.connectTimeout, _onConnectTimeout);
  }

  final SocketIoServer server;

  /// The Engine.IO session.
  final EngineSocket conn;

  final SocketEncoder _encoder;
  final SocketDecoder _decoder;
  final Map<String, Socket> _sockets = <String, Socket>{};

  /// Namespaces whose CONNECT still runs the middlewares.
  final Set<String> _connecting = <String>{};
  Timer? _connectTimer;

  /// True while the connection is open.
  bool get isOpen => conn.isOpen;

  /// Encodes and sends one packet.
  void sendPacket(SocketPacket packet) => writeEncoded(_encoder.encode(packet));

  /// Sends a packet that is already encoded.
  void writeEncoded(List<Object> parts) {
    if (!conn.isOpen) return;
    for (Object part in parts) {
      conn.send(part);
    }
  }

  /// Adds a socket that passed the middlewares.
  ///
  /// The connect timer stops here, not on the first CONNECT.
  /// So a client that only gets refused is still closed on time.
  void addSocket(Socket socket) {
    _connectTimer?.cancel();
    _connectTimer = null;
    _sockets[socket.nsp.name] = socket;
  }

  /// Forgets a socket that left.
  void remove(Socket socket) {
    if (_sockets[socket.nsp.name] == socket) _sockets.remove(socket.nsp.name);
  }

  /// Disconnects all sockets and closes the connection.
  void disconnect() {
    for (Socket socket in List<Socket>.of(_sockets.values)) {
      socket.disconnect();
    }
    conn.close();
  }

  @override
  void onEngineMessage(Object data) {
    // A binary packet keeps its parts in memory until the last one comes.
    // Official clients send events only after CONNECT is answered, so
    // refuse one before that: it can only fill memory.
    if (_sockets.isEmpty && data is String && _isBinaryHeader(data)) {
      conn.close(CloseReason.parseError);
      return;
    }
    SocketPacket? packet;
    try {
      packet = _decoder.add(data);
    } catch (_) {
      // Bad input from one client closes only that client.
      conn.close(CloseReason.parseError);
      return;
    }
    if (packet != null) _onPacket(packet);
  }

  @override
  void onEnginePing() {
    // A client that stops in the middle of a binary packet would keep its
    // parts in memory as long as it answers pings.
    if (_decoder.dropStale()) conn.close(CloseReason.parseError);
  }

  @override
  void onEngineClose(String reason) {
    _connectTimer?.cancel();
    for (Socket socket in List<Socket>.of(_sockets.values)) {
      socket.onClose(reason);
    }
    _sockets.clear();
  }

  void _onPacket(SocketPacket packet) {
    if (packet.type == SocketPacketType.connect) {
      unawaited(_connect(packet));
      return;
    }
    _sockets[packet.nsp]?.onPacket(packet);
  }

  Future<void> _connect(SocketPacket packet) async {
    // Skip a namespace that is joined or still in its middlewares.
    // Else many CONNECT packets at once make many sockets.
    if (_sockets.containsKey(packet.nsp) || _connecting.contains(packet.nsp)) {
      return;
    }
    Namespace? nsp = server.namespaceFor(packet.nsp);
    if (nsp == null) {
      sendPacket(
        SocketPacket(
          type: SocketPacketType.connectError,
          nsp: packet.nsp,
          data: const <String, Object>{'message': invalidNamespaceMessage},
        ),
      );
      return;
    }
    Object? auth = packet.data;
    int? maxAuthSize = server.options.maxAuthSize;
    if (maxAuthSize != null &&
        auth != null &&
        utf8.encode(jsonEncode(auth)).length > maxAuthSize) {
      sendPacket(
        SocketPacket(
          type: SocketPacketType.connectError,
          nsp: packet.nsp,
          data: const <String, Object>{'message': authTooLargeMessage},
        ),
      );
      return;
    }
    _connecting.add(packet.nsp);
    try {
      await nsp.add(
        this,
        auth is Map ? auth.cast<String, dynamic>() : <String, dynamic>{},
      );
    } finally {
      _connecting.remove(packet.nsp);
    }
  }

  /// True for the text part of a binary event or binary ack (`5` or `6`).
  static bool _isBinaryHeader(String text) {
    if (text.isEmpty) return false;
    int type = text.codeUnitAt(0) - 48;
    return type == SocketPacketType.binaryEvent.index ||
        type == SocketPacketType.binaryAck.index;
  }

  void _onConnectTimeout() {
    if (_sockets.isEmpty) conn.close();
  }
}
