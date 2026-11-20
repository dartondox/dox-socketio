import 'dart:convert';
import 'dart:typed_data';

import '../constants/engine_constants.dart';
import '../constants/socket_constants.dart';
import 'socket_packet.dart';

/// Decodes Socket.IO v5 packets. Keeps state for binary attachments.
///
/// Use one decoder per connection.
class SocketDecoder {
  /// [maxAttachments] is the most binary parts one packet may have.
  /// The parts are kept in memory until the last one comes.
  ///
  /// [maxDepth] is the deepest nesting of lists and maps in a packet.
  /// Deeper data is refused, so it can not overflow the stack later, when
  /// the data is encoded again.
  ///
  /// [maxAttachmentBytes] is the most bytes of all binary parts of one
  /// packet. Null means no limit.
  SocketDecoder({
    this.maxAttachments = defaultMaxAttachments,
    this.maxDepth = defaultMaxDepth,
    this.maxAttachmentBytes,
  }) {
    if (maxDepth < 1) {
      throw ArgumentError.value(maxDepth, 'maxDepth', 'must be 1 or more');
    }
  }

  final int maxAttachments;
  final int maxDepth;
  final int? maxAttachmentBytes;

  SocketPacket? _pending;
  int _expected = 0;
  int _pendingBytes = 0;
  bool _pendingAtLastCheck = false;
  final List<Uint8List> _buffers = <Uint8List>[];

  /// Drops a binary packet that is still not complete and was also waiting
  /// at the last call, and returns true. Call it once per ping: a client
  /// that stops in the middle of a packet is caught at the second ping, so
  /// a packet has one to two ping intervals to arrive.
  bool dropStale() {
    if (_pending == null) return false;
    if (!_pendingAtLastCheck) {
      _pendingAtLastCheck = true;
      return false;
    }
    _pending = null;
    _buffers.clear();
    return true;
  }

  /// Adds one Engine.IO message ([String] or binary).
  ///
  /// Returns the full packet, or null while it waits for attachments.
  /// Throws [FormatException] on bad input.
  SocketPacket? add(Object data) {
    if (data is String) {
      if (_pending != null) {
        throw const FormatException('got text while waiting for binary');
      }
      return _start(_decodeText(data));
    }
    if (data is List<int>) {
      if (_pending == null) {
        throw const FormatException('got binary without a packet');
      }
      int? maxBytes = maxAttachmentBytes;
      if (maxBytes != null) {
        _pendingBytes += data.length;
        if (_pendingBytes > maxBytes) {
          throw const FormatException('attachments too big');
        }
      }
      _buffers.add(data is Uint8List ? data : Uint8List.fromList(data));
      return _buffers.length == _expected ? _finish() : null;
    }
    throw const FormatException('unknown message type');
  }

  SocketPacket? _start(_DecodedText decoded) {
    if (decoded.attachments == 0) return decoded.packet;
    _pending = decoded.packet;
    _expected = decoded.attachments;
    _pendingBytes = 0;
    _pendingAtLastCheck = false;
    return null;
  }

  SocketPacket _finish() {
    SocketPacket packet = _pending!;
    Object? data = _reconstruct(packet.data, 0);
    _pending = null;
    _buffers.clear();
    return SocketPacket(
      type: packet.type,
      nsp: packet.nsp,
      data: data,
      id: packet.id,
    );
  }

  Object? _reconstruct(Object? data, int depth) {
    if (data is! List && data is! Map) return data;
    if (depth >= maxDepth) throw const FormatException('nested too deep');
    if (data is List) {
      return <Object?>[
        for (Object? item in data) _reconstruct(item, depth + 1),
      ];
    }
    if (data is Map) {
      if (data[placeholderKey] == true) {
        Object? num = data['num'];
        if (num is! int || num < 0 || num >= _buffers.length) {
          throw const FormatException('illegal attachment');
        }
        return _buffers[num];
      }
      return <String, Object?>{
        for (MapEntry<dynamic, dynamic> entry in data.entries)
          entry.key.toString(): _reconstruct(entry.value, depth + 1),
      };
    }
    return data;
  }

  /// Decodes the text part of a packet.
  _DecodedText _decodeText(String text) {
    if (text.isEmpty) throw const FormatException('empty packet');
    int typeIndex = text.codeUnitAt(0) - 48;
    if (typeIndex < 0 || typeIndex >= SocketPacketType.values.length) {
      throw FormatException('unknown packet type', text);
    }
    SocketPacketType type = SocketPacketType.values[typeIndex];
    int i = 1;

    int attachments = 0;
    if (type == SocketPacketType.binaryEvent ||
        type == SocketPacketType.binaryAck) {
      int dash = text.indexOf('-', i);
      int? count = dash < 0 ? null : int.tryParse(text.substring(i, dash));
      if (count == null || count < 0) {
        throw FormatException('illegal attachments', text);
      }
      if (count > maxAttachments) {
        throw FormatException('too many attachments', text);
      }
      attachments = count;
      i = dash + 1;
    }

    String nsp = defaultNamespace;
    if (i < text.length && text[i] == '/') {
      int comma = text.indexOf(',', i);
      int end = comma < 0 ? text.length : comma;
      nsp = text.substring(i, end);
      i = comma < 0 ? text.length : comma + 1;
    }

    int idStart = i;
    while (i < text.length && _isDigit(text.codeUnitAt(i))) {
      i++;
    }
    int? id = i > idStart ? int.parse(text.substring(idStart, i)) : null;

    Object? data;
    bool hasPayload = i < text.length;
    if (hasPayload) {
      _checkDepth(text, i);
      try {
        // Parse from [i] on, with no copy of the text.
        _OneValue value = _OneValue();
        const JsonDecoder().startChunkedConversion(value)
          ..addSlice(text, i, text.length, false)
          ..close();
        data = value.value;
      } on FormatException {
        throw FormatException('invalid payload', text);
      }
    }
    // Also with no payload: only CONNECT and DISCONNECT may have none.
    if (!_isPayloadValid(type, data, hasPayload)) {
      throw FormatException('invalid payload', text);
    }
    return _DecodedText(
      SocketPacket(type: type, nsp: nsp, data: data, id: id),
      attachments,
    );
  }

  bool _isDigit(int code) => code >= 48 && code <= 57;

  /// Throws when the JSON from [start] on nests lists and maps deeper than
  /// [maxDepth]. The JSON parser itself does not limit it, but encoding
  /// such data again (size check, logs, adapters) overflows the stack.
  void _checkDepth(String text, int start) {
    // Deeper JSON needs more than two brackets per level.
    if (text.length - start <= maxDepth * 2) return;
    int depth = 0;
    bool inString = false;
    for (int j = start; j < text.length; j++) {
      int code = text.codeUnitAt(j);
      if (inString) {
        if (code == 0x5C) {
          j++; // Skip the escaped character.
        } else if (code == 0x22) {
          inString = false;
        }
      } else if (code == 0x22) {
        inString = true;
      } else if (code == 0x5B || code == 0x7B) {
        if (++depth > maxDepth) throw FormatException('nested too deep');
      } else if (code == 0x5D || code == 0x7D) {
        depth--;
      }
    }
  }

  /// A JSON `null` is a payload too, so `40null` and `1null` are refused.
  bool _isPayloadValid(SocketPacketType type, Object? data, bool hasPayload) {
    switch (type) {
      case SocketPacketType.connect:
        return !hasPayload || data is Map;
      case SocketPacketType.disconnect:
        return !hasPayload;
      case SocketPacketType.connectError:
        return data is String || data is Map;
      case SocketPacketType.event:
      case SocketPacketType.binaryEvent:
        if (data is! List || data.isEmpty) return false;
        Object? name = data.first;
        return name is num ||
            (name is String && !reservedEvents.contains(name));
      case SocketPacketType.ack:
      case SocketPacketType.binaryAck:
        return data is List;
    }
  }
}

/// A decoded text packet and its number of attachments.
class _DecodedText {
  const _DecodedText(this.packet, this.attachments);

  final SocketPacket packet;
  final int attachments;
}

/// Keeps the one value of a chunked JSON decode.
class _OneValue implements Sink<Object?> {
  Object? value;

  @override
  void add(Object? data) => value = data;

  @override
  void close() {}
}
