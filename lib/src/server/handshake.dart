import '../engine/engine_request.dart';

/// Details of the connection, like `socket.handshake` in Node.js.
class Handshake {
  Handshake(EngineRequest request, this.auth)
    : headers = request.headers,
      query = request.query,
      address = request.address,
      url = request.url,
      secure = request.secure,
      time = request.time;

  /// Reads the handshake of a socket on another server. Same shape as
  /// `socket.handshake` in Node.js.
  Handshake.fromJson(Map<String, dynamic> json)
    : headers = _strings(json['headers']),
      query = _strings(json['query']),
      auth = json['auth'] is Map<String, dynamic>
          ? json['auth'] as Map<String, dynamic>
          : <String, dynamic>{},
      address = json['address']?.toString() ?? '',
      url = json['url']?.toString() ?? '',
      secure = json['secure'] == true,
      time = json['issued'] is num
          ? DateTime.fromMillisecondsSinceEpoch((json['issued'] as num).toInt())
          : DateTime.now();

  /// Request headers. Names are lower case.
  final Map<String, String> headers;

  /// Query parameters of the first request.
  final Map<String, String> query;

  /// The `auth` payload the client sent with CONNECT.
  final Map<String, dynamic> auth;

  /// Remote IP address.
  final String address;

  /// URL of the first request.
  final String url;

  /// True for HTTPS.
  final bool secure;

  /// When the connection was opened.
  final DateTime time;

  /// [time] in milliseconds since epoch.
  int get issued => time.millisecondsSinceEpoch;

  /// Node.js may send a list for a header or a query key. It is joined
  /// with `, `.
  static Map<String, String> _strings(Object? value) {
    if (value is! Map) return <String, String>{};
    return <String, String>{
      for (MapEntry<dynamic, dynamic> entry in value.entries)
        if (entry.value != null)
          entry.key.toString(): entry.value is List
              ? (entry.value as List<dynamic>).join(', ')
              : entry.value.toString(),
    };
  }
}
