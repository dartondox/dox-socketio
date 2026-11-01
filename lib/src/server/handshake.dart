import 'package:dox_core/dox_core.dart';

import '../engine/engine_request.dart';
import '../engine/origin_check.dart';

/// Details of the connection, like `socket.handshake` in Node.js.
class Handshake {
  Handshake(EngineRequest request, this.auth, {this.openOrigins = false})
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
      openOrigins = false,
      time = json['issued'] is num
          ? DateTime.fromMillisecondsSinceEpoch((json['issued'] as num).toInt())
          : DateTime.now();

  /// True when the server has no `allowedOrigins`. Then [session]
  /// warns once.
  final bool openOrigins;

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

  /// The token of the client, or null. It is `auth.token` (the client
  /// sends `io(url, { auth: { token } })`), else the `Authorization:
  /// Bearer` header. Browsers can not set headers on a websocket, so
  /// `auth` is the usual way.
  String? get token {
    Object? token = auth['token'];
    if (token is String && token.isNotEmpty) return token;
    String? header = headers['authorization'];
    if (header == null || !header.startsWith('Bearer ')) return null;
    String bearer = header.substring(7).trim();
    return bearer.isEmpty ? null : bearer;
  }

  /// The cookie [name] from the `Cookie` header, or null. Like
  /// `req.cookie(name)` in Dox, it is decrypted with the APP_KEY, so it
  /// needs a Dox app. A changed or foreign cookie gives null.
  /// Pass `decrypt: false` for a plain cookie.
  /// ```
  /// String? sessionId = socket.handshake.cookie('dox_session');
  /// ```
  String? cookie(String name, {bool decrypt = true}) {
    String? value = _cookies[name];
    if (value == null || !decrypt) return value;
    return DoxCookie.decrypt(name, value);
  }

  /// The Dox session of the client, from its session cookie. Use it to
  /// find the user of the web login (`SessionMiddleware`). It has the
  /// same checks as the middleware: a missing, changed, foreign or
  /// expired cookie gives an empty session. [config] wins over
  /// `AppConfig.session`. Read only: changes are not saved.
  /// With no `ServerOptions.allowedOrigins`, it warns once: any website
  /// could then open a socket with the cookie of a logged in user.
  /// ```
  /// Session session = await handshake.session();
  /// User? user = await Auth(guard: 'web').userFromSession(session);
  /// ```
  Future<Session> session([SessionConfig? config]) {
    if (openOrigins) warnSessionOpenOriginOnce();
    SessionMiddleware middleware = SessionMiddleware(config);
    return middleware.fromCookie(
      cookie(middleware.config.cookie, decrypt: false),
    );
  }

  /// Cookies of the `Cookie` header, read on first use. Same parse as
  /// `DoxRequest`.
  late final Map<String, String> _cookies = _parseCookies(headers['cookie']);

  static Map<String, String> _parseCookies(String? header) {
    Map<String, String> out = <String, String>{};
    if (header == null) return out;
    // Split on `;` only, like `DoxRequest`. A value can hold a comma.
    for (String cookie in header.split(';')) {
      int equals = cookie.indexOf('=');
      if (equals < 0) continue;
      out[cookie.substring(0, equals).trim()] = cookie
          .substring(equals + 1)
          .trim();
    }
    return out;
  }

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
