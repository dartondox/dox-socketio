import 'dart:io';

import 'package:dox_http_native/dox_http_native.dart';

import '../constants/engine_constants.dart';
import '../utils/id_generator.dart';
import 'engine_request.dart';
import 'engine_socket.dart';
import 'http_responder.dart';
import 'origin_check.dart';
import 'polling_transport.dart';
import 'server_options.dart';
import 'transport.dart';
import 'websocket_transport.dart';

/// Gets told about new Engine.IO sessions.
abstract class EngineConnectionListener {
  /// A new session is open.
  void onEngineConnection(EngineSocket socket);
}

/// Engine.IO v4 server. It turns HTTP requests into sessions.
class EngineServer implements EngineSessionStore {
  EngineServer({
    required this.options,
    required this.connectionListener,
    IdGenerator? ids,
    this._responder = const HttpResponder(),
  }) : _ids = ids ?? IdGenerator() {
    CorsOptions? cors = options.cors;
    if (cors != null && cors.credentials && cors.origins.contains('*')) {
      // A browser does not allow `*` with credentials, so Dox would have to
      // echo any origin. Then any site could read answers made with the
      // cookies of the user.
      throw ArgumentError(
        'CorsOptions: list the allowed origins when credentials is true. '
        '"*" is not allowed.',
      );
    }
    if (options.allowedOrigins == null) warnOpenOriginOnce();
  }

  final ServerOptions options;
  final EngineConnectionListener connectionListener;
  final IdGenerator _ids;
  final HttpResponder _responder;
  final Map<String, EngineSocket> _sessions = <String, EngineSocket>{};

  /// Number of open sessions.
  int get clientsCount => _sessions.length;

  /// Handles one HTTP request for the Engine.IO path.
  Future<void> handle(HttpRequest request) async {
    HttpResponse response = request.response;
    CorsOptions? cors = options.cors;
    if (cors != null) _applyCors(request, cors);
    if (request.method == 'OPTIONS') {
      if (cors == null) {
        _responder.error(response, EngineError.badRequest);
      } else {
        _responder.send(response, HttpStatus.noContent, '');
      }
      return;
    }

    Map<String, String> query = request.uri.queryParameters;
    if (query['EIO'] != engineProtocolVersion) {
      _responder.error(response, EngineError.unsupportedProtocolVersion);
      return;
    }
    String? transport = query['transport'];
    if (transport == null || !options.transports.contains(transport)) {
      _responder.error(response, EngineError.unknownTransport);
      return;
    }
    bool isWebsocket = transport == transportWebsocket;
    if (isWebsocket && !NativeWebSocket.isUpgradeRequest(request)) {
      _responder.error(response, EngineError.badRequest);
      return;
    }
    if (!_originAllowed(request, cors, isWebsocket)) {
      _responder.error(response, EngineError.forbidden);
      return;
    }

    String? sid = query['sid'];
    if (sid == null) {
      await _handshake(request, isWebsocket);
      return;
    }
    EngineSocket? session = _sessions[sid];
    if (session == null) {
      _responder.error(response, EngineError.unknownSid);
      return;
    }
    if (isWebsocket) {
      await _upgrade(request, session);
      return;
    }
    Transport current = session.transport;
    if (current is PollingTransport) {
      await current.handleRequest(request);
    } else {
      _responder.error(response, EngineError.badRequest);
    }
  }

  Future<void> _handshake(HttpRequest request, bool isWebsocket) async {
    if (request.method != 'GET') {
      _responder.error(request.response, EngineError.badHandshakeMethod);
      return;
    }
    EngineRequest info = EngineRequest.fromHttpRequest(request);
    Transport transport;
    if (isWebsocket) {
      WebSocket socket = await _upgradeSocket(request);
      transport = WebsocketTransport(
        socket,
        maxHttpBufferSize: options.maxHttpBufferSize,
      );
    } else {
      transport = PollingTransport(
        maxHttpBufferSize: options.maxHttpBufferSize,
      );
    }
    EngineSocket session = EngineSocket(
      id: _ids.next(),
      request: info,
      transport: transport,
      options: options,
      store: this,
    );
    _sessions[session.id] = session;
    session.open();
    connectionListener.onEngineConnection(session);
    if (transport is PollingTransport) await transport.handleRequest(request);
  }

  Future<void> _upgrade(HttpRequest request, EngineSocket session) async {
    if (!options.allowUpgrades ||
        session.upgrading ||
        session.transport is! PollingTransport) {
      _responder.error(request.response, EngineError.badRequest);
      return;
    }
    WebSocket socket = await _upgradeSocket(request);
    session.maybeUpgrade(
      WebsocketTransport(socket, maxHttpBufferSize: options.maxHttpBufferSize),
    );
  }

  /// Compression is off. A small compressed message could grow very big
  /// in memory. On the native server, a message over maxHttpBufferSize
  /// bytes is closed with 1009 in Rust, before Dart reads it.
  Future<WebSocket> _upgradeSocket(HttpRequest request) {
    return NativeWebSocket.upgrade(
      request,
      maxMessageSize: options.maxHttpBufferSize,
    );
  }

  /// True when a request with this `Origin` may use the server.
  ///
  /// A browser sends the cookies of the app with a websocket or a polling
  /// request from any site. So without a check any site could talk to the
  /// server as the logged in user (cross-site WebSocket hijacking).
  ///
  /// - no `Origin` header: allowed. This is not a browser.
  /// - [ServerOptions.allowedOrigins] is null: the 3.0.0 rule. Only a
  ///   websocket is checked, and only against [cors] when it is set.
  /// - otherwise: allowed when the origin is the same host as the `Host`
  ///   header, or is in `allowedOrigins`. It works like
  ///   `webSocketOriginAllowed` of dox_core.
  bool _originAllowed(
    HttpRequest request,
    CorsOptions? cors,
    bool isWebsocket,
  ) {
    String? origin = request.headers.value('origin');
    if (origin == null) return true;
    List<String>? allowed = options.allowedOrigins;
    if (allowed == null) {
      return !isWebsocket || cors == null || cors.allows(origin);
    }
    return originInList(origin, allowed) ||
        originIsSameHost(origin, request.headers.value(HttpHeaders.hostHeader));
  }

  void _applyCors(HttpRequest request, CorsOptions cors) {
    String? origin = request.headers.value('origin');
    if (origin == null || !cors.allows(origin)) return;
    HttpHeaders headers = request.response.headers;
    bool anyOrigin = cors.origins.contains('*');
    headers.set('Access-Control-Allow-Origin', anyOrigin ? '*' : origin);
    if (!anyOrigin) headers.add(HttpHeaders.varyHeader, 'Origin');
    if (cors.credentials) {
      headers.set('Access-Control-Allow-Credentials', 'true');
    }
    if (request.method != 'OPTIONS') return;
    headers.set('Access-Control-Allow-Methods', cors.methods.join(','));
    String? asked = request.headers.value('access-control-request-headers');
    String? allowed = cors.allowedHeaders?.join(',') ?? asked;
    if (allowed != null) headers.set('Access-Control-Allow-Headers', allowed);
    Duration? maxAge = cors.maxAge;
    if (maxAge != null) {
      headers.set('Access-Control-Max-Age', maxAge.inSeconds.toString());
    }
  }

  @override
  void removeSession(String id) => _sessions.remove(id);

  /// Closes all sessions.
  void close([String reason = CloseReason.serverShuttingDown]) {
    for (EngineSocket session in List<EngineSocket>.of(_sessions.values)) {
      session.close(reason);
    }
  }
}
