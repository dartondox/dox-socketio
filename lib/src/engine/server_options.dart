import 'dart:async';

import '../constants/engine_constants.dart';
import 'engine_request.dart';
import 'origin_check.dart';

/// CORS settings for the HTTP long-polling requests.
///
/// When [ServerOptions.allowedOrigins] is null, a websocket from an origin
/// not in [origins] is refused with 403. Polling is not checked.
class CorsOptions {
  const CorsOptions({
    this.origins = const <String>['*'],
    this.credentials = false,
    this.methods = const <String>['GET', 'POST'],
    this.allowedHeaders,
    this.maxAge,
  });

  /// Allowed origins. `*` allows any origin. `*` can not be used with
  /// [credentials].
  final List<String> origins;

  /// Send `Access-Control-Allow-Credentials: true`.
  final bool credentials;

  /// Allowed methods for preflight requests.
  final List<String> methods;

  /// Allowed request headers. When null, the requested headers are allowed.
  final List<String>? allowedHeaders;

  /// How long a browser may cache the preflight answer.
  final Duration? maxAge;

  /// True when [origin] may call the server. Case and a trailing `/` do
  /// not matter.
  bool allows(String origin) => originInList(origin, origins);
}

/// Decides if a handshake or a websocket upgrade may go on. Return true to
/// allow it. See [ServerOptions.allowRequest].
typedef AllowRequest = FutureOr<bool> Function(EngineRequest request);

/// Options of the Engine.IO and Socket.IO server.
class ServerOptions {
  const ServerOptions({
    this.path = defaultPath,
    this.pingInterval = defaultPingInterval,
    this.pingTimeout = defaultPingTimeout,
    this.upgradeTimeout = defaultUpgradeTimeout,
    this.connectTimeout = defaultConnectTimeout,
    this.maxHttpBufferSize = defaultMaxHttpBufferSize,
    this.maxAttachments = defaultMaxAttachments,
    this.transports = const <String>[transportPolling, transportWebsocket],
    this.allowUpgrades = true,
    this.cors,
    this.allowedOrigins,
    this.allowRequest,
    this.maxSessions,
  });

  /// URL path the server listens on. Must end with `/`.
  final String path;

  /// Time between two server pings.
  final Duration pingInterval;

  /// Time to wait for a pong before the session is closed.
  final Duration pingTimeout;

  /// Time a websocket upgrade may take.
  final Duration upgradeTimeout;

  /// Time a client has to join a namespace after the handshake.
  final Duration connectTimeout;

  /// Biggest message (in bytes) a client may send.
  ///
  /// A bigger websocket message closes the connection. dart:io reads the
  /// whole message before this check, so also set a limit in your proxy.
  final int maxHttpBufferSize;

  /// Most binary attachments one Socket.IO packet may have.
  /// A packet with more is a parse error and closes that client.
  final int maxAttachments;

  /// Allowed transports.
  final List<String> transports;

  /// Allow upgrades from polling to websocket.
  final bool allowUpgrades;

  /// CORS settings. When null, no CORS headers are sent.
  final CorsOptions? cors;

  /// Turns on the `Origin` check for websocket and polling requests.
  ///
  /// - `null` (the default): no new check. A websocket is checked against
  ///   [cors] when it is set, like 3.0.0. Polling is not checked. The
  ///   server logs one warning at start, because any website can connect
  ///   with the cookies of the user.
  /// - `[]`: only pages of the same host can connect.
  /// - a list, like `['https://app.example.com']`: the same host and the
  ///   listed sites can connect.
  /// - `['*']`: every site can connect.
  ///
  /// Other origins get 403. A client with no `Origin` header (not a
  /// browser) is always allowed. [cors] still sets the CORS headers.
  final List<String>? allowedOrigins;

  /// Runs for every handshake and every websocket upgrade, before a
  /// session is made or upgraded. It runs after the `Origin` check.
  ///
  /// Return false (or a future of false) to refuse the request. The client
  /// gets 403 with the Engine.IO error `{"code":4,"message":"Forbidden"}`,
  /// like `allowRequest` of the Node `engine.io` server. When the function
  /// throws, the request is refused the same way and the error is written
  /// to stderr.
  ///
  /// Requests of an open session (polling) do not run it again.
  /// Null (the default) allows every request.
  final AllowRequest? allowRequest;

  /// Most open Engine.IO sessions on this server. Null (the default) means
  /// no limit.
  ///
  /// When the limit is reached, a new handshake gets 503 with the
  /// Engine.IO error `{"code":3,"message":"Too many sessions"}`. Requests
  /// of open sessions still work. A slot is free again when a session
  /// closes.
  ///
  /// The count is per isolate. With many isolates, each isolate has its own
  /// server and its own count, so the whole app allows up to
  /// `maxSessions` times the number of isolates.
  final int? maxSessions;
}
