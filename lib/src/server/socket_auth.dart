import 'dart:async';

import 'handshake.dart';
import 'socket.dart';
import 'socket_io_exception.dart';

/// Finds the user of a connection, or null to refuse it.
typedef UserResolver = FutureOr<Object?> Function(Handshake handshake);

/// A middleware that logs a socket in at connect time.
///
/// Socket.IO gets its requests before the Dox router, so route middleware
/// (session, auth) does not run for it. Use this instead. [resolve] gets
/// the handshake and gives the user, or null. With a user, it is kept in
/// `socket.user`. With null, the client gets a `connect_error` with
/// [message], and `connection` does not run.
/// ```
/// class ChatNamespace extends NamespaceHandler {
///   @override
///   List<Middleware> get middleware => <Middleware>[SocketAuth(findUser).handle];
///
///   static Future<User?> findUser(Handshake handshake) async {
///     String? token = handshake.token;
///     if (token == null) return null;
///     return User().where('api_token', token).getFirst();
///   }
/// }
/// ```
/// With dox_auth, check a JWT or the web login session:
/// ```
/// static Future<User?> findUser(Handshake handshake) async {
///   String? token = handshake.token;
///   if (token != null) return Auth(guard: 'api').userFromToken(token);
///   return Auth(guard: 'web').userFromSession(await handshake.session());
/// }
/// ```
class SocketAuth {
  const SocketAuth(this.resolve, {this.message = 'Unauthorized'});

  /// Gives the user of the handshake, or null.
  final UserResolver resolve;

  /// The `connect_error` message for a refused client.
  final String message;

  /// The middleware. Pass it as `SocketAuth(findUser).handle`.
  Future<void> handle(Socket socket) async {
    Object? user = await resolve(socket.handshake);
    if (user == null) throw SocketIoException(message);
    socket.user = user;
  }
}
