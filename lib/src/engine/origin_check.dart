import 'dart:io';

/// True when [origin] is in [list], or [list] has `*`.
///
/// Case and a trailing `/` do not matter: `HTTPS://App.com/` and
/// `https://app.com` are the same origin. The CORS headers and the
/// `allowedOrigins` check both use this, so they always agree.
bool originInList(String origin, List<String> list) {
  if (list.contains('*')) return true;
  String clean = _cleanOrigin(origin);
  for (String allowed in list) {
    if (_cleanOrigin(allowed) == clean) return true;
  }
  return false;
}

/// True when the host and port of [origin] are the same as [host] (the
/// `Host` header).
bool originIsSameHost(String origin, String? host) {
  if (host == null || host.isEmpty) return false;
  Uri? originUri = Uri.tryParse(origin.trim());
  if (originUri == null || originUri.host.isEmpty) return false;
  Uri? hostUri = Uri.tryParse('${originUri.scheme}://${host.trim()}');
  if (hostUri == null) return false;
  return originUri.host.toLowerCase() == hostUri.host.toLowerCase() &&
      originUri.port == hostUri.port;
}

String _cleanOrigin(String origin) {
  String o = origin.trim().toLowerCase();
  while (o.endsWith('/')) {
    o = o.substring(0, o.length - 1);
  }
  return o;
}

/// True after the open-origin warning was written in this isolate.
///
/// Dart isolates share no memory, so "once per process" is not possible
/// here. [SocketIoProvider] sets this to true on every isolate but the
/// first, so a Dox app with many isolates still prints it once.
bool originWarningDone = false;

/// Writes the open-origin warning once. Same words as the dox core
/// warning, with the Socket.IO option name.
void warnOpenOriginOnce() {
  if (originWarningDone) return;
  originWarningDone = true;
  stderr.writeln(
    'dox_socketio: warning: Socket.IO does not check the Origin header. '
    'Any website can open a socket with the cookie of a logged in user. '
    'To allow only your own sites, set ServerOptions(allowedOrigins: '
    "['https://app.example.com']). [] allows the same host only.",
  );
}
