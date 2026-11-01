import 'dart:async';
import 'dart:io';

import 'package:dox_core/dox_core.dart';
import 'package:dox_socketio/dox_socketio.dart';
import 'package:test/test.dart';

/// Checks SocketAuth and the handshake token and cookie helpers.
void main() {
  late SocketIoServer io;
  late HttpServer http;
  List<Object?> users = <Object?>[];

  String wsUrl() =>
      'ws://127.0.0.1:${http.port}/socket.io/?EIO=4&transport=websocket';

  Object? findUser(Handshake handshake) =>
      handshake.token == 'good' ? 'alice' : null;

  setUp(() async {
    users.clear();
    io = SocketIoServer();
    io.use(SocketAuth(findUser).handle);
    io.on('connection', (Socket socket) => users.add(socket.user));
    http = await io.listen(0);
  });

  tearDown(() => io.close());

  /// Sends CONNECT with [auth] and gives the answer packet.
  Future<String> connect({
    String auth = '',
    Map<String, dynamic>? headers,
  }) async {
    WebSocket ws = await WebSocket.connect(wsUrl(), headers: headers);
    StreamIterator<dynamic> frames = StreamIterator<dynamic>(ws);
    await frames.moveNext(); // open packet
    ws.add('40$auth');
    await frames.moveNext();
    String answer = frames.current as String;
    await ws.close();
    return answer;
  }

  test('no token gets connect_error and no connection', () async {
    expect(await connect(), '44{"message":"Unauthorized"}');
    expect(
      await connect(auth: '{"token":"bad"}'),
      '44{"message":"Unauthorized"}',
    );
    expect(users, isEmpty);
  });

  test('auth token logs the socket in', () async {
    expect(await connect(auth: '{"token":"good"}'), startsWith('40{"sid":'));
    await Future<void>.delayed(Duration(milliseconds: 50));
    expect(users, <Object?>['alice']);
  });

  test('Bearer header logs the socket in', () async {
    String answer = await connect(
      headers: <String, dynamic>{'Authorization': 'Bearer good'},
    );
    expect(answer, startsWith('40{"sid":'));
  });

  test('custom message', () async {
    await io.close();
    io = SocketIoServer();
    io.use(SocketAuth(findUser, message: 'Log in first').handle);
    http = await io.listen(0);
    expect(await connect(), '44{"message":"Log in first"}');
  });

  test('handshake.cookie reads plain and encrypted cookies', () {
    String key = '4HyiSrq4N5Nfg6bOadIhbFEI8zbUkpxt';
    Dox().config = AppConfig(appKey: key);
    String sealed = AESEncryptor.encode(
      'session-1',
      key,
      associatedData: 'dox_session',
      purpose: KeyDerivation.cookie,
    );
    Handshake handshake = Handshake(
      EngineRequest(
        headers: <String, String>{'cookie': 'theme=dark; dox_session=$sealed'},
        query: <String, String>{},
        address: '',
        url: '/',
        secure: false,
      ),
      <String, dynamic>{},
    );
    expect(handshake.cookie('dox_session'), 'session-1');
    expect(handshake.cookie('theme', decrypt: false), 'dark');
    expect(handshake.cookie('theme'), isNull); // not encrypted
    expect(handshake.cookie('missing'), isNull);
  });
}
