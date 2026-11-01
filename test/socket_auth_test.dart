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

  Object? findUser(Handshake handshake) {
    if (handshake.auth['cookie'] == true) {
      return handshake.cookie('who', decrypt: false);
    }
    String? token = handshake.token;
    if (token == 'boom') throw StateError('database is down');
    return <String, String>{'good': 'alice', 'bob': 'bob'}[token];
  }

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
    await Future<void>.delayed(Duration(milliseconds: 50));
    expect(users, <Object?>['alice']);
  });

  test('two sockets each get their own user', () async {
    expect(await connect(auth: '{"token":"good"}'), startsWith('40{"sid":'));
    expect(await connect(auth: '{"token":"bob"}'), startsWith('40{"sid":'));
    await Future<void>.delayed(Duration(milliseconds: 50));
    expect(users, <Object?>['alice', 'bob']);
  });

  test('a findUser that throws refuses the client', () async {
    expect(
      await connect(auth: '{"token":"boom"}'),
      '44{"message":"Internal error"}',
    );
    expect(users, isEmpty);
  });

  test('a cookie value with a comma is read whole', () async {
    String answer = await connect(
      auth: '{"cookie":true}',
      headers: <String, dynamic>{'Cookie': 'theme=dark; who=carol,dan'},
    );
    expect(answer, startsWith('40{"sid":'));
    await Future<void>.delayed(Duration(milliseconds: 50));
    expect(users, <Object?>['carol,dan']);
  });

  test('custom message', () async {
    await io.close();
    io = SocketIoServer();
    io.use(SocketAuth(findUser, message: 'Log in first').handle);
    http = await io.listen(0);
    expect(await connect(), '44{"message":"Log in first"}');
  });

  group('handshake.cookie', () {
    String key = '4HyiSrq4N5Nfg6bOadIhbFEI8zbUkpxt';
    String oldKey = 'oldKeyoldKeyoldKeyoldKeyoldKey12';

    setUp(() {
      Dox().config = AppConfig(appKey: key, previousKeys: <String>[oldKey]);
    });

    String seal(String value, String name, [String? secret]) =>
        AESEncryptor.encode(
          value,
          secret ?? key,
          associatedData: name,
          purpose: KeyDerivation.cookie,
        );

    Handshake withCookie(String cookie) => Handshake(
      EngineRequest(
        headers: <String, String>{'cookie': cookie},
        query: <String, String>{},
        address: '',
        url: '/',
        secure: false,
      ),
      <String, dynamic>{},
    );

    test('reads plain and encrypted cookies', () {
      Handshake handshake = withCookie(
        'theme=dark; list=a,b; dox_session=${seal('session-1', 'dox_session')}',
      );
      expect(handshake.cookie('dox_session'), 'session-1');
      expect(handshake.cookie('theme', decrypt: false), 'dark');
      expect(handshake.cookie('list', decrypt: false), 'a,b');
      expect(handshake.cookie('theme'), isNull); // not encrypted
      expect(handshake.cookie('missing'), isNull);
    });

    test('a cookie sealed under another name is null', () {
      Handshake handshake = withCookie(
        'other=${seal('session-1', 'dox_session')}',
      );
      expect(handshake.cookie('other'), isNull);
    });

    test('a changed cookie is null', () {
      String sealed = seal('session-1', 'dox_session');
      String changed =
          '${sealed.substring(0, 10)}${sealed[10] == 'A' ? 'B' : 'A'}'
          '${sealed.substring(11)}';
      expect(withCookie('dox_session=$changed').cookie('dox_session'), isNull);
    });

    test('a cookie made with an old APP_KEY still works', () {
      String sealed = seal('session-1', 'dox_session', oldKey);
      expect(
        withCookie('dox_session=$sealed').cookie('dox_session'),
        'session-1',
      );
    });
  });
}
