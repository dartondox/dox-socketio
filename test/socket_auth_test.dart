import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:dox_core/dox_core.dart';
import 'package:dox_socketio/dox_socketio.dart';
import 'package:dox_socketio/src/engine/origin_check.dart';
import 'package:test/test.dart';

/// Checks SocketAuth and the handshake token and cookie helpers.
void main() {
  late SocketIoServer io;
  late HttpServer http;
  List<Object?> users = <Object?>[];

  String wsUrl() =>
      'ws://127.0.0.1:${http.port}/socket.io/?EIO=4&transport=websocket';

  FutureOr<Object?> findUser(Handshake handshake) async {
    if (handshake.auth['session'] == true) {
      return (await handshake.session()).get('auth_web');
    }
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

  test('the web login session logs the socket in', () async {
    String key = '4HyiSrq4N5Nfg6bOadIhbFEI8zbUkpxt';
    Dox().config = AppConfig(appKey: key);
    String sealed(int at) => AESEncryptor.encode(
      jsonEncode(<String, dynamic>{
        'data': <String, dynamic>{'auth_web': 7},
        'flash': <String>[],
        'token': null,
        'at': at,
      }),
      key,
      associatedData: 'dox_session',
      purpose: KeyDerivation.cookie,
    );
    int now = DateTime.now().millisecondsSinceEpoch;
    String good = sealed(now);
    String answer = await connect(
      auth: '{"session":true}',
      headers: <String, dynamic>{'Cookie': 'dox_session=$good'},
    );
    expect(answer, startsWith('40{"sid":'));
    // expired, changed or missing: refused
    String tail = good.endsWith('AAAA') ? 'BBBB' : 'AAAA';
    String changed = '${good.substring(0, good.length - 4)}$tail';
    for (String cookie in <String>[
      'dox_session=${sealed(now - const Duration(hours: 3).inMilliseconds)}',
      'dox_session=$changed',
      'theme=dark',
    ]) {
      expect(
        await connect(
          auth: '{"session":true}',
          headers: <String, dynamic>{'Cookie': cookie},
        ),
        '44{"message":"Unauthorized"}',
      );
    }
    await Future<void>.delayed(Duration(milliseconds: 50));
    expect(users, <Object?>[7]);
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

  group('handshake.session', () {
    String key = '4HyiSrq4N5Nfg6bOadIhbFEI8zbUkpxt';
    const SessionConfig cacheSession = SessionConfig(
      driver: 'cache',
      store: 'memory',
      cookie: 'cache_session',
    );

    setUp(() {
      Dox().config = AppConfig(appKey: key);
      Dox().cache = CacheManager(
        CacheConfig(
          defaultStore: 'memory',
          stores: <String, Map<String, dynamic>>{
            'memory': <String, dynamic>{'driver': 'memory'},
          },
        ),
      );
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

    /// a `cookie` driver session payload, saved [age] ago
    String payload([Duration age = Duration.zero]) =>
        jsonEncode(<String, dynamic>{
          'data': <String, dynamic>{'auth_web': 7},
          'flash': <String>[],
          'token': null,
          'at': DateTime.now().subtract(age).millisecondsSinceEpoch,
        });

    test('reads a cookie driver session', () async {
      Session session = await withCookie(
        'dox_session=${seal(payload(), 'dox_session')}',
      ).session();
      expect(session.get('auth_web'), 7);
    });

    test('reads a cache driver session', () async {
      await Dox().cache.store('memory').put('session:abc', <String, dynamic>{
        'data': <String, dynamic>{'auth_web': 8},
        'flash': <String>[],
        'token': null,
      }, Duration(minutes: 5));
      Session session = await withCookie(
        'cache_session=${seal('abc', 'cache_session')}',
      ).session(cacheSession);
      expect(session.get('auth_web'), 8);
      // an unknown id
      Session unknown = await withCookie(
        'cache_session=${seal('nope', 'cache_session')}',
      ).session(cacheSession);
      expect(unknown.get('auth_web'), isNull);
      // a plain id, not encrypted
      Session plain = await withCookie('cache_session=abc')
          .session(cacheSession);
      expect(plain.get('auth_web'), isNull);
    });

    test('no cookie is an empty session', () async {
      expect((await withCookie('theme=dark').session()).all(), isEmpty);
    });

    test('a changed, foreign or plain cookie is an empty session', () async {
      String sealed = seal(payload(), 'dox_session');
      String changed =
          '${sealed.substring(0, 10)}${sealed[10] == 'A' ? 'B' : 'A'}'
          '${sealed.substring(11)}';
      for (String value in <String>[
        changed,
        seal(payload(), 'other'),
        seal(payload(), 'dox_session', 'anotherKeyanotherKeyanotherKey12'),
        payload(),
      ]) {
        Session session = await withCookie('dox_session=$value').session();
        expect(session.get('auth_web'), isNull);
      }
    });

    test('warns once when allowedOrigins is not set', () async {
      Handshake open = Handshake(
        EngineRequest(
          headers: <String, String>{},
          query: <String, String>{},
          address: '',
          url: '/',
          secure: false,
        ),
        <String, dynamic>{},
        openOrigins: true,
      );
      sessionOriginWarningDone = false;
      await withCookie('theme=dark').session();
      expect(sessionOriginWarningDone, isFalse);
      await open.session();
      expect(sessionOriginWarningDone, isTrue);
    });

    test('the server tells the handshake about allowedOrigins', () async {
      Future<bool> openFor(ServerOptions options) async {
        SocketIoServer server = SocketIoServer(options: options);
        Completer<bool> seen = Completer<bool>();
        sessionOriginWarningDone = false;
        server.use((Socket socket) async {
          await socket.handshake.session();
          seen.complete(sessionOriginWarningDone);
        });
        HttpServer http = await server.listen(0);
        WebSocket ws = await WebSocket.connect(
          'ws://127.0.0.1:${http.port}/socket.io/?EIO=4&transport=websocket',
        );
        ws.add('40');
        bool warned = await seen.future;
        await ws.close();
        await server.close();
        return warned;
      }

      expect(await openFor(const ServerOptions()), isTrue);
      expect(
        await openFor(const ServerOptions(allowedOrigins: <String>[])),
        isFalse,
      );
    });

    test('an expired session is empty', () async {
      Session session = await withCookie(
        'dox_session=${seal(payload(Duration(hours: 3)), 'dox_session')}',
      ).session();
      expect(session.get('auth_web'), isNull);
    });
  });
}
