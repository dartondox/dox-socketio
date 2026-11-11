import 'dart:convert';
import 'dart:io';

import 'package:dox_socketio/dox_socketio.dart';
import 'package:test/test.dart';

/// Checks of `ServerOptions.allowRequest` and `ServerOptions.maxSessions`.
void main() {
  SocketIoServer? io;
  late HttpServer http;

  String base() => 'http://127.0.0.1:${http.port}/socket.io/';
  String wsUrl([String extra = '']) =>
      'ws://127.0.0.1:${http.port}/socket.io/?EIO=4&transport=websocket$extra';

  Future<void> start(ServerOptions options) async {
    SocketIoServer server = SocketIoServer(options: options);
    io = server;
    http = await server.listen(0);
  }

  Future<(int, String)> request(
    String method,
    String query, {
    String? body,
    Map<String, String> headers = const <String, String>{},
  }) async {
    HttpClient client = HttpClient();
    HttpClientRequest req = await client.openUrl(
      method,
      Uri.parse('${base()}?$query'),
    );
    headers.forEach(req.headers.set);
    if (body != null) req.write(body);
    HttpClientResponse res = await req.close();
    String text = await utf8.decodeStream(res);
    client.close(force: true);
    return (res.statusCode, text);
  }

  Future<(int, String)> handshake() =>
      request('GET', 'EIO=4&transport=polling');

  String sidOf(String openBody) =>
      (jsonDecode(openBody.substring(1)) as Map<String, dynamic>)['sid']
          as String;

  tearDown(() async {
    await io?.close();
    io = null;
  });

  group('allowRequest', () {
    test('a hook that says yes lets the client in', () async {
      List<String> seen = <String>[];
      await start(
        ServerOptions(
          allowRequest: (EngineRequest r) {
            seen.add(r.headers['x-key'] ?? '');
            return true;
          },
        ),
      );
      (int, String) res = await request(
        'GET',
        'EIO=4&transport=polling',
        headers: <String, String>{'x-key': 'abc'},
      );
      expect(res.$1, 200);
      expect(res.$2[0], '0');
      WebSocket ws = await WebSocket.connect(wsUrl());
      await ws.close();
      expect(seen, <String>['abc', '']);
    });

    test('a hook that says no gives 403 on polling', () async {
      await start(ServerOptions(allowRequest: (EngineRequest r) => false));
      (int, String) res = await handshake();
      expect(res.$1, 403);
      expect(jsonDecode(res.$2), <String, Object>{
        'code': 4,
        'message': 'Forbidden',
      });
      expect(io!.engine.clientsCount, 0);
    });

    test('a hook that says no refuses a websocket', () async {
      await start(ServerOptions(allowRequest: (EngineRequest r) => false));
      await expectLater(
        WebSocket.connect(wsUrl()),
        throwsA(isA<WebSocketException>()),
      );
      expect(io!.engine.clientsCount, 0);
    });

    test('the hook also runs for the upgrade of a polling session', () async {
      int calls = 0;
      await start(
        ServerOptions(allowRequest: (EngineRequest r) => ++calls == 1),
      );
      String sid = sidOf((await handshake()).$2);
      await expectLater(
        WebSocket.connect(wsUrl('&sid=$sid')),
        throwsA(isA<WebSocketException>()),
      );
      expect(calls, 2);
      // The polling session is still open.
      expect(io!.engine.clientsCount, 1);
    });

    test('an async hook works', () async {
      await start(
        ServerOptions(
          allowRequest: (EngineRequest r) async {
            await Future<void>.delayed(Duration(milliseconds: 10));
            return r.query['token'] == 'ok';
          },
        ),
      );
      expect(
        (await request('GET', 'EIO=4&transport=polling&token=ok')).$1,
        200,
      );
      expect(
        (await request('GET', 'EIO=4&transport=polling&token=no')).$1,
        403,
      );
    });

    test('a hook that throws refuses the request', () async {
      await start(
        ServerOptions(
          allowRequest: (EngineRequest r) => throw StateError('boom'),
        ),
      );
      (int, String) res = await handshake();
      expect(res.$1, 403);
      expect((jsonDecode(res.$2) as Map<String, dynamic>)['code'], 4);
      // The server still answers.
      expect((await handshake()).$1, 403);
    });
  });

  group('maxSessions', () {
    test('a new handshake at the limit gets 503', () async {
      await start(ServerOptions(maxSessions: 1));
      expect((await handshake()).$1, 200);
      (int, String) res = await handshake();
      expect(res.$1, 503);
      expect(jsonDecode(res.$2), <String, Object>{
        'code': 3,
        'message': 'Too many sessions',
      });
      await expectLater(
        WebSocket.connect(wsUrl()),
        throwsA(isA<WebSocketException>()),
      );
      expect(io!.engine.clientsCount, 1);
    });

    test('an open session still works at the limit', () async {
      await start(ServerOptions(maxSessions: 1));
      String sid = sidOf((await handshake()).$2);
      String query = 'EIO=4&transport=polling&sid=$sid';
      expect((await request('POST', query, body: '40')).$2, 'ok');
      (int, String) poll = await request('GET', query);
      expect(poll.$1, 200);
      expect(poll.$2, startsWith('40{"sid":"'));
      // The upgrade of the open session is not a new session.
      WebSocket ws = await WebSocket.connect(wsUrl('&sid=$sid'));
      await ws.close();
    });

    test('a closed session frees its slot', () async {
      await start(ServerOptions(maxSessions: 1));
      String sid = sidOf((await handshake()).$2);
      expect((await handshake()).$1, 503);
      // Client close packet.
      await request('POST', 'EIO=4&transport=polling&sid=$sid', body: '1');
      await Future<void>.delayed(Duration(milliseconds: 50));
      expect(io!.engine.clientsCount, 0);
      expect((await handshake()).$1, 200);
    });
  });
}
