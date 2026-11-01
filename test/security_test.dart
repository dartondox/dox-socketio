import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:dox_socketio/dox_socketio.dart';
import 'package:test/test.dart';

/// Checks for the fixes of the security review.
void main() {
  SocketIoServer? io;
  late HttpServer http;
  List<String> connected = <String>[];

  String wsUrl() =>
      'ws://127.0.0.1:${http.port}/socket.io/?EIO=4&transport=websocket';

  Future<void> start(ServerOptions options, [Middleware? middleware]) async {
    connected.clear();
    SocketIoServer server = SocketIoServer(options: options);
    io = server;
    if (middleware != null) server.use(middleware);
    server.on('connection', (Socket socket) => connected.add(socket.id));
    http = await server.listen(0);
  }

  /// Opens a raw websocket and reads the open packet.
  Future<StreamIterator<dynamic>> open(WebSocket ws) async {
    StreamIterator<dynamic> frames = StreamIterator<dynamic>(ws);
    await frames.moveNext();
    return frames;
  }

  tearDown(() async {
    await io?.close();
    io = null;
  });

  test('deeply nested binary packet is a parse error', () {
    SocketDecoder decoder = SocketDecoder();
    String deep = '${'[' * 100}1${']' * 100}';
    decoder.add('51-["e",$deep]');
    expect(
      () => decoder.add(Uint8List.fromList(<int>[1])),
      throwsFormatException,
    );
  });

  test('maxAttachments in ServerOptions limits binary parts', () async {
    expect(const ServerOptions().maxAttachments, 10);
    await start(ServerOptions(maxAttachments: 1));
    WebSocket ws = await WebSocket.connect(wsUrl());
    StreamIterator<dynamic> frames = await open(ws);
    ws.add('40');
    await frames.moveNext(); // CONNECT answer
    ws.add(
      '452-["e",{"_placeholder":true,"num":0},{"_placeholder":true,"num":1}]',
    );
    // The stream ends when the server closes the socket.
    bool closed = await Future<bool>(() async {
      while (await frames.moveNext()) {}
      return true;
    }).timeout(Duration(seconds: 2), onTimeout: () => false);
    expect(closed, isTrue);
  });

  test('many CONNECT packets at once make one socket', () async {
    await start(
      ServerOptions(),
      (Socket s) => Future<void>.delayed(Duration(milliseconds: 100)),
    );
    WebSocket ws = await WebSocket.connect(wsUrl());
    await open(ws);
    for (int i = 0; i < 5; i++) {
      ws.add('40');
    }
    await Future<void>.delayed(Duration(milliseconds: 300));
    expect(connected, hasLength(1));
    await ws.close();
  });

  test('websocket compression is off', () async {
    await start(ServerOptions());
    HttpClient client = HttpClient();
    HttpClientRequest req = await client.getUrl(
      Uri.parse(wsUrl().replaceFirst('ws:', 'http:')),
    );
    req.headers
      ..set('connection', 'Upgrade')
      ..set('upgrade', 'websocket')
      ..set('sec-websocket-key', 'dGhlIHNhbXBsZSBub25jZQ==')
      ..set('sec-websocket-version', '13')
      ..set('sec-websocket-extensions', 'permessage-deflate');
    HttpClientResponse res = await req.close();
    expect(res.statusCode, HttpStatus.switchingProtocols);
    expect(res.headers.value('sec-websocket-extensions'), isNull);
    (await res.detachSocket()).destroy();
    client.close(force: true);
  });

  test('websocket from an origin not in the list gets 403', () async {
    await start(
      ServerOptions(
        cors: CorsOptions(origins: <String>['http://allowed.test']),
      ),
    );
    await expectLater(
      WebSocket.connect(
        wsUrl(),
        headers: <String, String>{'origin': 'http://evil.test'},
      ),
      throwsA(isA<WebSocketException>()),
    );
    WebSocket ok = await WebSocket.connect(
      wsUrl(),
      headers: <String, String>{'origin': 'http://allowed.test'},
    );
    await ok.close();
  });

  Future<int> poll(Map<String, String> headers) async {
    HttpClient client = HttpClient();
    HttpClientRequest req = await client.getUrl(
      Uri.parse(
        'http://127.0.0.1:${http.port}/socket.io/?EIO=4&transport=polling',
      ),
    );
    headers.forEach(req.headers.set);
    HttpClientResponse res = await req.close();
    await res.drain<void>();
    client.close(force: true);
    return res.statusCode;
  }

  group('no cors and no checkOrigin (open, like 3.0.0)', () {
    test('websocket and polling from another site are allowed', () async {
      await start(ServerOptions());
      WebSocket ws = await WebSocket.connect(
        wsUrl(),
        headers: <String, String>{'origin': 'http://other.test'},
      );
      await ws.close();
      expect(await poll(<String, String>{'origin': 'http://other.test'}), 200);
    });

    test('logs one warning when the server starts', () {
      List<String> lines = <String>[];
      IOOverrides.runZoned(
        () {
          SocketIoServer(options: ServerOptions());
          SocketIoServer(options: ServerOptions(checkOrigin: true));
          SocketIoServer(options: ServerOptions(cors: CorsOptions()));
        },
        stderr: () => _LineSink(lines),
      );
      expect(lines, hasLength(1));
      expect(lines.single, contains('any website can connect'));
      expect(lines.single, contains('checkOrigin: true'));
    });
  });

  group('origin check with checkOrigin: true (same-origin only)', () {
    String self() => 'http://127.0.0.1:${http.port}';

    setUp(() => start(ServerOptions(checkOrigin: true)));

    test('websocket from another site gets 403', () async {
      await expectLater(
        WebSocket.connect(
          wsUrl(),
          headers: <String, String>{'origin': 'http://evil.test'},
        ),
        throwsA(isA<WebSocketException>()),
      );
    });

    test('polling from another site gets 403', () async {
      expect(await poll(<String, String>{'origin': 'http://evil.test'}), 403);
      String otherPort = 'http://127.0.0.1:${http.port + 1}';
      expect(await poll(<String, String>{'origin': otherPort}), 403);
    });

    test('same host is allowed', () async {
      WebSocket ws = await WebSocket.connect(
        wsUrl(),
        headers: <String, String>{'origin': self()},
      );
      await ws.close();
      expect(await poll(<String, String>{'origin': self()}), 200);
    });

    test('no Origin header is allowed (not a browser)', () async {
      WebSocket ws = await WebSocket.connect(wsUrl());
      await ws.close();
      expect(await poll(<String, String>{}), 200);
    });
  });

  test('polling from an origin in the cors list is allowed', () async {
    await start(
      ServerOptions(
        cors: CorsOptions(origins: <String>['http://allowed.test']),
      ),
    );
    HttpClient client = HttpClient();
    Future<int> poll(String origin) async {
      HttpClientRequest req = await client.getUrl(
        Uri.parse(
          'http://127.0.0.1:${http.port}/socket.io/?EIO=4&transport=polling',
        ),
      );
      req.headers.set('origin', origin);
      HttpClientResponse res = await req.close();
      await res.drain<void>();
      return res.statusCode;
    }

    expect(await poll('http://allowed.test'), 200);
    expect(await poll('http://evil.test'), 403);
    client.close(force: true);
  });

  test('"*" origin with credentials throws', () {
    expect(
      () => SocketIoServer(
        options: ServerOptions(cors: CorsOptions(credentials: true)),
      ),
      throwsArgumentError,
    );
  });

  test('middleware error text is not sent to the client', () async {
    await start(
      ServerOptions(),
      (Socket s) => throw StateError('SELECT * FROM secret'),
    );
    WebSocket ws = await WebSocket.connect(wsUrl());
    StreamIterator<dynamic> frames = await open(ws);
    ws.add('40');
    await frames.moveNext();
    expect(frames.current, '44{"message":"Internal error"}');
    await ws.close();
  });

  test('a refused client is closed after connectTimeout', () async {
    await start(
      ServerOptions(connectTimeout: Duration(milliseconds: 200)),
      (Socket s) => throw SocketIoException('no'),
    );
    WebSocket ws = await WebSocket.connect(wsUrl());
    StreamIterator<dynamic> frames = await open(ws);
    ws.add('40');
    // The stream ends when the server closes the socket.
    bool closed = await Future<bool>(() async {
      while (await frames.moveNext()) {}
      return true;
    }).timeout(Duration(seconds: 2), onTimeout: () => false);
    expect(closed, isTrue);
  });
}

/// Collects the lines written to a fake stderr.
class _LineSink implements Stdout {
  _LineSink(this.lines);

  final List<String> lines;

  @override
  void writeln([Object? object = '']) => lines.add('$object');

  @override
  dynamic noSuchMethod(Invocation invocation) => null;
}
