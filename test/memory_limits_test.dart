import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:dox_socketio/dox_socketio.dart';
import 'package:dox_socketio/src/engine/engine_socket.dart'
    show EngineSocketListener;
import 'package:test/test.dart';

/// An adapter whose server count fails, like a Redis adapter with Redis
/// down.
class _BrokenCountAdapter extends InMemoryAdapter {
  @override
  Future<int> serverCount(Namespace nsp) =>
      Future<int>.error(StateError('redis is down'));
}

class _Listener implements EngineConnectionListener {
  _Listener(this.onConnection);

  final void Function(EngineSocket) onConnection;

  @override
  void onEngineConnection(EngineSocket socket) => onConnection(socket);
}

/// Throws for the message `boom`.
class _ThrowingListener implements EngineSocketListener {
  _ThrowingListener(this.messages);

  final List<String> messages;

  @override
  void onEngineMessage(Object data) {
    messages.add('$data');
    if (data == 'boom') throw StateError('boom');
  }

  @override
  void onEnginePing() {}

  @override
  void onEngineClose(String reason) {}
}

/// Checks of the memory limits: binary parts, auth size, polling payloads,
/// the write buffer and waiting acks.
void main() {
  SocketIoServer? io;
  late HttpServer http;
  List<Socket> connected = <Socket>[];
  List<String> reasons = <String>[];

  String wsUrl() =>
      'ws://127.0.0.1:${http.port}/socket.io/?EIO=4&transport=websocket';
  Uri pollUrl([String extra = '']) => Uri.parse(
    'http://127.0.0.1:${http.port}/socket.io/?EIO=4&transport=polling$extra',
  );

  Future<void> start(ServerOptions options, {Adapter? adapter}) async {
    connected.clear();
    reasons.clear();
    SocketIoServer server = SocketIoServer(options: options, adapter: adapter);
    server.on('connection', (Socket socket) {
      connected.add(socket);
      socket.on('disconnect', (SocketEvent e) => reasons.add('${e.data}'));
    });
    io = server;
    http = await server.listen(0);
  }

  tearDown(() async {
    await io?.close();
    io = null;
  });

  /// Opens a websocket and reads the open packet. Answers each ping.
  Future<(WebSocket, StreamIterator<dynamic>)> openWs() async {
    WebSocket ws = await WebSocket.connect(wsUrl());
    StreamController<dynamic> frames = StreamController<dynamic>();
    ws.listen((dynamic frame) {
      if (frame == '2') {
        ws.add('3');
      } else {
        frames.add(frame);
      }
    }, onDone: frames.close);
    StreamIterator<dynamic> it = StreamIterator<dynamic>(frames.stream);
    await it.moveNext(); // open packet
    return (ws, it);
  }

  Future<bool> closedWithin(StreamIterator<dynamic> frames, Duration d) {
    return Future<bool>(() async {
      while (await frames.moveNext()) {}
      return true;
    }).timeout(d, onTimeout: () => false);
  }

  Future<(int, String)> send(String method, Uri url, [String? body]) async {
    HttpClient client = HttpClient();
    HttpClientRequest req = await client.openUrl(method, url);
    if (body != null) req.write(body);
    HttpClientResponse res = await req.close();
    String text = await utf8.decodeStream(res);
    client.close(force: true);
    return (res.statusCode, text);
  }

  Future<String> pollingSession() async {
    (int, String) res = await send('GET', pollUrl());
    return (jsonDecode(res.$2.substring(1)) as Map<String, dynamic>)['sid']
        as String;
  }

  group('binary packets', () {
    test('a binary packet before CONNECT closes the client', () async {
      await start(ServerOptions());
      var (WebSocket ws, StreamIterator<dynamic> frames) = await openWs();
      ws.add('451-["up",{"_placeholder":true,"num":0}]');
      expect(await closedWithin(frames, Duration(seconds: 2)), isTrue);
      expect(connected, isEmpty);
    });

    test('a binary packet after CONNECT still works', () async {
      await start(ServerOptions());
      List<Object?> got = <Object?>[];
      io!.on(
        'connection',
        (Socket s) => s.on('up', (SocketEvent e) {
          got.add(e.data);
        }),
      );
      var (WebSocket ws, StreamIterator<dynamic> frames) = await openWs();
      ws.add('40');
      await frames.moveNext();
      ws.add('451-["up",{"_placeholder":true,"num":0}]');
      ws.add(Uint8List.fromList(<int>[1, 2, 3]));
      await Future<void>.delayed(Duration(milliseconds: 100));
      expect(got, hasLength(1));
      expect(got.single, <int>[1, 2, 3]);
      await ws.close();
    });

    test('an unfinished binary packet is dropped at the next pings', () async {
      await start(
        ServerOptions(
          pingInterval: Duration(milliseconds: 100),
          pingTimeout: Duration(seconds: 5),
        ),
      );
      var (WebSocket ws, StreamIterator<dynamic> frames) = await openWs();
      ws.add('40');
      await frames.moveNext();
      ws.add(
        '452-["up",{"_placeholder":true,"num":0},'
        '{"_placeholder":true,"num":1}]',
      );
      ws.add(Uint8List(10));
      // The client answers pings, but never sends the second part.
      expect(await closedWithin(frames, Duration(seconds: 2)), isTrue);
      await Future<void>.delayed(Duration(milliseconds: 50));
      expect(reasons, <String>[CloseReason.parseError]);
    });

    test('the stale counter resets when a new binary packet starts', () async {
      await start(
        ServerOptions(
          pingInterval: Duration(milliseconds: 200),
          pingTimeout: Duration(seconds: 5),
        ),
      );
      var (WebSocket ws, StreamIterator<dynamic> frames) = await openWs();
      ws.add('40');
      await frames.moveNext();
      const String header =
          '452-["up",{"_placeholder":true,"num":0},'
          '{"_placeholder":true,"num":1}]';
      ws.add(header);
      ws.add(Uint8List(10));
      // One ping passes while the packet is not complete.
      await Future<void>.delayed(Duration(milliseconds: 250));
      ws.add(Uint8List(10)); // the packet is complete now
      // A new packet starts. One ping passes: this is its first, not its
      // second.
      ws.add(header);
      ws.add(Uint8List(10));
      Stopwatch watch = Stopwatch()..start();
      // Left unfinished, it is dropped at its second ping, about 400 ms
      // later. Without the reset it would be dropped at the first one.
      expect(await closedWithin(frames, Duration(seconds: 2)), isTrue);
      expect(watch.elapsed, greaterThan(Duration(milliseconds: 250)));
    });

    test('a binary event right after CONNECT to a namespace works', () async {
      await start(ServerOptions());
      List<Object?> got = <Object?>[];
      io!
          .of('/chat')
          .on(
            'connection',
            (Socket s) => s.on('up', (SocketEvent e) {
              got.add(e.data);
            }),
          );
      var (WebSocket ws, StreamIterator<dynamic> frames) = await openWs();
      ws.add('40/chat,');
      await frames.moveNext();
      ws.add('451-/chat,["up",{"_placeholder":true,"num":0}]');
      ws.add(Uint8List.fromList(<int>[4, 5]));
      await Future<void>.delayed(Duration(milliseconds: 100));
      expect(got, hasLength(1));
      expect(got.single, <int>[4, 5]);
      await ws.close();
    });

    test('maxAttachmentBytes closes the client', () async {
      expect(const ServerOptions().maxAttachmentBytes, isNull);
      await start(ServerOptions(maxAttachmentBytes: 150));
      var (WebSocket ws, StreamIterator<dynamic> frames) = await openWs();
      ws.add('40');
      await frames.moveNext();
      ws.add(
        '453-["up",{"_placeholder":true,"num":0},'
        '{"_placeholder":true,"num":1},{"_placeholder":true,"num":2}]',
      );
      ws.add(Uint8List(100));
      ws.add(Uint8List(100));
      expect(await closedWithin(frames, Duration(seconds: 2)), isTrue);
    });

    test('SocketDecoder counts all parts of one packet', () {
      SocketDecoder decoder = SocketDecoder(maxAttachmentBytes: 150);
      decoder.add(
        '52-["up",{"_placeholder":true,"num":0},'
        '{"_placeholder":true,"num":1}]',
      );
      decoder.add(Uint8List(100));
      expect(() => decoder.add(Uint8List(51)), throwsFormatException);
    });
  });

  group('maxAuthSize', () {
    test('a big auth payload gets connect_error', () async {
      expect(const ServerOptions().maxAuthSize, isNull);
      await start(ServerOptions(maxAuthSize: 20));
      var (WebSocket ws, StreamIterator<dynamic> frames) = await openWs();
      ws.add('40{"token":"${'a' * 20}"}');
      await frames.moveNext();
      expect(frames.current, '44{"message":"auth payload too large"}');
      ws.add('40{"token":"a"}');
      await frames.moveNext();
      expect(frames.current, startsWith('40{"sid":'));
      await ws.close();
    });

    test('no limit by default', () async {
      await start(ServerOptions());
      var (WebSocket ws, StreamIterator<dynamic> frames) = await openWs();
      ws.add('40{"token":"${'a' * 70000}"}');
      await frames.moveNext();
      expect(frames.current, startsWith('40{"sid":'));
      await ws.close();
    });
  });

  group('polling payload', () {
    test('a payload of separators is refused at once', () async {
      await start(ServerOptions());
      String sid = await pollingSession();
      Stopwatch watch = Stopwatch()..start();
      (int, String) res = await send(
        'POST',
        pollUrl('&sid=$sid'),
        '\x1e' * 500000,
      );
      expect(res.$1, 400);
      expect(watch.elapsed, lessThan(Duration(seconds: 2)));
      // The session is closed.
      expect((await send('GET', pollUrl('&sid=$sid'))).$1, 400);
    });

    test('many packets in one payload still work', () async {
      await start(ServerOptions());
      List<Object?> got = <Object?>[];
      io!.on(
        'connection',
        (Socket s) => s.on('e', (SocketEvent e) {
          got.add(e.data);
        }),
      );
      String sid = await pollingSession();
      await send('POST', pollUrl('&sid=$sid'), '40');
      await send('GET', pollUrl('&sid=$sid')); // CONNECT answer
      String body = <String>[for (int i = 0; i < 1000; i++) '42["e",$i]']
          .join('\x1e');
      expect((await send('POST', pollUrl('&sid=$sid'), body)).$1, 200);
      await Future<void>.delayed(Duration(milliseconds: 100));
      expect(got, hasLength(1000));
    });
  });

  group('packet errors', () {
    test('a failing packet does not drop the rest of the payload', () async {
      List<String> messages = <String>[];
      EngineServer engine = EngineServer(
        options: ServerOptions(),
        connectionListener: _Listener((EngineSocket socket) {
          socket.listener = _ThrowingListener(messages);
        }),
      );
      HttpServer server = await HttpServer.bind('127.0.0.1', 0);
      server.listen(engine.handle);
      Uri url(String extra) => Uri.parse(
        'http://127.0.0.1:${server.port}/socket.io/?EIO=4&transport=polling'
        '$extra',
      );
      (int, String) open = await send('GET', url(''));
      String sid =
          (jsonDecode(open.$2.substring(1)) as Map<String, dynamic>)['sid']
              as String;
      (int, String) res = await send(
        'POST',
        url('&sid=$sid'),
        '4a\x1e4boom\x1e4c',
      );
      expect(res, (200, 'ok'));
      expect(messages, <String>['a', 'boom', 'c']);
      await server.close(force: true);
    });
  });

  group('maxWriteBufferSize', () {
    test('a polling client that stops polling is closed', () async {
      expect(const ServerOptions().maxWriteBufferSize, isNull);
      await start(ServerOptions(maxWriteBufferSize: 1000));
      String sid = await pollingSession();
      await send('POST', pollUrl('&sid=$sid'), '40');
      await send('GET', pollUrl('&sid=$sid')); // CONNECT answer
      await Future<void>.delayed(Duration(milliseconds: 50));
      Socket socket = connected.single;
      socket.emit('small', 'x' * 100);
      expect(reasons, isEmpty);
      socket.emit('big', 'x' * 1000);
      await Future<void>.delayed(Duration(milliseconds: 50));
      expect(reasons, <String>[CloseReason.transportError]);
    });
  });

  group('acks', () {
    test('ServerOptions.ackTimeout times out emitWithAck', () async {
      expect(const ServerOptions().ackTimeout, isNull);
      await start(ServerOptions(ackTimeout: Duration(milliseconds: 100)));
      var (WebSocket ws, StreamIterator<dynamic> frames) = await openWs();
      ws.add('40');
      await frames.moveNext();
      await Future<void>.delayed(Duration(milliseconds: 50));
      Socket socket = connected.single;
      await expectLater(
        socket.emitWithAck('ask', 1),
        throwsA(isA<TimeoutException>()),
      );
      expect(socket.waitingAcks, 0);
      await ws.close();
    });

    test('an ack that is not awaited does not crash on disconnect', () async {
      await start(ServerOptions());
      var (WebSocket ws, StreamIterator<dynamic> frames) = await openWs();
      ws.add('40');
      await frames.moveNext();
      await Future<void>.delayed(Duration(milliseconds: 50));
      Socket socket = connected.single;
      unawaited(socket.emitWithAck('ask', 1));
      unawaited(socket.emitWithAck('ask', 2, timeout: Duration(seconds: 5)));
      Future<List<Object?>> awaited = socket.emitWithAck('ask', 3);
      await ws.close();
      // An unhandled error would fail this test.
      await expectLater(awaited, throwsStateError);
      await Future<void>.delayed(Duration(milliseconds: 50));
    });

    test('a remote broadcast ack with no timeout is dropped', () async {
      IsolateAdapter adapter = IsolateAdapter(
        requestsTimeout: Duration(milliseconds: 100),
      );
      await start(ServerOptions(), adapter: adapter);
      var (WebSocket ws, StreamIterator<dynamic> frames) = await openWs();
      ws.add('40');
      await frames.moveNext();
      await Future<void>.delayed(Duration(milliseconds: 50));
      Socket socket = connected.single;
      // A request from another server, with no timeout in its options.
      adapter.onMessage(
        '${adapter.key}-request#/#',
        RedisFormat.json.encode(<String, Object?>{
          'uid': 'other-server',
          'requestId': 'r1',
          'type': RedisRequestType.broadcast,
          'packet': <String, Object?>{
            'type': 2,
            'nsp': '/',
            'data': <Object?>['ask'],
          },
          'opts': <String, Object?>{'rooms': <String>[], 'except': <String>[]},
        }),
      );
      await Future<void>.delayed(Duration(milliseconds: 30));
      expect(socket.waitingAcks, 1);
      await Future<void>.delayed(Duration(milliseconds: 150));
      expect(socket.waitingAcks, 0);
      await ws.close();
    });

    test('a failed server count drops the waiting broadcast acks', () async {
      await start(ServerOptions(), adapter: _BrokenCountAdapter());
      var (WebSocket ws, StreamIterator<dynamic> frames) = await openWs();
      ws.add('40');
      await frames.moveNext();
      await Future<void>.delayed(Duration(milliseconds: 50));
      Socket socket = connected.single;
      await expectLater(
        io!.timeout(Duration(seconds: 30)).emitWithAck('ask'),
        throwsStateError,
      );
      expect(socket.waitingAcks, 0);
      await ws.close();
    });
  });

  group('rooms', () {
    test('join after disconnect leaves no room entry', () async {
      await start(ServerOptions());
      var (WebSocket ws, StreamIterator<dynamic> frames) = await openWs();
      ws.add('40');
      await frames.moveNext();
      await Future<void>.delayed(Duration(milliseconds: 50));
      Socket socket = connected.single;
      socket.join('live');
      expect(await io!.to('live').fetchSockets(), hasLength(1));
      socket.disconnect();
      socket.join('late');
      InMemoryAdapter adapter = io!.of('/').adapter as InMemoryAdapter;
      expect(adapter.rooms(socket.nsp), isEmpty);
      expect(adapter.socketRooms(socket.nsp, socket.id), isEmpty);
      await ws.close();
    });
  });
}
