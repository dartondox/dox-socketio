@Timeout(Duration(seconds: 60))
library;

import 'dart:async';
import 'dart:isolate';

import 'package:dox_core/dox_core.dart';
import 'package:dox_socketio/dox_socketio.dart';
import 'package:socket_io_client/socket_io_client.dart' as sio;
import 'package:test/test.dart';

import 'redis_adapter_test.dart' show collect, connectClient, next, settle;
import 'support/test_app.dart';

/// Server B: a [TestApp] in its own isolate, on the bus of [args.first].
/// It sends its url, then runs the commands it gets and sends the result.
Future<void> _serverB(List<Object> args) async {
  SendPort hub = args[0] as SendPort;
  SendPort out = args[1] as SendPort;
  IsolateBus.instance.connect(hub, 2);
  TestApp app = TestApp(adapter: IsolateAdapter());
  app.io.of('/').onServerSideEmit('ping', _pong);
  await app.start();
  ReceivePort commands = ReceivePort();
  commands.listen((dynamic command) async {
    switch (command) {
      case 'emit':
        await app.io.to('room1').emit('cross', <String, int>{'n': 1});
        out.send('done');
      case 'fetch':
        List<RemoteSocket> sockets = await app.io.fetchSockets();
        out.send(sockets.map(_id).toList());
      case 'stop':
        await app.stop();
        IsolateBus.instance.close();
        commands.close();
        out.send('done');
    }
  });
  out.send(<Object>[app.url, commands.sendPort]);
}

String _id(RemoteSocket socket) => socket.id;

void _pong(ServerSideEvent event) => event.ack('pong from B');

void main() {
  late TestApp serverA;
  late ReceivePort fromB;
  late StreamIterator<dynamic> replies;
  late SendPort toB;
  late String urlB;

  setUp(() async {
    serverA = TestApp(adapter: IsolateAdapter());
    await serverA.start();
    fromB = ReceivePort();
    replies = StreamIterator<dynamic>(fromB);
    await Isolate.spawn(_serverB, <Object>[
      IsolateBus.instance.port,
      fromB.sendPort,
    ]);
    await replies.moveNext();
    List<Object?> ready = replies.current as List<Object?>;
    urlB = ready[0]! as String;
    toB = ready[1]! as SendPort;
    // both isolates listen on the bus
    for (int i = 0; i < 100 && IsolateBus.instance.size < 2; i++) {
      await Future<void>.delayed(Duration(milliseconds: 10));
    }
  });

  tearDown(() async {
    toB.send('stop');
    await replies.moveNext();
    await replies.cancel();
    await serverA.stop();
    IsolateBus.instance.close();
  });

  Future<Object?> ask(String command) async {
    toB.send(command);
    await replies.moveNext();
    return replies.current;
  }

  test('IsolateSocketAdapterFactory makes an IsolateAdapter', () {
    expect(const IsolateSocketAdapterFactory().create(), isA<IsolateAdapter>());
  });

  test('a broadcast in one isolate reaches clients of the other', () async {
    sio.Socket inRoom = await connectClient(serverA.url, 'room1');
    sio.Socket outside = await connectClient(serverA.url, 'room2');
    List<Object?> outsideGot = collect(outside, 'cross');

    Future<Object?> got = next(inRoom, 'cross');
    await ask('emit');
    expect(await got, <String, dynamic>{'n': 1});

    // and back: a client of B gets a broadcast of A
    sio.Socket onB = await connectClient(urlB, 'room3');
    Future<Object?> back = next(onB, 'back');
    await serverA.io.to('room3').emit('back', 'from A');
    expect(await back, 'from A');

    // local stays in A
    List<Object?> local = collect(onB, 'local');
    await serverA.io.local.emit('local', 'x');
    await settle();
    expect(local, isEmpty);
    expect(outsideGot, isEmpty);
    inRoom.dispose();
    outside.dispose();
    onB.dispose();
  });

  test(
    'fetchSockets and serverSideEmitWithAck reach the other isolate',
    () async {
      sio.Socket onA = await connectClient(serverA.url, 'room1');
      List<Object?> ids = (await ask('fetch'))! as List<Object?>;
      expect(ids, hasLength(1));
      expect(await serverA.io.of('/').serverSideEmitWithAck('ping'), <Object?>[
        'pong from B',
      ]);
      onA.dispose();
    },
  );
}
