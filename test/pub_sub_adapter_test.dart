@Timeout(Duration(seconds: 60))
library;

import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:dox_socketio/dox_socketio.dart';
import 'package:socket_io_client/socket_io_client.dart' as sio;
import 'package:test/test.dart';

import 'redis_adapter_test.dart' show collect, connectClient, next, settle;
import 'support/test_app.dart';

/// A pub/sub adapter that sends straight to the other adapters of the
/// same test, in this isolate.
class _LinkedAdapter extends PubSubAdapter {
  _LinkedAdapter(this.peers);

  final List<_LinkedAdapter> peers;

  /// When set, a publish waits for it.
  Completer<void>? gate;

  /// When true, a publish throws.
  bool failPublish = false;

  @override
  Future<void> subscribe(Namespace nsp) async {}

  @override
  Future<void> unsubscribe(Namespace nsp) async {}

  @override
  Future<int> serverCount(Namespace nsp) async => peers.length;

  @override
  Future<void> publish(String channel, Uint8List message) async {
    if (failPublish) throw StateError('redis is down');
    Completer<void>? wait = gate;
    if (wait != null) await wait.future;
    await Future<void>.value();
    for (_LinkedAdapter peer in peers) {
      if (!identical(peer, this)) peer.onMessage(channel, message);
    }
  }
}

/// Keeps what is written to stderr.
class _Stderr implements Stdout {
  final List<String> lines = <String>[];

  @override
  void writeln([Object? object = '']) => lines.add('$object');

  @override
  dynamic noSuchMethod(Invocation invocation) => null;
}

/// Runs [body] and gives the stderr lines it wrote.
List<String> _stderrOf(void Function() body) {
  _Stderr err = _Stderr();
  IOOverrides.runZoned(body, stderr: () => err);
  return err.lines;
}

void main() {
  List<_LinkedAdapter> peers = <_LinkedAdapter>[];
  late TestApp serverA;
  late TestApp serverB;
  late _LinkedAdapter adapterA;
  late _LinkedAdapter adapterB;
  List<sio.Socket> clients = <sio.Socket>[];

  setUp(() async {
    peers.clear();
    adapterA = _LinkedAdapter(peers);
    adapterB = _LinkedAdapter(peers);
    peers.addAll(<_LinkedAdapter>[adapterA, adapterB]);
    serverA = TestApp(adapter: adapterA);
    serverB = TestApp(adapter: adapterB);
    await serverA.start();
    await serverB.start();
  });

  tearDown(() async {
    for (sio.Socket client in clients) {
      client.dispose();
    }
    clients.clear();
    await serverA.stop();
    await serverB.stop();
  });

  Future<sio.Socket> client(TestApp app, String room) async {
    sio.Socket socket = await connectClient(app.url, room);
    clients.add(socket);
    return socket;
  }

  group('skips the decode when no local socket can get it', () {
    // Bytes that fail to decode: a decode logs "bad adapter message".
    Uint8List bad = Uint8List.fromList(<int>[0xc1]);

    test('a one-room broadcast for a room this server does not have', () async {
      await client(serverB, 'room1');
      Namespace nsp = serverB.io.of('/');
      String base = adapterB.channelOf(nsp);
      expect(
        _stderrOf(() => adapterB.onMessage('${base}room2#', bad)),
        isEmpty,
      );
      expect(
        _stderrOf(() => adapterB.onMessage('${base}room1#', bad)),
        hasLength(1),
      );
      // A room name may hold `#`.
      expect(
        _stderrOf(() => adapterB.onMessage('${base}room1##', bad)),
        isEmpty,
      );
    });

    test('a namespace broadcast when this server has no socket', () async {
      Namespace nsp = serverB.io.of('/');
      String base = adapterB.channelOf(nsp);
      expect(_stderrOf(() => adapterB.onMessage(base, bad)), isEmpty);
      await client(serverB, '');
      expect(_stderrOf(() => adapterB.onMessage(base, bad)), hasLength(1));
    });

    test('requests are still decoded', () {
      Namespace nsp = serverB.io.of('/');
      expect(
        _stderrOf(
          () => adapterB.onMessage(adapterB.requestChannelOf(nsp), bad),
        ),
        hasLength(1),
      );
    });
  });

  group('broadcasts across servers', () {
    test('one room, many rooms, no room and except', () async {
      sio.Socket inRoom1 = await client(serverB, 'room1');
      sio.Socket inRoom2 = await client(serverB, 'room2');
      List<Object?> got1 = collect(inRoom1, 'n');
      List<Object?> got2 = collect(inRoom2, 'n');
      await serverA.io.to('room1').emit('n', 'one');
      await serverA.io.to('room1').to('room2').emit('n', 'many');
      await serverA.io.except('room1').emit('n', 'all but room1');
      await serverA.io.to('room3').emit('n', 'nobody');
      await settle();
      expect(got1, <Object?>['one', 'many']);
      expect(got2, <Object?>['many', 'all but room1']);
    });

    test('fetchSockets and socketsJoin still work', () async {
      await client(serverB, 'room1');
      List<RemoteSocket> sockets = await serverA.io.to('room1').fetchSockets();
      expect(sockets, hasLength(1));
      await serverA.io.to('room1').socketsJoin('room9');
      await settle();
      expect(await serverA.io.to('room9').fetchSockets(), hasLength(1));
    });
  });

  group('the local send does not wait for the publish', () {
    test('local clients get it while the publish waits', () async {
      sio.Socket local = await client(serverA, 'room1');
      sio.Socket remote = await client(serverB, 'room1');
      Future<Object?> gotLocal = next(local, 'n');
      Future<Object?> gotRemote = next(remote, 'n');
      Completer<void> gate = Completer<void>();
      adapterA.gate = gate;
      bool done = false;
      unawaited(serverA.io.to('room1').emit('n', 1).then((_) => done = true));
      expect(await gotLocal, 1);
      expect(done, isFalse, reason: 'emit waits for the publish');
      gate.complete();
      expect(await gotRemote, 1);
      await settle();
      expect(done, isTrue);
    });

    test('messages from one sender keep their order', () async {
      sio.Socket local = await client(serverA, 'room1');
      sio.Socket remote = await client(serverB, 'room1');
      List<Object?> gotLocal = collect(local, 'n');
      List<Object?> gotRemote = collect(remote, 'n');
      for (int i = 0; i < 20; i++) {
        unawaited(serverA.io.to('room1').emit('n', i));
      }
      await settle();
      List<int> all = List<int>.generate(20, (int i) => i);
      expect(gotLocal, all);
      expect(gotRemote, all);
    });

    test('a failed publish is logged and local clients still get it', () async {
      sio.Socket local = await client(serverA, 'room1');
      Future<Object?> gotLocal = next(local, 'n');
      adapterA.failPublish = true;
      _Stderr err = _Stderr();
      await IOOverrides.runZoned(
        () => serverA.io.to('room1').emit('n', 1),
        stderr: () => err,
      );
      expect(await gotLocal, 1);
      expect(err.lines.single, contains('[socketio] publish failed'));
    });
  });

  group('fetchSocketsData', () {
    test('other servers send only the id and that data key', () async {
      await client(serverB, 'room1');
      Socket onB = serverB.io.of('/').sockets.values.single;
      onB.data['keep'] = <String, Object?>{'n': 1};
      onB.data['drop'] = 'big';
      Namespace nsp = serverA.io.of('/');
      List<RemoteSocket> slim = await adapterA.fetchSocketsData(
        nsp,
        BroadcastOptions(rooms: <String>{'room1'}),
        'keep',
      );
      expect(slim.single.id, onB.id);
      expect(slim.single.data, <String, Object?>{
        'keep': <String, Object?>{'n': 1},
      });
      expect(slim.single.handshake.headers, isEmpty);
      // The normal call still sends the whole socket.
      List<RemoteSocket> full = await serverA.io.to('room1').fetchSockets();
      expect(full.single.data['drop'], 'big');
      expect(full.single.handshake.headers, isNotEmpty);
      expect(full.single.rooms, contains('room1'));
    });

    test('local sockets are the real sockets', () async {
      await client(serverA, 'room1');
      List<RemoteSocket> sockets = await adapterA.fetchSocketsData(
        serverA.io.of('/'),
        BroadcastOptions(rooms: <String>{'room1'}),
        'keep',
      );
      expect(sockets.single, isA<Socket>());
    });
  });
}
