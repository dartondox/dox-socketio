@Timeout(Duration(seconds: 60))
library;

import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:dox_socketio/dox_socketio.dart';
import 'package:ioredis/ioredis.dart';
import 'package:socket_io_client/socket_io_client.dart' as sio;
import 'package:test/test.dart';

import 'redis_adapter_test.dart'
    show
        answer,
        collect,
        connectClient,
        newAdapter,
        next,
        redisHost,
        redisOptions,
        redisPort,
        redisSkipReason,
        settle;
import 'support/node_runner.dart';
import 'support/test_app.dart';

/// A channel prefix of its own, so other test files that use Redis at the
/// same time are not counted as servers.
String newKey() => 'dox-cluster-${IdGenerator().next()}';

/// Ids of the sockets, sorted.
List<String> ids(List<RemoteSocket> sockets) =>
    sockets.map((RemoteSocket socket) => socket.id).toList()..sort();

/// The server socket of [client].
Socket serverSocket(TestApp app, sio.Socket client) =>
    app.io.sockets.sockets[client.id]!;

/// Many servers on one Redis: requests that go to all servers.
void main() {
  String? skip = redisSkipReason();

  group('two Dox servers', () {
    late TestApp serverA;
    late TestApp serverB;
    late RedisAdapter adapterA;
    late RedisAdapter adapterB;
    List<sio.Socket> clients = <sio.Socket>[];

    setUp(() async {
      String key = newKey();
      adapterA = newAdapter(RedisFormat.msgpack, key);
      adapterB = newAdapter(RedisFormat.json, key);
      serverA = TestApp(adapter: adapterA);
      serverB = TestApp(adapter: adapterB);
      await serverA.start();
      await serverB.start();
      await adapterA.ready();
      await adapterB.ready();
    });

    tearDown(() async {
      for (sio.Socket client in clients) {
        client.dispose();
      }
      clients.clear();
      await serverA.stop();
      await serverB.stop();
    });

    Future<sio.Socket> connect(TestApp app, String room) async {
      sio.Socket client = await connectClient(app.url, room);
      clients.add(client);
      return client;
    }

    test('fetchSockets sees the sockets of both servers', () async {
      sio.Socket a1 = await connect(serverA, 'r');
      sio.Socket b1 = await connect(serverB, 'r');
      sio.Socket b2 = await connect(serverB, 'other');
      serverSocket(serverB, b1).data['name'] = 'bob';

      List<RemoteSocket> all = await serverA.io.fetchSockets();
      expect(ids(all), (<String>[a1.id!, b1.id!, b2.id!])..sort());
      // Local sockets are real sockets, the others are remote copies.
      expect(all.firstWhere((RemoteSocket s) => s.id == a1.id), isA<Socket>());
      RemoteSocket bob = all.firstWhere((RemoteSocket s) => s.id == b1.id);
      expect(bob, isNot(isA<Socket>()));
      expect(bob.data, <String, Object?>{'name': 'bob'});
      expect(bob.rooms, <String>{b1.id!, 'r'});
      expect(bob.handshake.address, isNotEmpty);
      expect(bob.handshake.headers, contains('host'));
      expect(bob.nsp.name, '/');

      expect(
        ids(await serverB.io.to('r').fetchSockets()),
        (<String>[a1.id!, b1.id!])..sort(),
      );
      expect(
        ids(await serverA.io.to('r').except(b1.id!).fetchSockets()),
        <String>[a1.id!],
      );
      expect(ids(await serverA.io.local.fetchSockets()), <String>[a1.id!]);
      expect(await serverA.io.of('/admin').fetchSockets(), isEmpty);

      // A remote socket sends its actions to the server that holds it.
      Future<Object?> got = next(b1, 'hi');
      await bob.emit('hi', 'from A');
      expect(await got, 'from A');
      await bob.join('vip');
      await settle();
      expect(serverSocket(serverB, b1).rooms, contains('vip'));
      await bob.leave('vip');
      await settle();
      expect(serverSocket(serverB, b1).rooms, isNot(contains('vip')));
      Future<Object?> gone = next(b1, 'disconnect');
      await bob.disconnect();
      expect(await gone, 'io server disconnect');
    });

    test('socketsJoin, socketsLeave and disconnectSockets act on both '
        'servers', () async {
      sio.Socket a1 = await connect(serverA, 'r');
      sio.Socket b1 = await connect(serverB, 'r');
      sio.Socket b2 = await connect(serverB, 'other');

      await serverA.io.to('r').socketsJoin('vip');
      await settle();
      expect(
        ids(await serverB.io.to('vip').fetchSockets()),
        (<String>[a1.id!, b1.id!])..sort(),
      );

      await serverB.io.of('/').socketsLeave('vip');
      await settle();
      expect(await serverA.io.to('vip').fetchSockets(), isEmpty);

      Future<Object?> a1Gone = next(a1, 'disconnect');
      Future<Object?> b1Gone = next(b1, 'disconnect');
      await serverA.io.except('other').disconnectSockets();
      expect(await a1Gone, 'io server disconnect');
      expect(await b1Gone, 'io server disconnect');
      await settle();
      expect(b2.connected, isTrue);
      expect(ids(await serverA.io.fetchSockets()), <String>[b2.id!]);
    });

    test('serverSideEmit reaches the other server, not clients', () async {
      sio.Socket a1 = await connect(serverA, 'r');
      List<Object?> clientGot = collect(a1, 'hello');
      List<List<Object?>> gotA = <List<Object?>>[];
      List<List<Object?>> gotB = <List<Object?>>[];
      Completer<void> done = Completer<void>();
      serverA.io.onServerSideEmit('hello', (ServerSideEvent e) {
        gotA.add(e.args);
      });
      serverB.io.onServerSideEmit('hello', (ServerSideEvent e) {
        gotB.add(e.args);
        expect(e.hasAck, isFalse);
        done.complete();
      });

      await serverA.io.serverSideEmit('hello', <Object?>[
        1,
        'two',
        <String, int>{'three': 3},
      ]);
      await done.future.timeout(Duration(seconds: 5));
      expect(gotB, <List<Object?>>[
        <Object?>[
          1,
          'two',
          <String, int>{'three': 3},
        ],
      ]);
      await settle();
      expect(gotA, isEmpty, reason: 'the sender does not get its own event');
      expect(clientGot, isEmpty);
    });

    test('serverSideEmitWithAck gets one answer per other server', () async {
      serverA.io.onServerSideEmit('ping', (ServerSideEvent e) {
        e.ack('pong A ${e.data}');
      });
      serverB.io.onServerSideEmit('ping', (ServerSideEvent e) {
        e.ack(<String, Object?>{'from': 'B', 'got': e.data});
        e.ack('only the first answer is sent');
      });
      serverB.io
          .of('/admin')
          .onServerSideEmit('ping', (ServerSideEvent e) => e.ack('admin'));

      expect(
        await serverA.io.serverSideEmitWithAck('ping', <Object?>[1]),
        <Object?>[
          <String, Object?>{'from': 'B', 'got': 1},
        ],
      );
      expect(
        await serverB.io.serverSideEmitWithAck('ping', <Object?>['x']),
        <Object?>['pong A x'],
      );
      expect(
        await serverA.io.of('/admin').serverSideEmitWithAck('ping'),
        <Object?>['admin'],
      );
    });

    test('broadcast acks come back from clients of both servers', () async {
      sio.Socket a1 = await connect(serverA, 'r');
      sio.Socket b1 = await connect(serverB, 'r');
      sio.Socket b2 = await connect(serverB, 'other');
      answer(a1, 'ask', 'a1');
      answer(b1, 'ask', 'b1');
      answer(b2, 'ask', 'b2');

      List<Object?> all = await serverA.io
          .timeout(Duration(seconds: 3))
          .emitWithAck('ask', 'question');
      expect(all.cast<String>()..sort(), <String>['a1', 'b1', 'b2']);

      List<Object?> room = await serverB.io
          .to('r')
          .timeout(Duration(seconds: 3))
          .emitWithAck('ask');
      expect(room.cast<String>()..sort(), <String>['a1', 'b1']);

      expect(
        await serverA.io.local.timeout(Duration(seconds: 3)).emitWithAck('ask'),
        <Object?>['a1'],
      );
      expect(
        await serverA.io
            .to('nobody')
            .timeout(Duration(seconds: 3))
            .emitWithAck('ask'),
        isEmpty,
      );
    });

    test('a broadcast ack times out when a client does not answer', () async {
      sio.Socket a1 = await connect(serverA, 'r');
      await connect(serverB, 'r'); // never answers
      answer(a1, 'ask', 'a1');
      Object? error;
      try {
        await serverA.io
            .timeout(Duration(milliseconds: 500))
            .emitWithAck('ask');
      } catch (e) {
        error = e;
      }
      expect(error, isA<AckTimeoutException>());
      expect((error! as AckTimeoutException).responses, <Object?>['a1']);
      expect(() => serverA.io.to('r').emitWithAck('ask'), throwsStateError);
    });
  }, skip: skip);

  group('request timeouts', () {
    late TestApp serverA;
    late TestApp serverB;
    late Redis silent;

    setUp(() async {
      String key = newKey();
      RedisAdapter adapterA = newAdapter(
        RedisFormat.msgpack,
        key,
        Duration(milliseconds: 500),
      );
      RedisAdapter adapterB = newAdapter(RedisFormat.msgpack, key);
      serverA = TestApp(adapter: adapterA);
      serverB = TestApp(adapter: adapterB);
      await serverA.start();
      await serverB.start();
      await adapterA.ready();
      await adapterB.ready();
      // A server that listens for requests and never answers.
      silent = Redis(redisOptions);
      await silent.subscribe('$key-request#/#');
    });

    tearDown(() async {
      await silent.disconnect();
      await serverA.stop();
      await serverB.stop();
    });

    test('fetchSockets throws when a server does not answer', () async {
      expect(await adapterServerCount(serverA), 3);
      Stopwatch watch = Stopwatch()..start();
      await expectLater(
        serverA.io.fetchSockets(),
        throwsA(isA<TimeoutException>()),
      );
      expect(watch.elapsedMilliseconds, greaterThanOrEqualTo(450));
      // Local only does not ask the others.
      expect(await serverA.io.local.fetchSockets(), isEmpty);
    });

    test('serverSideEmitWithAck throws with the answers it got', () async {
      serverB.io.onServerSideEmit('ping', (ServerSideEvent e) => e.ack('B'));
      Object? error;
      try {
        await serverA.io.serverSideEmitWithAck('ping');
      } catch (e) {
        error = e;
      }
      expect(error, isA<AckTimeoutException>());
      expect((error! as AckTimeoutException).responses, <Object?>['B']);
    });
  }, skip: skip);

  group('in-memory adapter', () {
    test('acts on this server only', () async {
      TestApp app = TestApp();
      await app.start();
      sio.Socket client = await connectClient(app.url, 'r');
      answer(client, 'ask', 'yes');
      expect(ids(await app.io.fetchSockets()), <String>[client.id!]);
      await app.io.to('r').socketsJoin('vip');
      expect(app.io.sockets.sockets[client.id]!.rooms, contains('vip'));
      await app.io.socketsLeave('vip');
      expect(app.io.sockets.sockets[client.id]!.rooms, isNot(contains('vip')));
      expect(await app.io.serverSideEmitWithAck('ping'), isEmpty);
      await app.io.serverSideEmit('ping');
      expect(
        await app.io.timeout(Duration(seconds: 2)).emitWithAck('ask'),
        <Object?>['yes'],
      );
      Future<Object?> gone = next(client, 'disconnect');
      await app.io.disconnectSockets();
      expect(await gone, 'io server disconnect');
      client.dispose();
      await app.stop();
    });
  });

  group('Node.js server on the same Redis', () {
    NodeRunner node = NodeRunner();
    late TestApp server;

    setUpAll(node.install);

    tearDown(() => server.stop());

    for (RedisFormat format in RedisFormat.values) {
      test('fetchSockets, serverSideEmit and broadcast acks both ways '
          '(${format.name})', () async {
        String key = newKey();
        RedisAdapter adapter = newAdapter(format, key);
        server = TestApp(adapter: adapter);
        await server.start();
        await adapter.ready();
        sio.Socket dartClient = await connectClient(server.url, 'r');
        serverSocket(server, dartClient).data['from'] = 'dart';
        answer(dartClient, 'ask', 'dart');
        List<List<Object?>> notes = <List<Object?>>[];
        server.io.onServerSideEmit('note-dart', (ServerSideEvent e) {
          notes.add(e.args);
        });
        server.io.onServerSideEmit('ping-dart', (ServerSideEvent e) {
          e.ack('pong from dart: ${e.data}');
        });

        Process process = await node.start('redis-cluster.mjs', <String>[
          'redis://$redisHost:$redisPort',
          format.name,
          key,
        ]);
        StringBuffer output = StringBuffer();
        process.stderr.transform(utf8.decoder).listen(output.write);
        StreamController<String> lines = StreamController<String>();
        process.stdout
            .transform(utf8.decoder)
            .transform(const LineSplitter())
            .listen((String line) {
              output.writeln(line);
              lines.add(line);
            });
        StreamIterator<String> read = StreamIterator<String>(lines.stream);
        Future<String> nextLine() async {
          bool more = await read.moveNext().timeout(Duration(seconds: 15));
          if (!more) fail('node stopped: $output');
          return read.current;
        }

        try {
          expect(await nextLine(), 'READY');

          // Dart asks Node.
          List<RemoteSocket> sockets = await server.io.fetchSockets();
          expect(sockets, hasLength(2));
          RemoteSocket fromNode = sockets.firstWhere(
            (RemoteSocket s) => s is! Socket,
          );
          expect(fromNode.data, <String, Object?>{'from': 'node'});
          expect(fromNode.rooms, contains('r'));
          expect(fromNode.handshake.issued, greaterThan(0));
          expect(
            await server.io.serverSideEmitWithAck('ping-node', <Object?>['x']),
            <Object?>['pong from node: x'],
          );
          await server.io.serverSideEmit('note', <Object?>['hi']);
          expect(await nextLine(), 'NOTE hi');
          List<Object?> acks = await server.io
              .to('r')
              .timeout(Duration(seconds: 3))
              .emitWithAck('ask');
          expect(acks.cast<String>()..sort(), <String>['dart', 'node']);
          await fromNode.join('from-dart');
          expect(await nextLine(), 'JOINED from-dart');

          // Node asks Dart.
          process.stdin.writeln('GO');
          await process.stdin.flush();
          expect(await nextLine(), 'DONE');
          expect(await process.exitCode.timeout(Duration(seconds: 15)), 0);
          expect(notes, <List<Object?>>[
            <Object?>['hey'],
          ]);
        } finally {
          process.kill();
          dartClient.dispose();
        }
      });
    }
  }, skip: skip ?? NodeRunner.skipReason());
}

Future<int> adapterServerCount(TestApp app) =>
    app.io.adapter.serverCount(app.io.sockets);
