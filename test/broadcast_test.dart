@Timeout(Duration(seconds: 60))
library;

import 'dart:async';
import 'dart:convert';

import 'package:crypto/crypto.dart' show Hmac, sha256;
import 'package:dox_core/dox_core.dart';
import 'package:dox_socketio/dox_socketio.dart';
import 'package:http/http.dart' as http;
import 'package:ioredis/ioredis.dart';
import 'package:socket_io_client/socket_io_client.dart' as sio;
import 'package:test/test.dart';

import 'redis_adapter_test.dart'
    show
        collect,
        connectClient,
        newAdapter,
        next,
        redisOptions,
        redisSkipReason,
        settle;

Map<String, Map<String, Object?>> users = <String, Map<String, Object?>>{
  'alice': <String, Object?>{'id': 1, 'name': 'Alice'},
  'bob': <String, Object?>{'id': 2, 'name': 'Bob'},
};

/// The auth of a request, like `Auth` of dox_auth.
class TestAuth implements IAuth {
  TestAuth(this._user);

  final Map<String, Object?> _user;

  @override
  Future<void> verifyToken(IDoxRequest req) async {}

  @override
  bool isLoggedIn() => true;

  @override
  T? user<T>() => _user as T?;

  @override
  Map<String, dynamic>? toJson() => _user;

  dynamic id() => _user['id'];
}

/// Logs the request in from `Authorization: Bearer <name>`.
class TestAuthMiddleware implements IDoxMiddleware {
  @override
  IDoxRequest handle(IDoxRequest req) {
    String? name = req.header('authorization')?.substring(7);
    Map<String, Object?>? user = users[name];
    if (user != null) req.auth = TestAuth(user);
    return req;
  }
}

/// `/` with a socket login from `auth.token`. Guests may connect too.
class MainNamespace extends NamespaceHandler {
  @override
  List<Middleware> get middleware => <Middleware>[login];

  void login(Socket socket) => socket.user = users[socket.handshake.token];

  @override
  void onConnection(Socket socket) {}
}

class BroadcastRouter extends Router {
  @override
  void register() {
    Broadcast.routes(middleware: <dynamic>[TestAuthMiddleware()]);
  }
}

class OrderShipped with ShouldBroadcast {
  OrderShipped(this.id);

  final int id;

  @override
  List<Channel> broadcastOn() => <Channel>[PrivateChannel('orders.$id')];

  @override
  Map<String, Object?> broadcastWith() => <String, Object?>{'id': id};
}

class News with ShouldBroadcast {
  @override
  List<Channel> broadcastOn() => <Channel>[Channel('news')];

  @override
  String broadcastAs() => 'news.posted';
}

List<String> seenParams = <String>[];

bool ownsOrder(Object? user, String id) =>
    (user! as Map<String, Object?>)['id'].toString() == id;

Map<String, Object?>? chatMember(Object? user, String room) {
  if (room == 'closed') return null;
  Map<String, Object?> u = user! as Map<String, Object?>;
  return <String, Object?>{'id': u['id'], 'name': u['name']};
}

bool teamUser(Object? user, String team, String member) {
  seenParams.addAll(<String>[team, member]);
  return true;
}

bool throwingRule(Object? user) => throw StateError('db is down');

String get baseUrl => 'http://127.0.0.1:${Dox().server.httpServer.port}';

Future<sio.Socket> connect([String? user]) => connectClient(
  baseUrl,
  '',
  auth: user == null ? null : <String, String>{'token': user},
);

/// Sends `subscribe` and gives the ack.
Future<Object?> subscribe(sio.Socket socket, String channel, [String? auth]) {
  Completer<Object?> done = Completer<Object?>();
  socket.emitWithAck('subscribe', <String, Object?>{
    'channel': channel,
    'auth': ?auth,
  }, ack: done.complete);
  return done.future.timeout(Duration(seconds: 5));
}

Future<Object?> unsubscribe(sio.Socket socket, String channel) {
  Completer<Object?> done = Completer<Object?>();
  socket.emitWithAck('unsubscribe', <String, Object?>{
    'channel': channel,
  }, ack: done.complete);
  return done.future.timeout(Duration(seconds: 5));
}

const Map<String, Object> ok = <String, Object>{'ok': true};
const Map<String, Object> refused = <String, Object>{
  'ok': false,
  'error': 'Forbidden',
};

void main() {
  List<sio.Socket> clients = <sio.Socket>[];

  Future<sio.Socket> client([String? user]) async {
    sio.Socket socket = await connect(user);
    clients.add(socket);
    return socket;
  }

  setUp(() {
    Broadcast.channel('orders.{id}', ownsOrder);
    Broadcast.channel('chat.{room}', chatMember);
    Broadcast.channel('teams.{team}.users.{user}', teamUser);
    Broadcast.channel('broken', throwingRule);
    // the provider listened already: this only sets the send target again
    Broadcast.listen(SocketIoProvider.server!.sockets);
  });

  setUpAll(() async {
    Dox().initialize(
      AppConfig(
        appKey: '4HyiSrq4N5Nfg6bOadIhbFEI8zbUkpxt',
        serverPort: 0,
        totalIsolate: 1,
        routers: <Router>[BroadcastRouter()],
        providers: <DoxProvider>[
          SocketIoProvider(
            SocketIoConfig(
              broadcasting: true,
              namespaces: <NamespaceHandler>[MainNamespace()],
            ),
          ),
        ],
      ),
    );
    await Dox().startServer();
  });

  tearDown(() {
    for (sio.Socket socket in clients) {
      socket.dispose();
    }
    clients.clear();
    Broadcast.tokenLifetime = const Duration(minutes: 1);
    Broadcast.maxChannelLength = 200;
    Broadcast.maxChannelsPerSocket = 100;
    Broadcast.reset();
  });

  tearDownAll(() async {
    await SocketIoProvider.server?.close();
    await Dox().server.close(force: true);
  });

  group('rules', () {
    test('channel helpers add the prefix', () {
      expect(Channel('news').name, 'news');
      expect(PrivateChannel('orders.1').name, 'private-orders.1');
      expect(PresenceChannel('chat.1').name, 'presence-chat.1');
    });

    test('params are passed in order', () async {
      seenParams.clear();
      expect(
        await Broadcast.authorize(users['alice'], 'private-teams.7.users.3'),
        true,
      );
      expect(seenParams, <String>['7', '3']);
    });

    test('a param does not match a dot', () async {
      expect(
        await Broadcast.authorize(users['alice'], 'private-orders.1'),
        true,
      );
      expect(
        await Broadcast.authorize(users['alice'], 'private-orders.1.2'),
        isNull,
      );
      expect(
        await Broadcast.authorize(users['alice'], 'private-orders.2'),
        isNull,
      );
    });

    test('no rule, no user, or a throwing rule refuses', () async {
      expect(await Broadcast.authorize(users['alice'], 'private-x'), isNull);
      expect(await Broadcast.authorize(null, 'private-orders.1'), isNull);
      expect(
        await Broadcast.authorize(users['alice'], 'private-broken'),
        isNull,
      );
    });

    test('public is open, presence gives the member info', () async {
      expect(await Broadcast.authorize(null, 'news'), true);
      expect(
        await Broadcast.authorize(users['bob'], 'presence-chat.1'),
        <String, Object?>{'id': 2, 'name': 'Bob'},
      );
      expect(
        await Broadcast.authorize(users['bob'], 'presence-chat.closed'),
        isNull,
      );
    });
  });

  group('subscribe', () {
    test('public channel is open to guests', () async {
      sio.Socket guest = await client();
      expect(await subscribe(guest, 'news'), ok);
    });

    test('private channel needs a passing rule', () async {
      sio.Socket alice = await client('alice');
      sio.Socket bob = await client('bob');
      sio.Socket guest = await client();
      expect(await subscribe(alice, 'private-orders.1'), ok);
      expect(await subscribe(bob, 'private-orders.1'), refused);
      expect(await subscribe(guest, 'private-orders.1'), refused);
    });

    test('presence channel ok and refused', () async {
      sio.Socket alice = await client('alice');
      Future<Object?> here = next(alice, 'presence:subscribed');
      expect(await subscribe(alice, 'presence-chat.1'), ok);
      expect(await here, <Object?>[
        'presence-chat.1',
        <Object?>[
          <String, Object?>{
            'user_id': 1,
            'user_info': <String, Object?>{'id': 1, 'name': 'Alice'},
          },
        ],
      ]);
      expect(await subscribe(alice, 'presence-chat.closed'), refused);
    });

    test('a second listen on one namespace adds no handler', () async {
      Broadcast.listen(SocketIoProvider.server!.sockets);
      sio.Socket alice = await client('alice');
      List<Object?> here = collect(alice, 'presence:subscribed');
      expect(await subscribe(alice, 'presence-chat.l'), ok);
      await settle();
      expect(here, hasLength(1));
    });

    test('the same channel twice at once joins once', () async {
      sio.Socket alice = await client('alice');
      List<Object?> here = collect(alice, 'presence:subscribed');
      List<Object?> acks = await Future.wait(<Future<Object?>>[
        subscribe(alice, 'presence-chat.t'),
        subscribe(alice, 'presence-chat.t'),
      ]);
      expect(acks, unorderedEquals(<Object?>[ok, _err('already subscribing')]));
      await settle();
      expect(here, hasLength(1));
      expect(await subscribe(alice, 'presence-chat.t'), ok);
    });

    test('channel name and channel count limits', () async {
      Broadcast.maxChannelLength = 10;
      Broadcast.maxChannelsPerSocket = 2;
      sio.Socket guest = await client();
      expect(
        await subscribe(guest, 'a' * 11),
        _err('channel name is too long'),
      );
      expect(await subscribe(guest, 'a' * 10), ok);
      expect(await subscribe(guest, 'b'), ok);
      expect(await subscribe(guest, 'c'), _err('too many channels'));
      expect(await unsubscribe(guest, 'b'), ok);
      expect(await subscribe(guest, 'c'), ok);
    });

    test('a channel that is not a string gets an error', () async {
      sio.Socket guest = await client();
      Completer<Object?> done = Completer<Object?>();
      guest.emitWithAck('subscribe', <String, Object>{
        'channel': 5,
      }, ack: done.complete);
      expect(await done.future, _err('channel is missing'));
    });

    test('unsubscribe of a channel never joined is ok', () async {
      sio.Socket alice = await client('alice');
      expect(await unsubscribe(alice, 'presence-chat.never'), ok);
    });

    test('a missing channel gets an error', () async {
      sio.Socket guest = await client();
      Completer<Object?> done = Completer<Object?>();
      guest.emitWithAck('subscribe', 'oops', ack: done.complete);
      expect(await done.future, <String, Object?>{
        'ok': false,
        'error': 'channel is missing',
      });
    });
  });

  group('token', () {
    test('a valid token lets a guest subscribe', () async {
      sio.Socket guest = await client();
      String token = Broadcast.authToken(guest.id!, 'private-orders.9');
      expect(await subscribe(guest, 'private-orders.9', token), ok);
    });

    test('a wrong signature is refused', () async {
      sio.Socket guest = await client();
      String token = Broadcast.authToken(guest.id!, 'private-orders.9');
      String last = token.endsWith('0') ? '1' : '0';
      String bad = token.substring(0, token.length - 1) + last;
      expect(await subscribe(guest, 'private-orders.9', bad), refused);
      expect(await subscribe(guest, 'private-orders.9', 'junk'), refused);
    });

    test('an expired token is refused', () async {
      sio.Socket guest = await client();
      Broadcast.tokenLifetime = const Duration(seconds: -5);
      String token = Broadcast.authToken(guest.id!, 'private-orders.9');
      expect(await subscribe(guest, 'private-orders.9', token), refused);
    });

    test('a token of another socket is refused', () async {
      sio.Socket guest = await client();
      String token = Broadcast.authToken('other-socket', 'private-orders.9');
      expect(await subscribe(guest, 'private-orders.9', token), refused);
    });

    test('a token of another channel is refused', () async {
      sio.Socket guest = await client();
      String token = Broadcast.authToken(guest.id!, 'private-orders.8');
      expect(await subscribe(guest, 'private-orders.9', token), refused);
    });

    test('malformed tokens are refused', () async {
      sio.Socket guest = await client();
      String signed(String body) => '$body.${_sign(body)}';
      String b64(String text) => base64Url.encode(utf8.encode(text));
      for (String token in <String>[
        'nodot',
        signed('!!not base64!!'),
        signed(b64('not json')),
        signed(b64('[1, 2]')),
      ]) {
        expect(await subscribe(guest, 'private-orders.9', token), refused);
      }
    });

    test('a presence token needs a member', () async {
      sio.Socket guest = await client();
      String plain = Broadcast.authToken(guest.id!, 'presence-chat.1');
      expect(await subscribe(guest, 'presence-chat.1', plain), refused);
    });
  });

  group('presence', () {
    test('two sockets of one user are one member', () async {
      sio.Socket bob = await client('bob');
      await subscribe(bob, 'presence-chat.2');
      List<Object?> joining = collect(bob, 'presence:joining');
      List<Object?> leaving = collect(bob, 'presence:leaving');

      sio.Socket alice1 = await client('alice');
      await subscribe(alice1, 'presence-chat.2');
      sio.Socket alice2 = await client('alice');
      Future<Object?> here = next(alice2, 'presence:subscribed');
      await subscribe(alice2, 'presence-chat.2');
      await settle();
      expect(joining, hasLength(1));
      expect((joining.single! as List<Object?>).last, <String, Object?>{
        'user_id': 1,
        'user_info': <String, Object?>{'id': 1, 'name': 'Alice'},
      });
      List<Object?> members =
          (await here as List<Object?>).last as List<Object?>;
      expect(members.map(_userId).toSet(), <Object?>{1, 2});
      expect(members, hasLength(2));

      alice1.dispose();
      await settle();
      expect(leaving, isEmpty);
      expect(await unsubscribe(alice2, 'presence-chat.2'), ok);
      await settle();
      expect(leaving, hasLength(1));
      expect((leaving.single! as List<Object?>).first, 'presence-chat.2');
    });
  });

  group('presence more', () {
    test('two sockets of one user at the same time: one joining', () async {
      sio.Socket bob = await client('bob');
      await subscribe(bob, 'presence-chat.same');
      List<Object?> joining = collect(bob, 'presence:joining');
      sio.Socket alice1 = await client('alice');
      sio.Socket alice2 = await client('alice');
      await Future.wait(<Future<Object?>>[
        subscribe(alice1, 'presence-chat.same'),
        subscribe(alice2, 'presence-chat.same'),
      ]);
      await settle();
      expect(joining, hasLength(1));
    });

    test('a disconnect sends presence:leaving', () async {
      sio.Socket bob = await client('bob');
      sio.Socket alice = await client('alice');
      await subscribe(bob, 'presence-chat.d');
      await subscribe(alice, 'presence-chat.d');
      Future<Object?> leaving = next(bob, 'presence:leaving');
      alice.dispose();
      expect((await leaving)! as List<Object?>, <Object?>[
        'presence-chat.d',
        <String, Object?>{
          'user_id': 1,
          'user_info': <String, Object?>{'id': 1, 'name': 'Alice'},
        },
      ]);
    });
  });

  group('events', () {
    test('only subscribed sockets get the event', () async {
      sio.Socket alice = await client('alice');
      sio.Socket other = await client('alice');
      await subscribe(alice, 'private-orders.1');
      List<Object?> got = collect(alice, 'OrderShipped');
      List<Object?> notGot = collect(other, 'OrderShipped');
      await Broadcast.event(OrderShipped(1));
      await broadcast(OrderShipped(2));
      await settle();
      expect(got, <Object?>[
        <Object?>[
          'private-orders.1',
          <String, Object?>{'id': 1},
        ],
      ]);
      expect(notGot, isEmpty);
    });

    test('toOthers skips the sender and broadcastAs names it', () async {
      sio.Socket a = await client();
      sio.Socket b = await client();
      await subscribe(a, 'news');
      await subscribe(b, 'news');
      List<Object?> gotA = collect(a, 'news.posted');
      List<Object?> gotB = collect(b, 'news.posted');
      await Broadcast.event(News(), toOthers: a.id);
      await settle();
      expect(gotA, isEmpty);
      expect(gotB, hasLength(1));
    });

    test('unsubscribe stops the events', () async {
      sio.Socket a = await client();
      await subscribe(a, 'news');
      await unsubscribe(a, 'news');
      List<Object?> got = collect(a, 'news.posted');
      await Broadcast.event(News());
      await settle();
      expect(got, isEmpty);
    });
  });

  group('http auth', () {
    Future<http.Response> auth(String? user, String socketId, String channel) {
      return http.post(
        Uri.parse('$baseUrl/broadcasting/auth'),
        headers: <String, String>{
          'content-type': 'application/json',
          'x-socket-id': socketId,
          'authorization': ?(user == null ? null : 'Bearer $user'),
        },
        body: jsonEncode(<String, String>{
          'socket_id': socketId,
          'channel_name': channel,
        }),
      );
    }

    test('200 with a token that subscribes', () async {
      sio.Socket guest = await client();
      http.Response res = await auth('alice', guest.id!, 'private-orders.1');
      expect(res.statusCode, 200);
      String token =
          (jsonDecode(res.body) as Map<String, dynamic>)['auth'] as String;
      expect(await subscribe(guest, 'private-orders.1', token), ok);
    });

    test('presence token carries the member', () async {
      sio.Socket guest = await client();
      http.Response res = await auth('bob', guest.id!, 'presence-chat.5');
      expect(res.statusCode, 200);
      String token =
          (jsonDecode(res.body) as Map<String, dynamic>)['auth'] as String;
      Future<Object?> here = next(guest, 'presence:subscribed');
      expect(await subscribe(guest, 'presence-chat.5', token), ok);
      List<Object?> members =
          (await here as List<Object?>).last as List<Object?>;
      expect(members.map(_userId), <Object?>[2]);
    });

    test('403 for a socket_id that is not a Socket.IO id', () async {
      expect(
        (await auth('alice', 'bad id!', 'private-orders.1')).statusCode,
        403,
      );
      expect(
        (await auth('alice', 'a' * 21, 'private-orders.1')).statusCode,
        403,
      );
    });

    test('403 for a refused or guest user', () async {
      expect((await auth('bob', 'sid', 'private-orders.1')).statusCode, 403);
      expect((await auth(null, 'sid', 'private-orders.1')).statusCode, 403);
      expect((await auth('alice', '', 'private-orders.1')).statusCode, 403);
    });
  });

  group('fake', () {
    test('keeps events and sends nothing', () async {
      BroadcastFake fake = Broadcast.fake();
      sio.Socket a = await client();
      await subscribe(a, 'news');
      List<Object?> got = collect(a, 'news.posted');
      fake.assertNothingBroadcasted();
      await Broadcast.event(News(), toOthers: 'x');
      await broadcast(OrderShipped(4));
      await settle();
      expect(got, isEmpty);
      fake.assertBroadcasted<News>();
      fake.assertBroadcasted<OrderShipped>(_isFour);
      fake.assertBroadcastedOn<OrderShipped>('private-orders.4');
      fake.assertBroadcastedTimes<News>(1);
      expect(fake.broadcasted.first.toOthers, 'x');
      expect(
        () => fake.assertNotBroadcasted<News>(),
        throwsA(isA<AssertionError>()),
      );
      expect(
        () => fake.assertBroadcastedOn<News>('private-orders.4'),
        throwsA(isA<AssertionError>()),
      );
    });
  });

  group('two servers over redis', () {
    test('presence joins and member list cross servers', () async {
      // own key: servers of other test files do not answer requests
      SocketIoServer a = SocketIoServer(
        adapter: newAdapter(RedisFormat.msgpack, 'broadcast-test'),
      );
      SocketIoServer b = SocketIoServer(
        adapter: newAdapter(RedisFormat.msgpack, 'broadcast-test'),
      );
      for (SocketIoServer io in <SocketIoServer>[a, b]) {
        io.use(MainNamespace().login);
        Broadcast.listen(io.sockets);
      }
      String urlA = 'http://127.0.0.1:${(await a.listen(0)).port}';
      String urlB = 'http://127.0.0.1:${(await b.listen(0)).port}';
      try {
        sio.Socket bob = await connectClient(
          urlA,
          '',
          auth: <String, String>{'token': 'bob'},
        );
        clients.add(bob);
        await subscribe(bob, 'presence-chat.r');
        List<Object?> joining = collect(bob, 'presence:joining');
        sio.Socket alice = await connectClient(
          urlB,
          '',
          auth: <String, String>{'token': 'alice'},
        );
        clients.add(alice);
        Future<Object?> here = next(alice, 'presence:subscribed');
        await subscribe(alice, 'presence-chat.r');
        List<Object?> members =
            (await here as List<Object?>).last as List<Object?>;
        expect(members.map(_userId).toSet(), <Object?>{1, 2});
        await settle();
        expect(joining, hasLength(1));
        List<Object?> got = collect(bob, 'news.posted');
        await subscribe(bob, 'news');
        await Broadcast.event(News());
        await settle();
        expect(got, hasLength(1));
        Future<Object?> leaving = next(bob, 'presence:leaving');
        alice.dispose();
        expect(((await leaving)! as List<Object?>).first, 'presence-chat.r');
      } finally {
        await a.close();
        await b.close();
      }
    });

    test('emitter sends with no listen, toOthers skips one', () async {
      SocketIoServer io = SocketIoServer(
        adapter: newAdapter(RedisFormat.msgpack, 'broadcast-emit'),
      );
      io.use(MainNamespace().login);
      Broadcast.listen(io.sockets);
      String url = 'http://127.0.0.1:${(await io.listen(0)).port}';
      Redis redis = Redis(redisOptions);
      try {
        Broadcast.reset();
        expect(() => Broadcast.event(News()), throwsStateError);
        Broadcast.emitter = SocketIoEmitter(redis, key: 'broadcast-emit');
        sio.Socket a = await connectClient(url, '');
        sio.Socket b = await connectClient(url, '');
        clients.addAll(<sio.Socket>[a, b]);
        await subscribe(a, 'news');
        await subscribe(b, 'news');
        List<Object?> gotA = collect(a, 'news.posted');
        List<Object?> gotB = collect(b, 'news.posted');
        await Broadcast.event(News());
        await Broadcast.event(News(), toOthers: a.id);
        await settle();
        expect(gotA, hasLength(1));
        expect(gotB, hasLength(2));
      } finally {
        await redis.disconnect();
        await io.close();
      }
    });

    test(
      'a server that does not answer: no join, leaving still sent',
      () async {
        SocketIoServer io = SocketIoServer(
          adapter: newAdapter(
            RedisFormat.msgpack,
            'broadcast-dead',
            const Duration(milliseconds: 300),
          ),
        );
        io.use(MainNamespace().login);
        Broadcast.listen(io.sockets);
        String url = 'http://127.0.0.1:${(await io.listen(0)).port}';
        Redis dead = Redis(redisOptions);
        try {
          sio.Socket bob = await connectClient(
            url,
            '',
            auth: <String, String>{'token': 'bob'},
          );
          sio.Socket alice = await connectClient(
            url,
            '',
            auth: <String, String>{'token': 'alice'},
          );
          clients.addAll(<sio.Socket>[bob, alice]);
          expect(await subscribe(bob, 'presence-chat.x'), ok);
          expect(await subscribe(alice, 'presence-chat.x'), ok);
          // a peer that counts as a server but never answers
          await dead.subscribe('broadcast-dead-request#/#');
          sio.Socket carol = await connectClient(
            url,
            '',
            auth: <String, String>{'token': 'bob'},
          );
          clients.add(carol);
          expect(
            await subscribe(carol, 'presence-chat.y'),
            _err('presence is not available'),
          );
          Future<Object?> leaving = next(bob, 'presence:leaving');
          expect(await unsubscribe(alice, 'presence-chat.x'), ok);
          expect(((await leaving)! as List<Object?>).first, 'presence-chat.x');
        } finally {
          await dead.disconnect();
          await io.close();
        }
      },
    );
  }, skip: redisSkipReason());
}

Object? _userId(Object? member) => (member! as Map<String, Object?>)['user_id'];

bool _isFour(OrderShipped event) => event.id == 4;

Map<String, Object> _err(String message) => <String, Object>{
  'ok': false,
  'error': message,
};

String _sign(String body) => Hmac(
  sha256,
  KeyDerivation.forAppKey('dox:broadcast'),
).convert(utf8.encode(body)).toString();
