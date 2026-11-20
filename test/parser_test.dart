import 'dart:convert';
import 'dart:math';
import 'dart:typed_data';

import 'package:dox_socketio/dox_socketio.dart';
import 'package:test/test.dart';

void main() {
  group('EngineParser', () {
    EngineParser parser = const EngineParser();

    test('encodes text packets', () {
      expect(
        parser.encodeForWebsocket(EnginePacket(EnginePacketType.message, 'hi')),
        '4hi',
      );
      expect(parser.encodeForPolling(EnginePacket(EnginePacketType.ping)), '2');
    });

    test('SharedText makes one websocket frame and works for polling', () {
      SharedText shared = SharedText('2["hi","é"]');
      expect(utf8.decode(shared.frame), '42["hi","é"]');
      expect(identical(shared.frame, shared.frame), isTrue);
      EnginePacket packet = EnginePacket(EnginePacketType.message, shared);
      expect(parser.encodeForPolling(packet), '42["hi","é"]');
    });

    test('encodes binary for websocket and polling', () {
      Uint8List bytes = Uint8List.fromList(<int>[1, 2, 3]);
      EnginePacket packet = EnginePacket(EnginePacketType.message, bytes);
      expect(parser.encodeForWebsocket(packet), bytes);
      expect(parser.encodeForPolling(packet), 'bAQID');
    });

    test('encodes and decodes a polling payload', () {
      String payload = parser.encodePayload(<EnginePacket>[
        EnginePacket(EnginePacketType.message, 'hello'),
        EnginePacket(
          EnginePacketType.message,
          Uint8List.fromList(<int>[1, 2, 3, 4]),
        ),
        EnginePacket(EnginePacketType.pong),
      ]);
      expect(payload, '4hello\x1ebAQIDBA==\x1e3');
      List<EnginePacket> packets = parser.decodePayload(payload);
      expect(packets.map((EnginePacket p) => p.type), <EnginePacketType>[
        EnginePacketType.message,
        EnginePacketType.message,
        EnginePacketType.pong,
      ]);
      expect(packets[0].data, 'hello');
      expect(packets[1].data, <int>[1, 2, 3, 4]);
    });

    test('decodes probe and binary frames', () {
      EnginePacket probe = parser.decodeFrame('2probe');
      expect(probe.type, EnginePacketType.ping);
      expect(probe.data, 'probe');
      EnginePacket binary = parser.decodeFrame(<int>[5, 6]);
      expect(binary.type, EnginePacketType.message);
      expect(binary.isBinary, isTrue);
    });

    test('throws on bad packets', () {
      expect(() => parser.decodeText(''), throwsFormatException);
      expect(() => parser.decodeText('9x'), throwsFormatException);
      expect(() => parser.decodeText('bnot base64!'), throwsFormatException);
    });
  });

  group('SocketEncoder', () {
    SocketEncoder encoder = const SocketEncoder();

    test('encodes connect, event, ack in the default namespace', () {
      expect(
        encoder.encode(
          SocketPacket(
            type: SocketPacketType.connect,
            data: <String, String>{'sid': 'abc'},
          ),
        ),
        <Object>['0{"sid":"abc"}'],
      );
      expect(
        encoder.encode(
          SocketPacket(
            type: SocketPacketType.event,
            data: <Object>['chat', 'hi'],
            id: 12,
          ),
        ),
        <Object>['212["chat","hi"]'],
      );
      expect(
        encoder.encode(
          SocketPacket(type: SocketPacketType.ack, id: 3, data: <Object>[1]),
        ),
        <Object>['33[1]'],
      );
    });

    test('adds a custom namespace', () {
      expect(
        encoder.encode(
          SocketPacket(type: SocketPacketType.disconnect, nsp: '/admin'),
        ),
        <Object>['1/admin,'],
      );
      expect(
        encoder.encode(
          SocketPacket(
            type: SocketPacketType.event,
            nsp: '/admin',
            id: 1,
            data: <Object>['x'],
          ),
        ),
        <Object>['2/admin,1["x"]'],
      );
    });

    test('turns binary data into attachments', () {
      Uint8List a = Uint8List.fromList(<int>[1]);
      Uint8List b = Uint8List.fromList(<int>[2]);
      List<Object> parts = encoder.encode(
        SocketPacket(
          type: SocketPacketType.event,
          nsp: '/files',
          data: <Object>[
            'upload',
            <String, Object>{
              'a': a,
              'list': <Object>[b, 'text'],
            },
          ],
        ),
      );
      expect(
        parts.first,
        '52-/files,["upload",{"a":{"_placeholder":true,"num":0},'
        '"list":[{"_placeholder":true,"num":1},"text"]}]',
      );
      expect(parts.sublist(1), <Object>[a, b]);
    });

    test('binary ack', () {
      List<Object> parts = encoder.encode(
        SocketPacket(
          type: SocketPacketType.ack,
          id: 7,
          data: <Object>[
            Uint8List.fromList(<int>[9]),
          ],
        ),
      );
      expect(parts.first, '61-7[{"_placeholder":true,"num":0}]');
    });
  });

  group('SocketDecoder', () {
    test('decodes connect with auth and namespace', () {
      SocketPacket packet = SocketDecoder().add('0/admin,{"token":"x"}')!;
      expect(packet.type, SocketPacketType.connect);
      expect(packet.nsp, '/admin');
      expect(packet.data, <String, Object>{'token': 'x'});
    });

    test('decodes connect without payload', () {
      SocketPacket packet = SocketDecoder().add('0')!;
      expect(packet.nsp, '/');
      expect(packet.data, isNull);
    });

    test('decodes events with ack ids', () {
      SocketPacket packet = SocketDecoder().add('2/chat,42["msg",{"a":1}]')!;
      expect(packet.isEvent, isTrue);
      expect(packet.nsp, '/chat');
      expect(packet.id, 42);
      expect(packet.data, <Object>[
        'msg',
        <String, Object>{'a': 1},
      ]);
    });

    test('decodes acks', () {
      SocketPacket packet = SocketDecoder().add('35["ok"]')!;
      expect(packet.isAck, isTrue);
      expect(packet.id, 5);
    });

    test('waits for binary attachments', () {
      SocketDecoder decoder = SocketDecoder();
      expect(
        decoder.add(
          '52-3["up",{"_placeholder":true,"num":1},'
          '{"_placeholder":true,"num":0}]',
        ),
        isNull,
      );
      expect(decoder.add(Uint8List.fromList(<int>[1])), isNull);
      SocketPacket packet = decoder.add(Uint8List.fromList(<int>[2]))!;
      expect(packet.type, SocketPacketType.binaryEvent);
      expect(packet.id, 3);
      expect(packet.data, <Object>[
        'up',
        <int>[2],
        <int>[1],
      ]);
    });

    test('refuses more attachments than maxAttachments', () {
      expect(
        () => SocketDecoder().add('51000000-["x"]'),
        throwsFormatException,
      );
      expect(
        () => SocketDecoder(maxAttachments: 2).add('53-["x"]'),
        throwsFormatException,
      );
      expect(SocketDecoder(maxAttachments: 2).add('52-["x"]'), isNull);
      expect(SocketDecoder().add('510-["x"]'), isNull);
    });

    test('round trips binary through the encoder', () {
      Uint8List bytes = Uint8List.fromList(<int>[0, 255]);
      List<Object> parts = const SocketEncoder().encode(
        SocketPacket(
          type: SocketPacketType.event,
          data: <Object>[
            'e',
            <String, Object>{'b': bytes},
          ],
        ),
      );
      SocketDecoder decoder = SocketDecoder();
      SocketPacket? packet;
      for (Object part in parts) {
        packet = decoder.add(part);
      }
      expect(packet!.data, <Object>[
        'e',
        <String, Object>{'b': bytes},
      ]);
    });

    test('reads the JSON after the namespace and the id', () {
      SocketPacket packet = SocketDecoder().add('2/chat,12["é",{"a":[1]}]')!;
      expect(packet.nsp, '/chat');
      expect(packet.id, 12);
      expect(packet.data, <Object?>[
        'é',
        <String, Object?>{
          'a': <Object?>[1],
        },
      ]);
      expect(
        () => SocketDecoder().add('2/chat,12["a",'),
        throwsFormatException,
      );
    });

    test('rejects bad packets', () {
      expect(() => SocketDecoder().add('9'), throwsFormatException);
      expect(() => SocketDecoder().add('/'), throwsFormatException);
      expect(() => SocketDecoder().add('2{"a":1}'), throwsFormatException);
      expect(() => SocketDecoder().add('2["connect"]'), throwsFormatException);
      expect(() => SocketDecoder().add('2[]'), throwsFormatException);
      expect(() => SocketDecoder().add('0"text"'), throwsFormatException);
      expect(() => SocketDecoder().add('1{}'), throwsFormatException);
      expect(() => SocketDecoder().add('5x-["a"]'), throwsFormatException);
      expect(() => SocketDecoder().add('2[bad json'), throwsFormatException);
      expect(() => SocketDecoder().add(Uint8List(1)), throwsFormatException);
      SocketDecoder waiting = SocketDecoder()
        ..add('51-["a",{"_placeholder":true,"num":0}]');
      expect(() => waiting.add('2["b"]'), throwsFormatException);
      SocketDecoder badNum = SocketDecoder()
        ..add('51-["a",{"_placeholder":true,"num":4}]');
      expect(() => badNum.add(Uint8List(1)), throwsFormatException);
    });

    test('only CONNECT and DISCONNECT may have no data', () {
      expect(SocketDecoder().add('0')!.data, isNull);
      expect(SocketDecoder().add('1/chat,')!.data, isNull);
      for (String text in <String>['2', '20', '2/chat,', '3', '31', '4']) {
        expect(() => SocketDecoder().add(text), throwsFormatException);
      }
      expect(() => SocketDecoder().add('51-'), throwsFormatException);
      expect(() => SocketDecoder().add('61-1'), throwsFormatException);
      // A JSON null is a payload too.
      for (String text in <String>['0null', '0/chat,null', '1null', '1{}']) {
        expect(() => SocketDecoder().add(text), throwsFormatException);
      }
    });

    test('rejects data nested deeper than maxDepth', () {
      String deep(int n) => '${'[' * n}1${']' * n}';
      expect(SocketDecoder().add('2["e",${deep(31)}]'), isNotNull);
      expect(
        () => SocketDecoder().add('2["e",${deep(32)}]'),
        throwsFormatException,
      );
      expect(
        () => SocketDecoder().add('0{"a":${deep(200000)}}'),
        throwsFormatException,
      );
      // Brackets inside strings do not count.
      String brackets = '[' * 100;
      expect(SocketDecoder().add('2["e","\\"$brackets"]'), isNotNull);
      // A string that ends in an escaped backslash: the brackets after it
      // are real and count.
      expect(
        () => SocketDecoder().add('2["e","\\\\",${deep(32)}]'),
        throwsFormatException,
      );
      expect(SocketDecoder().add('2["e","\\\\",${deep(30)}]'), isNotNull);
    });

    test('counts maps and mixed nesting', () {
      String maps(int n) => '${'{"a":' * n}1${'}' * n}';
      expect(SocketDecoder().add('0${maps(32)}'), isNotNull);
      expect(() => SocketDecoder().add('0${maps(33)}'), throwsFormatException);
      // [{[ ... ]}]: 34 levels with the event list.
      String mixed = '${'[{"a":' * 16}[1]${'}]' * 16}';
      expect(() => SocketDecoder().add('2["e",$mixed]'), throwsFormatException);
      expect(SocketDecoder(maxDepth: 34).add('2["e",$mixed]'), isNotNull);
    });

    test('skips the scan only when the payload is too short', () {
      SocketDecoder decoder = SocketDecoder(maxDepth: 2);
      // 4 characters (2 * maxDepth): skipped, and depth 2 is fine.
      expect(decoder.add('31[[]]'), isNotNull);
      // 6 characters: scanned, and depth 3 is refused.
      expect(() => decoder.add('31[[[]]]'), throwsFormatException);
      expect(SocketDecoder(maxDepth: 3).add('31[[[]]]'), isNotNull);
    });

    test('maxDepth below 1 is refused', () {
      expect(() => SocketDecoder(maxDepth: 0), throwsArgumentError);
      expect(() => SocketDecoder(maxDepth: -1), throwsArgumentError);
      expect(
        () => SocketIoServer(options: ServerOptions(maxDepth: 0)),
        throwsArgumentError,
      );
    });

    test('maxDepth can be raised', () {
      String deep = '${'[' * 39}1${']' * 39}';
      // 40 levels with the event list.
      expect(() => SocketDecoder().add('2["e",$deep]'), throwsFormatException);
      expect(SocketDecoder(maxDepth: 64).add('2["e",$deep]'), isNotNull);
    });
  });

  group('IdGenerator', () {
    test('gives 20 url-safe characters', () {
      IdGenerator ids = IdGenerator();
      Set<String> seen = <String>{};
      for (int i = 0; i < 100; i++) {
        String id = ids.next();
        expect(id, matches(RegExp(r'^[A-Za-z0-9_-]{20}$')));
        seen.add(id);
      }
      expect(seen, hasLength(100));
    });

    test('uses 4 random calls per id', () {
      _CountingRandom random = _CountingRandom();
      IdGenerator(random).next();
      expect(random.calls, 4);
    });
  });
}

class _CountingRandom implements Random {
  final Random _inner = Random(1);
  int calls = 0;

  @override
  int nextInt(int max) {
    calls++;
    return _inner.nextInt(max);
  }

  @override
  bool nextBool() => _inner.nextBool();

  @override
  double nextDouble() => _inner.nextDouble();
}
