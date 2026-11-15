import 'package:dox_socketio/dox_socketio.dart';
import 'package:test/test.dart';

void main() {
  group('RoomStore', () {
    late RoomStore store;

    setUp(() {
      store = RoomStore()
        ..addAll('a', <String>{'a', 'room1', 'vip'})
        ..addAll('b', <String>{'b', 'room1'})
        ..addAll('c', <String>{'c', 'room2'});
    });

    test('empty rooms means everyone', () {
      expect(store.match(<String>{}, <String>{}), <String>{'a', 'b', 'c'});
    });

    test('rooms are a union', () {
      expect(store.match(<String>{'room1', 'room2'}, <String>{}), <String>{
        'a',
        'b',
        'c',
      });
      expect(store.match(<String>{'room1'}, <String>{}), <String>{'a', 'b'});
    });

    test('except removes sockets in those rooms', () {
      expect(store.match(<String>{'room1'}, <String>{'vip'}), <String>{'b'});
      expect(store.match(<String>{}, <String>{'a'}), <String>{'b', 'c'});
    });

    test('members gives the same sockets as match', () {
      List<Set<String>> cases = <Set<String>>[
        <String>{},
        <String>{'room1'},
        <String>{'room1', 'room2'},
        <String>{'none'},
      ];
      for (Set<String> rooms in cases) {
        for (Set<String> except in <Set<String>>[
          <String>{},
          <String>{'vip'},
        ]) {
          expect(
            store.members(rooms, except).toSet(),
            store.match(rooms, except),
          );
        }
      }
    });

    test('except with many rooms, unknown rooms and the socket id', () {
      store.addAll('d', <String>{'d', 'room1', 'room2'});
      expect(
        store
            .members(<String>{'room1', 'room2'}, <String>{'vip', 'c'})
            .toList(),
        unorderedEquals(<String>['b', 'd']),
      );
      expect(
        store.members(<String>{'room1'}, <String>{'none'}).toSet(),
        <String>{'a', 'b', 'd'},
      );
      expect(
        store.members(<String>{}, <String>{'a', 'room2'}).toSet(),
        <String>{'b'},
      );
    });

    test('members with except makes no copy of the room', () {
      Iterable<String> ids = store.members(<String>{'room1'}, <String>{'vip'});
      expect(ids, isNot(isA<Set<String>>()));
      expect(ids.toList(), <String>['b']);
    });

    test('hasSockets', () {
      expect(store.hasSockets(), isTrue);
      expect(store.hasSockets('room1'), isTrue);
      expect(store.hasSockets('none'), isFalse);
      store
        ..delAll('a')
        ..delAll('b')
        ..delAll('c');
      expect(store.hasSockets(), isFalse);
      expect(store.hasSockets('room1'), isFalse);
    });

    test('unknown rooms match nobody', () {
      expect(store.match(<String>{'none'}, <String>{}), isEmpty);
    });

    test('del and delAll clean up', () {
      store.del('a', 'room1');
      expect(store.socketRooms('a'), <String>{'a', 'vip'});
      expect(store.match(<String>{'room1'}, <String>{}), <String>{'b'});
      store.delAll('b');
      expect(store.match(<String>{'room1'}, <String>{}), isEmpty);
      expect(store.socketRooms('b'), isEmpty);
    });
  });

  group('BroadcastOptions', () {
    test('JSON shape matches the Node.js redis adapter', () {
      BroadcastOptions options = BroadcastOptions(
        rooms: <String>{'r'},
        except: <String>{'x'},
        local: true,
      );
      expect(options.toJson(), <String, Object>{
        'rooms': <String>['r'],
        'except': <String>['x'],
        'flags': <String, Object>{'local': true},
      });
      BroadcastOptions back = BroadcastOptions.fromJson(<String, dynamic>{
        'rooms': <String>['r'],
        'except': <String>[],
        'flags': <String, dynamic>{},
      });
      expect(back.rooms, <String>{'r'});
      expect(back.local, isFalse);
    });
  });
}
