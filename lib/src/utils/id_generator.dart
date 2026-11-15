import 'dart:convert';
import 'dart:math';
import 'dart:typed_data';

/// Makes random ids for sessions and sockets.
///
/// The ids are 20 url-safe base64 characters, like the Node.js server.
class IdGenerator {
  IdGenerator([Random? random]) : _random = random ?? Random.secure();

  final Random _random;

  /// Returns a new random id.
  String next() {
    // 4 calls of 32 bits each give 16 bytes. The id uses 15 of them.
    // ponytail: Random has no "fill bytes" call, so 4 is the fewest.
    Uint8List bytes = Uint8List(16);
    ByteData view = ByteData.sublistView(bytes);
    for (int i = 0; i < 16; i += 4) {
      view.setUint32(i, _random.nextInt(_max32));
    }
    return base64Url.encode(Uint8List.sublistView(bytes, 0, 15));
  }

  /// The biggest bound `nextInt` takes: 2^32.
  static const int _max32 = 0x100000000;
}
