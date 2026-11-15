import 'dart:io';

import 'package:dox_core/dox_core.dart';
import 'package:dox_socketio/dox_socketio.dart';
import 'package:dox_socketio/src/engine/origin_check.dart';
import 'package:test/test.dart';

/// Runs the provider setup and returns what it wrote to stderr.
List<String> _setup({required bool webServer}) {
  List<String> lines = <String>[];
  originWarningDone = false;
  sessionLimitWarningDone = false;
  Dox().isWebServer = webServer;
  Dox().isolateId = 1;
  IOOverrides.runZoned(() {
    SocketIoProvider(SocketIoConfig()).setup();
  }, stderr: () => _LineSink(lines));
  return lines;
}

void main() {
  tearDown(() => Dox().isWebServer = false);

  test('setup in the web server prints the warnings once', () {
    List<String> lines = _setup(webServer: true);
    expect(lines, hasLength(2));
    expect(lines.first, contains('Any website can open a socket'));
    expect(lines.last, contains('ServerOptions(maxSessions:) is not set'));
  });

  test('setup in a queue or schedule process prints nothing', () {
    expect(_setup(webServer: false), isEmpty);
  });

  test('setup in the web server prints nothing with allowedOrigins and maxSessions', () {
    List<String> lines = <String>[];
    originWarningDone = false;
    sessionLimitWarningDone = false;
    Dox().isWebServer = true;
    Dox().isolateId = 1;
    IOOverrides.runZoned(() {
      SocketIoProvider(
        SocketIoConfig(
          options: ServerOptions(allowedOrigins: <String>[], maxSessions: 100),
        ),
      ).setup();
    }, stderr: () => _LineSink(lines));
    expect(lines, isEmpty);
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
