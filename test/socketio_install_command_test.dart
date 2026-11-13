import 'dart:io';

import 'package:dox_annotation/install.dart';
import 'package:dox_socketio/commands.dart';
import 'package:test/test.dart';

/// records the `dart` commands, runs nothing
class FakeRunner extends ProcessRunner {
  final List<String> calls = <String>[];

  @override
  Future<int> run(String executable, List<String> args, String folder) async {
    calls.add(args.join(' '));
    return 0;
  }
}

const String envDart = '''
abstract class AppEnv {
  // my comment {
  String get appName => 'dox';
}
''';

void main() {
  late Directory root;
  late FakeRunner runner;

  setUp(() {
    root = Directory.systemTemp.createTempSync('dox_socketio_install_');
    File(
      '${root.path}/pubspec.yaml',
    ).writeAsStringSync('name: my_app\n\ndependencies:\n  dox_core: ^3.1.0\n');
    File('${root.path}/.env').writeAsStringSync('APP_KEY=x\n');
    File('${root.path}/lib/config/env.dart')
      ..createSync(recursive: true)
      ..writeAsStringSync(envDart);
    runner = FakeRunner();
    exitCode = 0;
  });

  tearDown(() {
    root.deleteSync(recursive: true);
  });

  String read(String path) => File('${root.path}/$path').readAsStringSync();

  Future<void> install(List<String> args) {
    return SocketIoInstallCommand(root: root, runner: runner).run(args);
  }

  /// every file of the app and its text
  Map<String, String> snapshot() {
    Map<String, String> files = <String, String>{};
    for (FileSystemEntity f in root.listSync(recursive: true)) {
      if (f is File) files[f.path] = f.readAsStringSync();
    }
    return files;
  }

  test('sets up Socket.IO with no options', () async {
    await install(<String>[]);
    expect(exitCode, 0);
    expect(runner.calls, <String>[
      'pub add dox_socketio',
      'pub add --dev dox_builder',
      'pub add --dev build_runner',
      'run build_runner build',
    ]);
    String config = read('lib/config/socketio.dart');
    expect(config, contains("import 'package:my_app/config/env.dart';"));
    expect(config, contains('enabled: env.socketioEnabled,'));
    expect(config, isNot(contains('broadcasting')));
    expect(
      read('lib/providers/socketio_provider.dart'),
      contains('AppSocketIoProvider'),
    );
    expect(read('.env'), contains('SOCKETIO_ENABLED=true\n'));
    expect(
      read('lib/config/env.dart'),
      contains('bool get socketioEnabled => false;'),
    );
  });

  test('--broadcasting turns on broadcasting in the config', () async {
    await install(<String>['--broadcasting']);
    expect(exitCode, 0);
    expect(read('lib/config/socketio.dart'), contains('broadcasting: true,'));
  });

  test('a second run changes nothing', () async {
    await install(<String>[]);
    File('${root.path}/lib/config/socketio.dart').writeAsStringSync('mine');
    Map<String, String> before = snapshot();
    await install(<String>['--broadcasting']);
    expect(exitCode, 0);
    expect(snapshot(), before);
  });

  test('--broadcasting with an existing config says it was skipped', () async {
    File('${root.path}/lib/config/socketio.dart')
      ..createSync(recursive: true)
      ..writeAsStringSync('mine');
    await install(<String>['--broadcasting']);
    expect(read('lib/config/socketio.dart'), 'mine');
    expect(exitCode, 0);
  });

  test('a bad option stops with exit code 1 and writes nothing', () async {
    for (List<String> args in <List<String>>[
      <String>['--broadcasting=true'],
      <String>['--broadcast'],
      <String>['broadcasting'],
      <String>['--broadcasting', 'true'],
      <String>['--broadcasting', '--broadcasting'],
    ]) {
      Map<String, String> before = snapshot();
      await install(args);
      expect(exitCode, 1, reason: '$args');
      expect(runner.calls, isEmpty, reason: '$args');
      expect(snapshot(), before, reason: '$args');
      exitCode = 0;
    }
  });

  test('AppEnv comments stay the same', () async {
    await install(<String>[]);
    expect(read('lib/config/env.dart'), startsWith(envDart.split('}\n')[0]));
  });

  test(
    'without AppEnv it gives a hint, exit code 1, and changes nothing',
    () async {
      for (String? env in <String?>[null, 'class Other {\n}\n']) {
        File file = File('${root.path}/lib/config/env.dart');
        if (env == null) {
          file.deleteSync();
        } else {
          file.writeAsStringSync(env);
        }
        Map<String, String> before = snapshot();
        await install(<String>[]);
        expect(exitCode, 1);
        expect(runner.calls, isEmpty);
        expect(snapshot(), before);
        exitCode = 0;
      }
    },
  );
}
