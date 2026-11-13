import 'dart:io';

import 'package:dox_annotation/command.dart';
import 'package:dox_annotation/install.dart';

/// `dox socketio:install [--broadcasting]`. `dox add socketio` runs it.
/// It sets up Socket.IO in an app: the config, the provider and the
/// `SOCKETIO_ENABLED` key. `--broadcasting` turns on broadcasting in the
/// config. It never overwrites a file, so it is safe to run twice.
class SocketIoInstallCommand extends Command {
  /// [root] is the app folder (the current folder by default). [runner]
  /// runs `dart`, tests pass a fake one.
  SocketIoInstallCommand({this.root, this.runner});

  final Directory? root;
  final ProcessRunner? runner;

  @override
  String get name => 'socketio:install';

  @override
  String get description => 'Set up Socket.IO in this app';

  @override
  String get usage => 'socketio:install [--broadcasting]';

  @override
  Future<void> run(List<String> args) async {
    bool broadcasting = args.contains('--broadcasting');
    if (args.length > (broadcasting ? 1 : 0)) {
      print('Unknown option. The only option is --broadcasting.');
      exitCode = 1;
      return;
    }
    Installer installer = Installer(root: root, runner: runner);
    String app = installer.appName;

    /// first, so an app without the AppEnv class gets the hint and
    /// nothing else is changed
    installer.addEnvGetter(envGetter);
    if (!installer.ok) {
      installer.printSummary(<String>[
        'lib/config/env.dart has no `abstract class AppEnv`. Run '
            '`dox env:generate` to make it, then run this again.',
      ]);
      return;
    }

    await installer.addDependency('dox_socketio');
    await installer.addDependency('dox_builder', dev: true);
    await installer.addDependency('build_runner', dev: true);

    installer.writeFile('lib/config/socketio.dart', config(app, broadcasting));
    installer.writeFile('lib/providers/socketio_provider.dart', provider(app));
    installer.addEnv('SOCKETIO_ENABLED', 'true');

    await installer.buildRunner();
    installer.printSummary(<String>[
      'Make a namespace (a `NamespaceHandler` class) and add it to '
          '`namespaces` in lib/config/socketio.dart.',
      if (broadcasting)
        'Broadcasting is on. Add `Broadcast.routes(...)` to a router and '
            'write your channel rules. See the broadcasting docs.',
    ]);
  }

  /// the getter of `AppEnv` that `lib/config/socketio.dart` reads
  static const String envGetter =
      '/// serve Socket.IO on the app port\n'
      '  bool get socketioEnabled => false;';

  /// `lib/config/socketio.dart`, the same as the docs show
  static String config(String app, bool broadcasting) {
    return '''
import 'package:dox_socketio/dox_socketio.dart';
import 'package:$app/config/env.dart';

SocketIoConfig socketIoConfig = SocketIoConfig(
  enabled: env.socketioEnabled,
  namespaces: <NamespaceHandler>[],${broadcasting ? '\n  broadcasting: true,' : ''}
);
''';
  }

  /// `lib/providers/socketio_provider.dart`, the same as the docs show
  static String provider(String app) {
    return '''
import 'package:dox_socketio/dox_socketio.dart';
import 'package:$app/config/socketio.dart';

class AppSocketIoProvider extends SocketIoProvider {
  AppSocketIoProvider() : super(socketIoConfig);
}
''';
  }
}
