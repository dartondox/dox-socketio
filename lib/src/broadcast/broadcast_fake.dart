import 'broadcast.dart';

/// An event that `Broadcast.event()` got while `Broadcast.fake()` was on.
class BroadcastedEvent {
  const BroadcastedEvent(this.event, {this.toOthers});

  /// The event, as it was sent.
  final ShouldBroadcast event;

  /// The socket id of `toOthers`, or null.
  final String? toOthers;

  /// The channel names of the event.
  List<String> get channels => event.broadcastOn().map(_name).toList();

  static String _name(Channel channel) => channel.name;
}

/// Keeps every sent event in [broadcasted] and sends nothing. For tests.
/// `Broadcast.fake()` returns it.
///
/// A failed assert throws an [AssertionError].
class BroadcastFake {
  /// Every sent event, oldest first.
  final List<BroadcastedEvent> broadcasted = <BroadcastedEvent>[];

  /// The sent events of type [T].
  List<T> eventsOf<T extends ShouldBroadcast>() {
    return broadcasted.map(_event).whereType<T>().toList();
  }

  /// An event of type [T] was sent. With [test], one of them must pass it.
  void assertBroadcasted<T extends ShouldBroadcast>([
    bool Function(T event)? test,
  ]) {
    List<T> events = eventsOf<T>();
    if (events.isEmpty) {
      throw AssertionError('Expected $T to be broadcast. ${_summary()}');
    }
    if (test != null && !events.any(test)) {
      throw AssertionError(
        'Expected $T that passes the test to be broadcast, but none of '
        'the ${events.length} sent did.',
      );
    }
  }

  /// An event of type [T] was sent on the channel [name] (with its
  /// `private-` or `presence-` prefix).
  void assertBroadcastedOn<T extends ShouldBroadcast>(String name) {
    for (BroadcastedEvent sent in broadcasted) {
      if (sent.event is T && sent.channels.contains(name)) return;
    }
    throw AssertionError('Expected $T to be broadcast on $name. ${_summary()}');
  }

  /// Exactly [times] events of type [T] were sent.
  void assertBroadcastedTimes<T extends ShouldBroadcast>(int times) {
    int count = eventsOf<T>().length;
    if (count != times) {
      throw AssertionError(
        'Expected $T to be broadcast $times times, but it was sent $count '
        'times.',
      );
    }
  }

  /// No event of type [T] was sent.
  void assertNotBroadcasted<T extends ShouldBroadcast>() {
    int count = eventsOf<T>().length;
    if (count > 0) {
      throw AssertionError(
        'Expected no $T to be broadcast, but it was sent $count times.',
      );
    }
  }

  /// No event was sent at all.
  void assertNothingBroadcasted() {
    if (broadcasted.isNotEmpty) {
      throw AssertionError('Expected no events to be broadcast. ${_summary()}');
    }
  }

  /// Forgets the sent events.
  void clear() => broadcasted.clear();

  String _summary() {
    if (broadcasted.isEmpty) return 'No events were broadcast.';
    return 'Broadcast: ${broadcasted.map(_typeName).join(', ')}.';
  }

  static ShouldBroadcast _event(BroadcastedEvent sent) => sent.event;

  static String _typeName(BroadcastedEvent sent) => '${sent.event.runtimeType}';
}
