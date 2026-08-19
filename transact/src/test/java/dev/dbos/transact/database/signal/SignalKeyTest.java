package dev.dbos.transact.database.signal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.junit.jupiter.api.Test;

class SignalKeyTest {

  @Test
  void aKeyNamesTheSameThingLocallyAndOffTheWire() {
    // A waiter is woken either by the writer in this process, which raises key.toString(), or by a
    // notification the writer pushed, which the listener turns back into a key with signalFor. If
    // those two ever disagree, local wake-ups keep working while cross-process ones silently stop
    // and every remote waiter quietly falls back to its poll.
    List<SignalKey> keys =
        List.of(
            new SignalKey.Event("wf", "key"),
            new SignalKey.Message("wf", "topic"),
            new SignalKey.Stream("wf", "key"));

    for (var key : keys) {
      assertEquals(
          key.toString(),
          SignalKey.signalFor(key.channel(), key.payload()),
          "%s must name the same waiter whichever direction it arrives from"
              .formatted(key.getClass().getSimpleName()));
    }
  }

  @Test
  void everyKeyTypeHasItsOwnChannelAndPrefix() {
    assertEquals(SignalKey.WORKFLOW_EVENTS_CHANNEL, new SignalKey.Event("wf", "k").channel());
    assertEquals(SignalKey.NOTIFICATIONS_CHANNEL, new SignalKey.Message("wf", "t").channel());
    assertEquals(SignalKey.STREAMS_CHANNEL, new SignalKey.Stream("wf", "k").channel());

    assertEquals("e::wf::k", new SignalKey.Event("wf", "k").toString());
    assertEquals("m::wf::t", new SignalKey.Message("wf", "t").toString());
    assertEquals("s::wf::k", new SignalKey.Stream("wf", "k").toString());
  }

  @Test
  void aNotificationOnAChannelWeDoNotOwnIsRejected() {
    assertThrows(
        IllegalArgumentException.class, () -> SignalKey.signalFor("some_other_channel", "wf::k"));
  }
}
