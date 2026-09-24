package dev.dbos.transact;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import dev.dbos.transact.workflow.SerializationStrategy;
import dev.dbos.transact.workflow.Timeout;

import java.lang.reflect.RecordComponent;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

/**
 * DBOSClient.EnqueueOptions survives until 2.0 only as a front for the top-level EnqueueOptions, so
 * the two must stay field-for-field identical and the conversion must carry every field.
 */
@SuppressWarnings("removal")
class DeprecatedEnqueueOptionsTest {

  @Test
  void theTwoRecordsDeclareTheSameComponents() {
    assertEquals(
        componentNames(EnqueueOptions.class), componentNames(DBOSClient.EnqueueOptions.class));
  }

  @Test
  void conversionCarriesEveryField() throws Exception {
    var legacy =
        new DBOSClient.EnqueueOptions(
            "wf",
            "cls",
            "inst",
            "q",
            "wfid",
            "v1",
            Duration.ofSeconds(5),
            null,
            "dedup",
            3,
            "part",
            Duration.ofSeconds(7),
            SerializationStrategy.PORTABLE,
            "user",
            "role",
            List.of("r1", "r2"),
            Map.of("k", "v"),
            "peer-app");
    // An explicit timeout and a deadline can't be set together, so the deadline is carried by a
    // second conversion.
    var withDeadline = legacy.withTimeout(null).withDeadline(Instant.parse("2030-01-01T00:00:00Z"));

    assertCarried(legacy, "deadline");
    assertCarried(withDeadline, "timeout");
  }

  /** Every component but {@code unset} is set on {@code legacy} and survives the conversion. */
  private static void assertCarried(DBOSClient.EnqueueOptions legacy, String unset)
      throws Exception {
    var converted = legacy.toEnqueueOptions();
    for (var legacyComponent : DBOSClient.EnqueueOptions.class.getRecordComponents()) {
      var name = legacyComponent.getName();
      if (name.equals(unset)) {
        continue;
      }
      var value = legacyComponent.getAccessor().invoke(legacy);
      assertNotNull(value, name + " should be set by this test");
      // The one component whose type changed: the legacy Duration becomes an explicit Timeout.
      var expected = value instanceof Duration d && name.equals("timeout") ? Timeout.of(d) : value;
      assertEquals(expected, EnqueueOptions.class.getMethod(name).invoke(converted), name);
    }
  }

  private static List<String> componentNames(Class<? extends Record> type) {
    return Arrays.stream(type.getRecordComponents()).map(RecordComponent::getName).toList();
  }
}
