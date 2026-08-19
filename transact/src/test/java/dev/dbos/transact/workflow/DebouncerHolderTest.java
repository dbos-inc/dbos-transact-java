package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * The deduplication index is global across the applications sharing a system database, so a
 * debouncer has to decide what a holder's owner means for it, and has to keep reading steps a
 * previous version recorded.
 */
public class DebouncerHolderTest {

  // ==================== Ownership ====================

  @Test
  void aPeersHolderIsForeign() {
    var holder = new DeduplicationHolder("wf-1", "app-b");

    assertTrue(holder.isForeignTo("app-a"));
    assertFalse(holder.isForeignTo("app-b"));
  }

  @Test
  void anUnclaimedHolderBelongsToEveryone() {
    var holder = new DeduplicationHolder("wf-1", null);

    assertFalse(holder.isForeignTo("app-a"));
  }

  @Test
  void aNamelessCallerIsShutOutOfNothing() {
    var holder = new DeduplicationHolder("wf-1", "app-b");

    assertFalse(holder.isForeignTo(null));
  }

  // ==================== Replay of a step recorded before application names ====================

  /**
   * That version recorded the holder's workflow id on its own. Such a replay only happens when the
   * application version is pinned across the upgrade -- patching mode pins it -- since the SDK
   * version is otherwise hashed into the computed version, and recovery only claims workflows
   * matching it.
   */
  @Test
  void adaptsAWorkflowIdRecordedBeforeApplicationNames() {
    var holder = Debouncer.toDeduplicationHolder("wf-123");

    assertEquals("wf-123", holder.workflowId());
    // Recorded before ownership existed, so it is reported unclaimed -- which is also how every
    // application sharing the system database treated it at the time.
    assertNull(holder.applicationName());
    assertFalse(holder.isForeignTo("app-a"));
  }

  @Test
  void passesThroughAHolderRecordedByThisVersion() {
    var recorded = new DeduplicationHolder("wf-456", "app-a");

    assertSame(recorded, Debouncer.toDeduplicationHolder(recorded));
  }

  @Test
  void keepsNullMeaningTheKeyIsUnheld() {
    assertNull(Debouncer.toDeduplicationHolder(null));
  }

  @Test
  void rejectsAShapeItCannotAdapt() {
    var e = assertThrows(IllegalStateException.class, () -> Debouncer.toDeduplicationHolder(42));

    assertTrue(e.getMessage().contains("java.lang.Integer"));
  }
}
