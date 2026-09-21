package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.Constants;
import dev.dbos.transact.json.DBOSPortableSerializer;

import java.util.HashMap;
import java.util.Map;

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

  // ==================== Replay under a serializer that drops Java types ====================

  /**
   * The portable serializer, and any custom JSON one, records the holder as a plain object and
   * hands back a map. The step still has to replay: the ids were never lost, only the type.
   */
  @Test
  void adaptsAHolderRoundTrippedThroughThePortableSerializer() {
    var serializer = DBOSPortableSerializer.INSTANCE;
    var recorded =
        serializer.deserialize(serializer.serialize(new DeduplicationHolder("wf-7", "app-b")));

    var holder = Debouncer.toDeduplicationHolder(recorded);

    assertEquals("wf-7", holder.workflowId());
    assertEquals("app-b", holder.applicationName());
    assertTrue(holder.isForeignTo("app-a"));
  }

  @Test
  void adaptsAnUnclaimedHolderRoundTrippedThroughThePortableSerializer() {
    var serializer = DBOSPortableSerializer.INSTANCE;
    var recorded =
        serializer.deserialize(serializer.serialize(new DeduplicationHolder("wf-8", null)));

    var holder = Debouncer.toDeduplicationHolder(recorded);

    assertEquals("wf-8", holder.workflowId());
    assertNull(holder.applicationName());
  }

  @Test
  void rejectsAMapThatIsNotAHolder() {
    Map<String, Object> notAHolder = new HashMap<>();
    notAHolder.put("something", "else");

    assertThrows(IllegalStateException.class, () -> Debouncer.toDeduplicationHolder(notAHolder));
  }

  @Test
  void rejectsAShapeItCannotAdapt() {
    var e = assertThrows(IllegalStateException.class, () -> Debouncer.toDeduplicationHolder(42));

    assertTrue(e.getMessage().contains("java.lang.Integer"));
  }

  // ==================== Telling the two kinds of holder apart ====================

  @Test
  void aServiceWorkflowHolderIsAService() {
    var holder =
        new DeduplicationHolder(
            "wf-1",
            "app-a",
            Constants.DEBOUNCER_WORKFLOW_NAME,
            Constants.DEBOUNCER_CLASS_NAME,
            null,
            WorkflowState.PENDING,
            false);

    assertTrue(holder.isDebouncerService());
    assertFalse(holder.isDebouncedInstanceOf("process", "com.example.Impl", null));
  }

  @Test
  void aHolderOfUnknownNameCountsAsAService() {
    // Recorded before debounced workflows existed, when nothing else held a debounce key.
    assertTrue(new DeduplicationHolder("wf-1", "app-a").isDebouncerService());
  }

  @Test
  void aDebouncedHolderMatchesItsOwnWorkflowOnly() {
    var holder =
        new DeduplicationHolder(
            "wf-1", "app-a", "process", "com.example.Impl", null, WorkflowState.DELAYED, true);

    assertFalse(holder.isDebouncerService());
    assertTrue(holder.isDebouncedInstanceOf("process", "com.example.Impl", null));
    // An absent instance name is spelled both ways.
    assertTrue(holder.isDebouncedInstanceOf("process", "com.example.Impl", ""));
    assertFalse(holder.isDebouncedInstanceOf("other", "com.example.Impl", null));
    assertFalse(holder.isDebouncedInstanceOf("process", "com.example.Other", null));
    assertFalse(holder.isDebouncedInstanceOf("process", "com.example.Impl", "east"));
  }

  @Test
  void aWorkflowDeduplicatedOnItsOwnAccountIsNeither() {
    var holder =
        new DeduplicationHolder(
            "wf-1", "app-a", "process", "com.example.Impl", null, WorkflowState.ENQUEUED, false);

    assertFalse(holder.isDebouncerService());
    assertFalse(holder.isDebouncedInstanceOf("process", "com.example.Impl", null));
  }

  // ==================== Replay of a lookup step recorded before the bounce ====================

  @Test
  void aRecordedWorkflowIdMeansNothingWasBounced() {
    var result = Debouncer.toDebounceResult("wf-123");

    assertFalse(result.bounced());
    assertEquals("wf-123", result.holder().workflowId());
    assertTrue(result.holder().isDebouncerService());
  }

  @Test
  void aRecordedHolderMeansNothingWasBounced() {
    var result = Debouncer.toDebounceResult(new DeduplicationHolder("wf-456", "app-a"));

    assertFalse(result.bounced());
    assertEquals("wf-456", result.holder().workflowId());
    assertTrue(result.holder().isForeignTo("app-b"));
    assertTrue(result.holder().isDebouncerService());
  }

  @Test
  void aRecordedNullMeansTheKeyWasUnheld() {
    var result = Debouncer.toDebounceResult(null);

    assertFalse(result.bounced());
    assertNull(result.holder());
  }

  @Test
  void passesThroughAResultRecordedByThisVersion() {
    var recorded = new DebounceResult("wf-9", null);

    assertSame(recorded, Debouncer.toDebounceResult(recorded));
  }

  @Test
  void adaptsAResultRoundTrippedThroughThePortableSerializer() {
    var serializer = DBOSPortableSerializer.INSTANCE;
    var holder =
        new DeduplicationHolder(
            "wf-7", "app-b", "process", "com.example.Impl", "east", WorkflowState.DELAYED, true);
    var recorded = serializer.deserialize(serializer.serialize(new DebounceResult(null, holder)));

    var result = Debouncer.toDebounceResult(recorded);

    assertFalse(result.bounced());
    assertEquals(holder, result.holder());
  }

  @Test
  void adaptsABouncedResultRoundTrippedThroughThePortableSerializer() {
    var serializer = DBOSPortableSerializer.INSTANCE;
    var recorded = serializer.deserialize(serializer.serialize(new DebounceResult("wf-10", null)));

    var result = Debouncer.toDebounceResult(recorded);

    assertEquals("wf-10", result.bouncedWorkflowId());
    assertNull(result.holder());
  }

  // ==================== Replay of the first step ====================

  @Test
  void adaptsIdsRecordedBeforeTheBounce() {
    // That version recorded only the two ids; nothing was extended then.
    var recorded = new HashMap<String, Object>();
    recorded.put("userWorkflowId", "user-1");
    recorded.put("messageId", "msg-1");

    var ids = Debouncer.toDebounceIds(recorded);

    assertEquals("user-1", ids.userWorkflowId());
    assertEquals("msg-1", ids.messageId());
    assertNull(ids.bouncedWorkflowId());
  }

  @Test
  void adaptsIdsRoundTrippedThroughThePortableSerializer() {
    var serializer = DBOSPortableSerializer.INSTANCE;
    var recorded =
        serializer.deserialize(
            serializer.serialize(new Debouncer.DebounceIds("user-2", "msg-2", "wf-2")));

    var ids = Debouncer.toDebounceIds(recorded);

    assertEquals("user-2", ids.userWorkflowId());
    assertEquals("wf-2", ids.bouncedWorkflowId());
  }

  @Test
  void rejectsIdsItCannotAdapt() {
    assertThrows(IllegalStateException.class, () -> Debouncer.toDebounceIds("just-a-string"));
  }
}
