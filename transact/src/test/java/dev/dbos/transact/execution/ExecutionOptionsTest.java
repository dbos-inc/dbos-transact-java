package dev.dbos.transact.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class ExecutionOptionsTest {

  @Test
  public void negativePriorityIsRejected() {
    // The public options refuse it too; this is the check for the internal paths that build
    // ExecutionOptions directly.
    assertThrows(IllegalArgumentException.class, () -> withPriority(-1));
    assertEquals(0, withPriority(0).priority());
  }

  private static ExecutionOptions withPriority(int priority) {
    return new ExecutionOptions("wf-id", null, null, "q", null, priority, null, null, null, null);
  }
}
