package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import dev.dbos.transact.utils.WorkflowStatusBuilder;

import java.lang.reflect.Constructor;
import java.lang.reflect.RecordComponent;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class WorkflowStatusTest {

  /**
   * {@code equals} and {@code hashCode} are written by hand, for the {@code Object[]} input, so a
   * component added to the record without being added to them is silently ignored. Each component
   * on its own must tell two statuses apart.
   */
  @Test
  void everyComponentTakesPartInEqualsAndHashCode() throws Exception {
    RecordComponent[] components = WorkflowStatus.class.getRecordComponents();
    Constructor<WorkflowStatus> ctor =
        WorkflowStatus.class.getDeclaredConstructor(
            Arrays.stream(components).map(RecordComponent::getType).toArray(Class<?>[]::new));
    Object[] none = new Object[components.length];
    WorkflowStatus base = ctor.newInstance(none);

    for (int i = 0; i < components.length; i++) {
      Object[] args = none.clone();
      args[i] = sample(components[i].getType());
      WorkflowStatus changed = ctor.newInstance(args);
      String name = components[i].getName();

      assertNotEquals(base, changed, name + " is missing from equals");
      assertNotEquals(base.hashCode(), changed.hashCode(), name + " is missing from hashCode");
      assertEquals(changed, ctor.newInstance(args.clone()), name + " breaks equals");
    }
  }

  @Test
  void statusesDifferingOnlyInApplicationNameAreNotEqual() {
    WorkflowStatus mine = new WorkflowStatusBuilder("wf").applicationName("a").build();
    WorkflowStatus theirs = new WorkflowStatusBuilder("wf").applicationName("b").build();

    assertNotEquals(mine, theirs);
  }

  private static Object sample(Class<?> type) {
    if (type == String.class || type == Object.class) return "x";
    if (type == WorkflowState.class) return WorkflowState.SUCCESS;
    if (type == List.class) return List.of("x");
    if (type == Object[].class) return new Object[] {"x"};
    if (type == ErrorResult.class) return new ErrorResult("C", "m", null, null, null);
    if (type == Instant.class) return Instant.ofEpochSecond(1);
    if (type == Integer.class) return 1;
    if (type == Duration.class) return Duration.ofSeconds(1);
    if (type == Boolean.class) return Boolean.TRUE;
    if (type == Map.class) return Map.of("k", "v");
    throw new IllegalArgumentException("no sample value for " + type);
  }
}
