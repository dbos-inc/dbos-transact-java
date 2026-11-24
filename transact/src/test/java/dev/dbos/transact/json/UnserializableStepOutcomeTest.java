package dev.dbos.transact.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.Workflow;

import java.util.concurrent.atomic.AtomicInteger;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A step result or error the serializer refuses is recorded as the step's error, so a replay
 * returns that error instead of running the step again, and the first run sees the same error.
 */
class UnserializableStepOutcomeTest {

  /** The serializer refuses it: reading its one property throws. */
  public static class Unwritable {
    public String getValue() {
      throw new IllegalStateException("cannot read the value");
    }
  }

  /** Java serialization refuses it: its payload is not {@link java.io.Serializable}. */
  public static class UnserializableException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    @SuppressWarnings({"unused", "serial"})
    private final Object payload = new Object();

    public UnserializableException(String message) {
      super(message);
    }
  }

  public interface StepService {
    String catchUnwritableResult();

    String catchUnserializableError();
  }

  public static class StepServiceImpl implements StepService {
    private final DBOS dbos;
    final AtomicInteger resultRuns = new AtomicInteger();
    final AtomicInteger errorRuns = new AtomicInteger();

    StepServiceImpl(DBOS dbos) {
      this.dbos = dbos;
    }

    @Override
    @Workflow
    public String catchUnwritableResult() {
      try {
        dbos.runStep(
            () -> {
              resultRuns.incrementAndGet();
              return new Unwritable();
            },
            "unwritableResult");
        return "returned";
      } catch (RuntimeException e) {
        return "caught " + e.getClass().getName() + ": " + e.getMessage();
      } finally {
        dbos.runStep(() -> "after", "afterStep");
      }
    }

    @Override
    @Workflow
    public String catchUnserializableError() {
      try {
        dbos.runStep(
            () -> {
              errorRuns.incrementAndGet();
              throw new UnserializableException("the step failed");
            },
            "unserializableError");
        return "returned";
      } catch (RuntimeException e) {
        return "caught " + e.getClass().getName() + ": " + e.getMessage();
      } finally {
        dbos.runStep(() -> "after", "afterStep");
      }
    }
  }

  @AutoClose final PgContainer pgContainer = new PgContainer();
  @AutoClose DBOS dbos;
  @AutoClose HikariDataSource dataSource;
  StepServiceImpl impl;
  StepService proxy;

  @BeforeEach
  void beforeEach() {
    dataSource = pgContainer.dataSource();
    dbos = new DBOS(pgContainer.dbosConfig());
    impl = new StepServiceImpl(dbos);
    proxy = dbos.registerProxy(StepService.class, impl);
    dbos.launch();
  }

  @Test
  void aResultTheSerializerRefusesIsRecordedAsTheStepsError() throws Exception {
    var workflowId = "unwritable-result";
    String firstRun;
    try (var _o = new WorkflowOptions(workflowId).setContext()) {
      firstRun = proxy.catchUnwritableResult();
    }
    assertTrue(
        firstRun.startsWith(
            "caught java.lang.RuntimeException: Cannot serialize the result of step"
                + " unwritableResult: "),
        firstRun);

    var steps = DBUtils.getStepRows(dataSource, workflowId);
    assertEquals(2, steps.size());
    assertNull(steps.get(0).output());
    assertNotNull(steps.get(0).error());

    // A fork from after the step replays its recorded error: the body takes the same branch, and
    // the step does not run again.
    var fork = dbos.<String, RuntimeException>forkWorkflow(workflowId, 1);
    assertEquals(firstRun, fork.getResult());
    assertEquals(1, impl.resultRuns.get());
  }

  @Test
  void anErrorTheSerializerRefusesIsRecordedAsAStandIn() throws Exception {
    var workflowId = "unserializable-step-error";
    String firstRun;
    try (var _o = new WorkflowOptions(workflowId).setContext()) {
      firstRun = proxy.catchUnserializableError();
    }
    // The body is handed the stand-in, as a replay will be, not the original.
    assertEquals(
        "caught java.lang.RuntimeException: "
            + UnserializableException.class.getName()
            + ": the step failed",
        firstRun);

    var steps = DBUtils.getStepRows(dataSource, workflowId);
    assertEquals(2, steps.size());
    assertNotNull(steps.get(0).error());

    var fork = dbos.<String, RuntimeException>forkWorkflow(workflowId, 1);
    assertEquals(firstRun, fork.getResult());
    assertEquals(1, impl.errorRuns.get());
  }
}
