package dev.dbos.transact.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.WorkflowState;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A workflow error the serializer refuses is recorded as a stand-in naming its class and message,
 * so the workflow ends ERROR instead of staying PENDING and running again on every recovery.
 */
class UnserializableWorkflowErrorTest {

  /** Java serialization refuses it: its payload is not {@link java.io.Serializable}. */
  public static class UnserializableException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    @SuppressWarnings({"unused", "serial"})
    private final Object payload = new Object();

    public UnserializableException(String message) {
      super(message);
    }
  }

  /** Serializable, but reading its message throws. */
  public static class BrokenMessageException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    @Override
    public String getMessage() {
      throw new IllegalStateException("no message");
    }
  }

  public interface FailingService {
    String throwUnserializable();

    String throwBrokenMessage();
  }

  public static class FailingServiceImpl implements FailingService {
    final AtomicInteger runs = new AtomicInteger();

    @Override
    @Workflow
    public String throwUnserializable() {
      runs.incrementAndGet();
      throw new UnserializableException("the workflow failed");
    }

    @Override
    @Workflow
    public String throwBrokenMessage() {
      runs.incrementAndGet();
      throw new BrokenMessageException();
    }
  }

  @AutoClose final PgContainer pgContainer = new PgContainer();
  @AutoClose DBOS dbos;
  FailingServiceImpl impl;
  FailingService proxy;

  @BeforeEach
  void beforeEach() {
    dbos = new DBOS(pgContainer.dbosConfig());
    impl = new FailingServiceImpl();
    proxy = dbos.registerProxy(FailingService.class, impl);
    dbos.launch();
  }

  @Test
  void anErrorTheSerializerRefusesIsRecordedAsAStandIn() throws Exception {
    var workflowId = "unserializable-error";
    try (var _o = new WorkflowOptions(workflowId).setContext()) {
      // The caller of the first run gets the workflow's own error.
      assertThrows(UnserializableException.class, () -> proxy.throwUnserializable());
    }

    var expected = UnserializableException.class.getName() + ": the workflow failed";
    var status = dbos.getWorkflowStatus(workflowId).orElseThrow();
    assertEquals(WorkflowState.ERROR, status.status());
    assertEquals(RuntimeException.class.getName(), status.error().className());
    assertEquals(expected, status.error().message());

    // The recorded outcome replays without running the body again.
    var replayed =
        assertThrows(RuntimeException.class, () -> dbos.retrieveWorkflow(workflowId).getResult());
    assertEquals(RuntimeException.class, replayed.getClass());
    assertEquals(expected, replayed.getMessage());
    try (var _o = new WorkflowOptions(workflowId).setContext()) {
      var rerun = assertThrows(RuntimeException.class, () -> proxy.throwUnserializable());
      assertEquals(expected, rerun.getMessage());
    }
    assertEquals(1, impl.runs.get());
  }

  @Test
  void anErrorWhoseMessageThrowsIsRecordedAsAStandIn() throws Exception {
    var workflowId = "broken-message-error";
    try (var _o = new WorkflowOptions(workflowId).setContext()) {
      // Logging the error after it is recorded calls getMessage too, so the caller may see that
      // failure rather than the workflow's error. The recorded outcome is what this checks.
      assertThrows(RuntimeException.class, () -> proxy.throwBrokenMessage());
    }

    var status = dbos.getWorkflowStatus(workflowId).orElseThrow();
    assertEquals(WorkflowState.ERROR, status.status());
    assertEquals(RuntimeException.class.getName(), status.error().className());
    assertEquals(
        BrokenMessageException.class.getName()
            + ": <getMessage threw "
            + IllegalStateException.class.getName()
            + ">",
        status.error().message());
    assertEquals(1, impl.runs.get());
  }
}
