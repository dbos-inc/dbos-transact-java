package dev.dbos.transact.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.Workflow;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.regex.Pattern;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A replayed step error that can't be read back still names its class in the workflow's outcome.
 */
class UnreadableErrorReplayTest {

  public static class ReplayedStepException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public ReplayedStepException(String message) {
      super(message);
    }
  }

  public interface FailingService {
    String failAtStep();
  }

  public static class FailingServiceImpl implements FailingService {
    private final DBOS dbos;

    FailingServiceImpl(DBOS dbos) {
      this.dbos = dbos;
    }

    @Override
    @Workflow
    public String failAtStep() {
      return dbos.runStep(
          () -> {
            throw new ReplayedStepException("the step failed");
          },
          "failingStep");
    }
  }

  private static final Pattern PAYLOAD = Pattern.compile("\"base64bytes\":\"([^\"]+)\"");

  @AutoClose final PgContainer pgContainer = new PgContainer();
  @AutoClose DBOS dbos;
  @AutoClose HikariDataSource dataSource;
  FailingService proxy;

  @BeforeEach
  void beforeEach() {
    dataSource = pgContainer.dataSource();
  }

  private void launch() {
    dbos = new DBOS(pgContainer.dbosConfig());
    proxy = dbos.registerProxy(FailingService.class, new FailingServiceImpl(dbos));
    dbos.launch();
  }

  @Test
  void aWorkflowThatPropagatesAnUnreadableStepErrorRecordsItsClass() throws Exception {
    launch();
    var workflowId = "unreadable-replay";
    try (var _o = new WorkflowOptions(workflowId).setContext()) {
      assertThrows(ReplayedStepException.class, () -> proxy.failAtStep());
    }

    // The step's recorded error now names a class this JVM doesn't have, as after a rename.
    dbos.close();
    var steps = DBUtils.getStepRows(dataSource, workflowId);
    assertEquals(1, steps.size());
    var name = ReplayedStepException.class.getName();
    var missing = name.substring(0, name.length() - 4) + "Lost";
    var matcher = PAYLOAD.matcher(steps.get(0).error());
    assertTrue(matcher.find(), steps.get(0).error());
    var payload =
        new String(Base64.getDecoder().decode(matcher.group(1)), StandardCharsets.ISO_8859_1);
    assertTrue(payload.contains(name));
    var renamed =
        Base64.getEncoder()
            .encodeToString(payload.replace(name, missing).getBytes(StandardCharsets.ISO_8859_1));
    var error =
        steps.get(0).error().substring(0, matcher.start(1))
            + renamed
            + steps.get(0).error().substring(matcher.end(1));
    try (var conn = dataSource.getConnection();
        var stmt =
            conn.prepareStatement(
                "UPDATE dbos.operation_outputs SET error = ? WHERE workflow_uuid = ?")) {
      stmt.setString(1, error);
      stmt.setString(2, workflowId);
      assertEquals(1, stmt.executeUpdate());
    }

    // Recovery replays the step, whose error reads back as a PortableWorkflowException, and the
    // workflow lets it propagate.
    DBUtils.setWorkflowState(dataSource, workflowId, "PENDING");
    launch();
    var thrown =
        assertThrows(
            PortableWorkflowException.class, () -> dbos.retrieveWorkflow(workflowId).getResult());
    assertEquals(name, thrown.getErrorName());
    assertEquals("the step failed", thrown.getMessage());
    assertInstanceOf(ClassNotFoundException.class, thrown.getSuppressed()[0]);

    var status = dbos.getWorkflowStatus(workflowId).orElseThrow();
    assertEquals(name, status.error().className());
    assertEquals("the step failed", status.error().message());
  }
}
