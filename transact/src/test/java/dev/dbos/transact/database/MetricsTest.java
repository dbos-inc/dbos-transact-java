package dev.dbos.transact.database;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.DbSetupTestBase;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.workflow.Workflow;

import java.sql.SQLException;
import java.time.Instant;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

interface MetricsService {
  String testWorkflowA();

  String testWorkflowB();
}

class MetricsServiceImpl implements MetricsService {

  @Override
  @Workflow
  public String testWorkflowA() {
    DBOS.runStep(() -> "x", "testStepX");
    DBOS.runStep(() -> "x", "testStepX");
    return "a";
  }

  @Override
  @Workflow
  public String testWorkflowB() {
    DBOS.runStep(() -> "y", "testStepY");
    return "b";
  }
}

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
public class MetricsTest extends DbSetupTestBase {
  private MetricsService proxy;

  @BeforeEach
  void beforeEachTest() throws SQLException {
    DBUtils.recreateDB(dbosConfig);
    DBOS.reinitialize(dbosConfig);
    proxy = DBOS.registerWorkflows(MetricsService.class, new MetricsServiceImpl());
    DBOS.launch();
  }

  @AfterEach
  void afterEachTest() throws Exception {
    DBOS.shutdown();
  }

  @Test
  public void testGetMetrics() throws Exception {
    // create some metrics data before the start time
    assertEquals("a", proxy.testWorkflowA());
    assertEquals("b", proxy.testWorkflowB());

    // Record start time before creating workflows
    var start = Instant.now();

    // Execute workflows to create metrics data
    assertEquals("a", proxy.testWorkflowA());
    assertEquals("a", proxy.testWorkflowA());
    assertEquals("b", proxy.testWorkflowB());

    // Record end time after creating workflows
    var end = Instant.now();

    // create some metrics data after the end time
    assertEquals("a", proxy.testWorkflowA());
    assertEquals("b", proxy.testWorkflowB());

    // Query metrics
    var sysdb = DBOSTestAccess.getSystemDatabase();
    var metrics = sysdb.getMetrics(start, end);
    assertEquals(4, metrics.size());

    // Convert to map for easier assertion
    var metricsMap =
        metrics.stream()
            .collect(
                Collectors.toMap(
                    m -> "%s:%s".formatted(m.metricType(), m.metricName()), m -> m.value()));

    // Verify step counts
    assertEquals(2, metricsMap.get("workflow_count:testWorkflowA"));
    assertEquals(1, metricsMap.get("workflow_count:testWorkflowB"));
    assertEquals(4, metricsMap.get("step_count:testStepX"));
    assertEquals(1, metricsMap.get("step_count:testStepY"));
  }
}
