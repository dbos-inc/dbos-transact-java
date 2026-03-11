package dev.dbos.transact.database;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.Workflow;

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

  private final DBOS.Instance dbos;

  public MetricsServiceImpl(DBOS.Instance dbos) {
    this.dbos = dbos;
  }

  @Override
  @Workflow
  public String testWorkflowA() {
    dbos.runStep(() -> "x", "testStepX");
    dbos.runStep(() -> "x", "testStepX");
    return "a";
  }

  @Override
  @Workflow
  public String testWorkflowB() {
    dbos.runStep(() -> "y", "testStepY");
    return "b";
  }
}

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
@org.junit.jupiter.api.parallel.Execution(org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT)
public class MetricsTest {

  final PgContainer pgContainer = new PgContainer();

  @BeforeEach
  void beforeEach() {
    pgContainer.start();
  }

  @AfterEach
  void afterEach() {
    pgContainer.stop();
  }

  @Test
  public void testGetMetrics() throws Exception {

    var dbosConfig = pgContainer.dbosConfig();
    var dbos = new DBOS.Instance(dbosConfig);
    var proxy = dbos.registerWorkflows(MetricsService.class, new MetricsServiceImpl(dbos));
    dbos.launch();

    try {
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
      var sysdb = DBOSTestAccess.getSystemDatabase(dbos);
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
    } finally {
      dbos.shutdown();
    }
  }
}
