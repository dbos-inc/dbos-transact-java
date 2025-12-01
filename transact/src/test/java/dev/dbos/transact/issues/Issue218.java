package dev.dbos.transact.issues;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DbSetupTestBase;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.workflow.Queue;
import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.WorkflowHandle;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

interface Example {

  public void taskWorkflow(int i) throws Exception;

  public void parentParallel() throws Exception;
}

class ExampleImpl implements Example {

  private final Queue queue;
  private Example proxy;

  public ExampleImpl(Queue queue) {
    this.queue = queue;
  }

  public void setProxy(Example proxy) {
    this.proxy = proxy;
  }

  @Workflow(name = "task-workflow")
  public void taskWorkflow(int i) throws Exception {
    System.out.printf("Task %d started%n", i);
    Thread.sleep(i * 100);
    System.out.printf("Task %d completed%n", i);
  }

  @Workflow(name = "parent-parallel")
  public void parentParallel() throws Exception {
    System.out.println("parent-parallel started");
    List<WorkflowHandle<Void, Exception>> handles = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      final int index = i;
      WorkflowHandle<Void, Exception> handle =
          DBOS.startWorkflow(
              () -> this.proxy.taskWorkflow(index),
              new StartWorkflowOptions().withQueue(this.queue));
      handles.add(handle);
    }
    System.out.println("parent-parallel submitted all child tasks");
    for (WorkflowHandle<Void, Exception> handle : handles) {
      try {
        handle.getResult();
      } catch (Exception e) {
        System.out.println("Task failed " + e);
        throw e;
      }
    }
    System.out.println("parent-parallel completed");
  }
}

// @org.junit.jupiter.api.Timeout(value = 2, unit = TimeUnit.MINUTES)
public class Issue218 extends DbSetupTestBase {
  private HikariDataSource dataSource;

  @BeforeEach
  void beforeEachTest() throws SQLException {
    DBUtils.recreateDB(dbosConfig);
    DBOS.reinitialize(dbosConfig);
    dataSource = SystemDatabase.createDataSource(dbosConfig);
  }

  @AfterEach
  void afterEachTest() throws Exception {
    DBOS.shutdown();
  }

  @Test
  void issue218() throws Exception {

    var queue = new Queue("test-queue");
    DBOS.registerQueue(queue);
    var impl = new ExampleImpl(queue);
    Example proxy = DBOS.registerWorkflows(Example.class, impl);
    impl.setProxy(proxy);

    DBOS.launch();

    var handle = DBOS.startWorkflow(() -> proxy.parentParallel());
    var wfid = handle.workflowId();
    DBOS.shutdown();

    var rows = DBUtils.getWorkflowRows(dataSource);
    for (var row : rows) {
      var expected = row.workflowId().equals(wfid) ? "PENDING" : "ENQUEUED";
      assertEquals(expected, row.status());
    }

    var steps = DBUtils.getStepRows(dataSource, wfid);
    for (var step : steps) {
      assertNull(step.output());
      assertNull(step.error());
      assertTrue(step.childWorkflowId().startsWith(wfid));
    }

    DBOS.launch();
    assertDoesNotThrow(() -> DBOS.getResult(wfid));

    rows = DBUtils.getWorkflowRows(dataSource);
    for (var row : rows) {
      assertEquals("SUCCESS", row.status());
    }

    steps = DBUtils.getStepRows(dataSource, wfid);
    for (var step : steps) {
      assertTrue(
          step.functionName().equals("task-workflow")
              || step.functionName().equals("DBOS.getResult"));
      assertNull(step.error());
      assertTrue(step.childWorkflowId().startsWith(wfid));
      if (step.functionName().equals("task-workflow")) {
        assertNull(step.output());
        assertNull(step.startedAt());
        assertNull(step.completedAt());
      }
      if (step.functionName().equals("DBOS.getResult")) {
        assertNotNull(step.output());
        assertNotNull(step.startedAt());
        assertNotNull(step.completedAt());
      }
    }
  }
}
