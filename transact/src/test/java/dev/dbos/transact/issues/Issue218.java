package dev.dbos.transact.issues;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.Queue;
import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.WorkflowHandle;
import dev.dbos.transact.workflow.WorkflowState;

import java.util.ArrayList;
import java.util.List;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

interface Issue218Service {

  public void taskWorkflow(int i) throws Exception;

  public void parentParallel() throws Exception;
}

class Issue218ServiceImpl implements Issue218Service {

  private static final Logger logger = LoggerFactory.getLogger(Issue218ServiceImpl.class);

  private final DBOS dbos;
  private final Queue queue;
  private Issue218Service proxy;

  public Issue218ServiceImpl(DBOS dbos, Queue queue) {
    this.dbos = dbos;
    this.queue = queue;
  }

  public void setProxy(Issue218Service proxy) {
    this.proxy = proxy;
  }

  @Workflow(name = "task-workflow")
  public void taskWorkflow(int i) throws Exception {
    logger.info("Task {} started", i);
    Thread.sleep(i * 100);
    logger.info("Task {} completed", i);
  }

  @Workflow(name = "parent-parallel")
  public void parentParallel() throws Exception {
    logger.info("parent-parallel started");
    List<WorkflowHandle<Void, Exception>> handles = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      final int index = i;
      WorkflowHandle<Void, Exception> handle =
          dbos.startWorkflow(
              () -> this.proxy.taskWorkflow(index),
              new StartWorkflowOptions().withQueue(this.queue));
      handles.add(handle);
    }
    logger.info("parent-parallel submitted all child tasks");
    for (WorkflowHandle<Void, Exception> handle : handles) {
      try {
        handle.getResult();
      } catch (Exception e) {
        logger.error("Task failed", e);
        throw e;
      }
    }
    logger.info("parent-parallel completed");
  }
}

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
public class Issue218 {

  @AutoClose final PgContainer pgContainer = new PgContainer();
  final Queue queue = new Queue("test-queue");

  DBOSConfig dbosConfig;
  @AutoClose HikariDataSource dataSource;

  @BeforeEach
  void beforeEach() {
    dbosConfig = pgContainer.dbosConfig();
    dataSource = pgContainer.dataSource();
  }

  @Test
  void issue218() throws Exception {

    String wfid;
    try (var dbos = new DBOS(dbosConfig)) {

      var proxy = register(dbos);
      dbos.launch();

      var handle = dbos.startWorkflow(() -> proxy.parentParallel());
      wfid = handle.workflowId();
    }

    var rows = DBUtils.getWorkflowRows(dataSource);
    for (var row : rows) {
      var expected = row.workflowId().equals(wfid) ? WorkflowState.PENDING : WorkflowState.ENQUEUED;
      assertEquals(expected.name(), row.status());
    }

    var steps = DBUtils.getStepRows(dataSource, wfid);
    for (var step : steps) {
      assertNull(step.output());
      assertNull(step.error());
      assertTrue(step.childWorkflowId().startsWith(wfid));
    }

    try (var dbos = new DBOS(dbosConfig)) {
      register(dbos);
      dbos.launch();

      assertDoesNotThrow(() -> dbos.getResult(wfid));
    }

    rows = DBUtils.getWorkflowRows(dataSource);
    for (var row : rows) {
      assertEquals(WorkflowState.SUCCESS.name(), row.status());
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

  private Issue218Service register(DBOS dbos) {
    dbos.registerQueue(queue);
    var impl = new Issue218ServiceImpl(dbos, queue);
    var proxy = dbos.registerWorkflows(Issue218Service.class, impl);
    impl.setProxy(proxy);
    return proxy;
  }
}
