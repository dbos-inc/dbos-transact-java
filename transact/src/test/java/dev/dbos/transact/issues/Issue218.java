package dev.dbos.transact.issues;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.workflow.Queue;
import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.WorkflowHandle;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
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
      }
    }
    System.out.println("parent-parallel completed");
  }
}

// @org.junit.jupiter.api.Timeout(value = 2, unit = TimeUnit.MINUTES)
public class Issue218 {
  private static DBOSConfig dbosConfig;

  @BeforeAll
  static void onetimeSetup() throws Exception {
    dbosConfig =
        DBOSConfig.defaultsFromEnv("systemdbtest")
            .withDatabaseUrl("jdbc:postgresql://localhost:5432/dbos_java_sys")
            .withMaximumPoolSize(2);
  }

  @BeforeEach
  void beforeEachTest() throws SQLException {
    DBUtils.recreateDB(dbosConfig);
    DBOS.reinitialize(dbosConfig);
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
    DBOSTestAccess.getQueueService().pause();

    var handle = DBOS.startWorkflow(() -> proxy.parentParallel());
    Thread.sleep(500);
    DBOS.shutdown();

    DBOS.launch();
    var result = handle.getResult();
  }
}
