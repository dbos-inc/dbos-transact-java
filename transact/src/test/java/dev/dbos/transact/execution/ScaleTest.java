package dev.dbos.transact.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.WorkflowHandle;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledForJreRange;
import org.junit.jupiter.api.condition.EnabledForJreRange;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.condition.JRE;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

interface ScaleService {
  String workflow(String input);
}

class ScaleServiceImpl implements ScaleService {

  @Override
  @Workflow
  public String workflow(String input) {
    try {
      Thread.sleep(60_000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return input + input;
  }
}

@org.junit.jupiter.api.Timeout(value = 10, unit = java.util.concurrent.TimeUnit.MINUTES)
public class ScaleTest {
  private static final Logger logger = LoggerFactory.getLogger(ScaleTest.class);
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
  @EnabledForJreRange(min = JRE.JAVA_21)
  public void virtualThreadPoolJava21() throws Exception {
    DBOS.launch();

    assertFalse(DBOSTestAccess.getDbosExecutor().usingThreadPoolExecutor());
  }

  @Test
  @DisabledForJreRange(min = JRE.JAVA_21)
  public void threadPoolJava17() throws Exception {
    DBOS.launch();

    assertTrue(DBOSTestAccess.getDbosExecutor().usingThreadPoolExecutor());
  }

  @Test
  @EnabledIfEnvironmentVariable(named = "CI", matches = "true")
  public void scaleTest() throws Exception {
    var service = DBOS.registerWorkflows(ScaleService.class, new ScaleServiceImpl());
    DBOS.launch();

    var usingThreadPoolExecutor = DBOSTestAccess.getDbosExecutor().usingThreadPoolExecutor();
    final int count =
        Runtime.getRuntime().availableProcessors() * (usingThreadPoolExecutor ? 50 : 500) * 4;

    ArrayList<WorkflowHandle<String, RuntimeException>> handles = new ArrayList<>();
    long startTime = System.nanoTime();
    for (var i = 0; i < count; i++) {
      final var msg = "%d".formatted(i);
      var handle = DBOS.startWorkflow(() -> service.workflow(msg));
      handles.add(handle);
    }

    for (var i = 0; i < count; i++) {
      var expected = "%1$d%1$d".formatted(i);
      var handle = handles.get(i);
      assertEquals(expected, handle.getResult());
    }
    long endTime = System.nanoTime();

    logger.info("scaleTest time {}", Duration.ofNanos(endTime - startTime));
  }
}
