package dev.dbos.transact.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.WorkflowHandle;

import java.time.Duration;
import java.util.ArrayList;

import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
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

@org.junit.jupiter.api.Timeout(value = 5, unit = java.util.concurrent.TimeUnit.MINUTES)
public class ScaleTest {
  private static final Logger logger = LoggerFactory.getLogger(ScaleTest.class);

  @AutoClose final PgContainer pgContainer = new PgContainer();

  DBOSConfig dbosConfig;

  @BeforeEach
  void setUp() {
    dbosConfig = pgContainer.dbosConfig();
  }

  @Test
  @EnabledIfEnvironmentVariable(named = "SCALE_TEST", matches = "^true$")
  public void scaleTest() throws Exception {
    try (var dbos = new DBOS(dbosConfig)) {
      var service = dbos.registerWorkflows(ScaleService.class, new ScaleServiceImpl());
      dbos.launch();

      var usingThreadPoolExecutor = DBOSTestAccess.getDbosExecutor(dbos).usingThreadPoolExecutor();
      final int count =
          Runtime.getRuntime().availableProcessors() * (usingThreadPoolExecutor ? 50 : 500) * 4;

      ArrayList<WorkflowHandle<String, RuntimeException>> handles = new ArrayList<>();
      long startTime = System.nanoTime();
      for (var i = 0; i < count; i++) {
        final var msg = "%d".formatted(i);
        var handle = dbos.startWorkflow(() -> service.workflow(msg));
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
}
