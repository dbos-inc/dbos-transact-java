package dev.dbos.transact.txstep;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.internal.StepCheckpointStore;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.utils.TxStepOutputRow;
import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.internal.StepResult;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

interface CheckpointService {
  void threeSteps() throws SQLException;
}

class CheckpointServiceImpl implements CheckpointService {
  private final JdbcStepFactory factory;

  CheckpointServiceImpl(JdbcStepFactory factory) {
    this.factory = factory;
  }

  @Override
  @Workflow
  public void threeSteps() throws SQLException {
    for (var name : List.of("one", "two", "three")) {
      factory.txStep((Connection conn) -> {}, name);
    }
  }
}

public class StepCheckpointRegistryTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();

  DBOSConfig dbosConfig;
  @AutoClose DBOS dbos;
  @AutoClose HikariDataSource dataSource;
  // A delete that relied on autocommit would be rolled back when its connection went back here.
  @AutoClose HikariDataSource noAutoCommitDataSource;

  @BeforeEach
  void beforeEach() {
    dbosConfig = pgContainer.dbosConfig();
    dataSource = pgContainer.dataSource();
    var cfg = new HikariConfig();
    cfg.setJdbcUrl(pgContainer.jdbcUrl());
    cfg.setUsername(pgContainer.username());
    cfg.setPassword(pgContainer.password());
    cfg.setAutoCommit(false);
    noAutoCommitDataSource = new HikariDataSource(cfg);
    dbos = new DBOS(dbosConfig);
  }

  private List<Integer> checkpoints(String schema, String workflowId) throws SQLException {
    return DBUtils.getTxStepRows(dataSource, workflowId, schema).stream()
        .map(TxStepOutputRow::stepId)
        .sorted()
        .toList();
  }

  @Test
  public void deletesAWorkflowsCheckpointsFromAStepOn() throws Exception {
    var factory = new JdbcStepFactory(dbos, noAutoCommitDataSource, "registry_jdbc");
    var proxy = dbos.registerProxy(CheckpointService.class, new CheckpointServiceImpl(factory));
    dbos.launch();

    try (var o = new WorkflowOptions("wf-registry").setContext()) {
      proxy.threeSteps();
    }
    assertEquals(List.of(0, 1, 2), checkpoints("registry_jdbc", "wf-registry"));

    var executor = DBOSTestAccess.getDbosExecutor(dbos);
    assertTrue(executor.hasStepCheckpointStores());
    executor.deleteStepCheckpoints("wf-registry", 1);
    assertEquals(List.of(0), checkpoints("registry_jdbc", "wf-registry"));

    // Deleting again is not an error, and 0 deletes the rest.
    executor.deleteStepCheckpoints("wf-registry", 1);
    executor.deleteStepCheckpoints("wf-registry", 0);
    assertEquals(List.of(), checkpoints("registry_jdbc", "wf-registry"));
  }

  @Test
  public void theDefaultDeleteCommits() throws Exception {
    // A factory that inherits PostgresStepFactory's own delete rather than overriding it.
    new MinimalStepFactory(dbos, noAutoCommitDataSource, "registry_default");
    dbos.launch();
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute(
          "INSERT INTO registry_default.tx_step_outputs (workflow_id, step_id) VALUES"
              + " ('wf-default', 0), ('wf-default', 1), ('wf-other', 1)");
    }

    DBOSTestAccess.getDbosExecutor(dbos).deleteStepCheckpoints("wf-default", 1);

    assertEquals(List.of(0), checkpoints("registry_default", "wf-default"));
    assertEquals(List.of(1), checkpoints("registry_default", "wf-other"));
  }

  @Test
  public void aFactoryCreatedAfterLaunchIsRefused() {
    dbos.launch();
    var refused =
        assertThrows(
            IllegalStateException.class,
            () -> new JdbcStepFactory(dbos, dataSource, "registry_late"));
    assertTrue(refused.getMessage().contains("before DBOS is launched"), refused.getMessage());
    assertFalse(DBOSTestAccess.getDbosExecutor(dbos).hasStepCheckpointStores());
  }

  @Test
  public void aStoreRegisteredTwiceHasOneEntry() {
    var deletes = new AtomicInteger();
    var store = new RecordingStore(deletes, null);
    dbos.integration().registerStepCheckpointStore(store);
    dbos.integration().registerStepCheckpointStore(store);
    dbos.launch();

    DBOSTestAccess.getDbosExecutor(dbos).deleteStepCheckpoints("wf", 0);
    assertEquals(1, deletes.get());
  }

  @Test
  public void aFailedDeleteNamesTheWorkflow() {
    dbos.integration()
        .registerStepCheckpointStore(
            new RecordingStore(new AtomicInteger(), new SQLException("datasource down")));
    dbos.launch();

    var thrown =
        assertThrows(
            RuntimeException.class,
            () -> DBOSTestAccess.getDbosExecutor(dbos).deleteStepCheckpoints("wf-down", 0));
    assertTrue(thrown.getMessage().contains("wf-down"), thrown.getMessage());
    assertEquals("datasource down", thrown.getCause().getMessage());
  }

  @Test
  public void anUncheckedFailureNamesTheWorkflowToo() {
    // Jdbi and jOOQ report database errors as unchecked exceptions.
    dbos.integration()
        .registerStepCheckpointStore(
            new RecordingStore(new AtomicInteger(), new IllegalStateException("pool closed")));
    dbos.launch();

    var thrown =
        assertThrows(
            RuntimeException.class,
            () -> DBOSTestAccess.getDbosExecutor(dbos).deleteStepCheckpoints("wf-closed", 0));
    assertTrue(thrown.getMessage().contains("wf-closed"), thrown.getMessage());
    assertEquals("pool closed", thrown.getCause().getMessage());
  }

  @Test
  public void aFailedBatchNamesOnlyTheFirstFewWorkflows() {
    dbos.integration()
        .registerStepCheckpointStore(
            new RecordingStore(new AtomicInteger(), new SQLException("datasource down")));
    dbos.launch();

    var ids = java.util.stream.IntStream.range(0, 12).mapToObj(i -> "wf-" + i).toList();
    var thrown =
        assertThrows(
            RuntimeException.class,
            () -> DBOSTestAccess.getDbosExecutor(dbos).deleteStepCheckpoints(ids));
    var message = thrown.getMessage();
    assertTrue(message.contains("12 workflows"), message);
    assertTrue(message.contains("wf-0") && message.contains("wf-4"), message);
    assertFalse(message.contains("wf-5"), message);
    assertTrue(message.contains("and 7 more"), message);
  }

  @Test
  public void deletesEveryCheckpointOfTheGivenWorkflows() throws Exception {
    var factory = new JdbcStepFactory(dbos, noAutoCommitDataSource, "registry_batch");
    var proxy = dbos.registerProxy(CheckpointService.class, new CheckpointServiceImpl(factory));
    dbos.launch();

    for (var id : List.of("wf-a", "wf-b", "wf-c")) {
      try (var o = new WorkflowOptions(id).setContext()) {
        proxy.threeSteps();
      }
    }

    var executor = DBOSTestAccess.getDbosExecutor(dbos);
    executor.deleteStepCheckpoints(List.of("wf-a", "wf-c", "wf-missing"));
    assertEquals(List.of(), checkpoints("registry_batch", "wf-a"));
    assertEquals(List.of(0, 1, 2), checkpoints("registry_batch", "wf-b"));
    assertEquals(List.of(), checkpoints("registry_batch", "wf-c"));

    executor.deleteStepCheckpoints(List.of());
    assertEquals(List.of(0, 1, 2), checkpoints("registry_batch", "wf-b"));
  }

  @Test
  public void anEmptyBatchDoesNotReachTheDatabase() {
    var opens = new AtomicInteger();
    new MinimalStepFactory(
        dbos,
        () -> {
          opens.incrementAndGet();
          return dataSource.getConnection();
        },
        "registry_empty");
    dbos.launch();
    var opensAfterCreate = opens.get();

    DBOSTestAccess.getDbosExecutor(dbos).deleteStepCheckpoints(List.of());
    assertEquals(opensAfterCreate, opens.get());
  }

  @Test
  public void theDefaultBatchDeleteCommits() throws Exception {
    new MinimalStepFactory(dbos, noAutoCommitDataSource, "registry_default_batch");
    dbos.launch();
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute(
          "INSERT INTO registry_default_batch.tx_step_outputs (workflow_id, step_id) VALUES"
              + " ('wf-a', 0), ('wf-a', 1), ('wf-b', 0), ('wf-c', 0)");
    }

    DBOSTestAccess.getDbosExecutor(dbos).deleteStepCheckpoints(List.of("wf-a", "wf-b"));

    assertEquals(List.of(), checkpoints("registry_default_batch", "wf-a"));
    assertEquals(List.of(), checkpoints("registry_default_batch", "wf-b"));
    assertEquals(List.of(0), checkpoints("registry_default_batch", "wf-c"));
  }

  /** Counts its calls, and fails every one with {@code failure} when it is not null. */
  static class RecordingStore implements StepCheckpointStore {
    private final AtomicInteger calls;
    private final Exception failure;

    RecordingStore(AtomicInteger calls, Exception failure) {
      this.calls = calls;
      this.failure = failure;
    }

    @Override
    public void deleteCheckpoints(String workflowId, int fromStepId) throws SQLException {
      record();
    }

    @Override
    public void deleteCheckpoints(Collection<String> workflowIds) throws SQLException {
      record();
    }

    private void record() throws SQLException {
      calls.incrementAndGet();
      if (failure instanceof SQLException e) {
        throw e;
      }
      if (failure instanceof RuntimeException e) {
        throw e;
      }
    }
  }

  /** The smallest PostgresStepFactory: it runs no steps, and only inherits the default delete. */
  static class MinimalStepFactory extends PostgresStepFactory {
    MinimalStepFactory(DBOS dbos, javax.sql.DataSource dataSource, String schema) {
      this(dbos, dataSource::getConnection, schema);
    }

    MinimalStepFactory(DBOS dbos, ConnectionOpener opener, String schema) {
      super(dbos, schema, null, opener);
    }

    @Override
    protected Optional<StepResult> checkExecution(String workflowId, int stepId, String stepName) {
      return Optional.empty();
    }

    @Override
    protected void recordError(String workflowId, int stepId, Exception exception) {}
  }
}
