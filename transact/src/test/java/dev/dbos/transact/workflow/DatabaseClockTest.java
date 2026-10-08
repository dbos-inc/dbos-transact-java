package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.exceptions.DBOSAwaitedWorkflowCancelledException;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.utils.PgContainer;

import java.sql.SQLException;
import java.time.Duration;
import java.util.UUID;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

interface ClockService {
  String quick();

  String sleeper(long sleepMs);
}

class ClockServiceImpl implements ClockService {
  private final DBOS dbos;

  ClockServiceImpl(DBOS dbos) {
    this.dbos = dbos;
  }

  @Override
  @Workflow
  public String quick() {
    return "done";
  }

  @Override
  @Workflow
  public String sleeper(long sleepMs) {
    dbos.sleep(Duration.ofMillis(sleepMs));
    return "woke";
  }
}

/**
 * The SDK sets and checks workflow deadlines on the database's clock, so a JVM whose clock
 * disagrees with the database's by far more than any timeout here still times them right.
 *
 * <p>The skew is the database's: a schema ahead of pg_catalog on the connections' search path
 * shadows now() and clock_timestamp() with copies shifted by an hour, which every unqualified call
 * in the SDK's SQL resolves to. Postgres only: CockroachDB does not let a function shadow a
 * builtin.
 */
public class DatabaseClockTest {

  private static final String CLOCK_SCHEMA = "dbos_test_skewed_clock";
  private static final String QUEUE = "clock_queue";

  @AutoClose final PgContainer pgContainer = new PgContainer();
  @AutoClose HikariDataSource dataSource;
  @AutoClose DBOS dbos;
  ClockService proxy;

  @BeforeEach
  void beforeEach() {
    assumeFalse(PgContainer.USE_COCKROACH_DB, "CockroachDB cannot shadow now()");
    dataSource = pgContainer.dataSource();
  }

  @AfterEach
  void afterEach() throws SQLException {
    if (dataSource != null) {
      try (var conn = dataSource.getConnection();
          var stmt = conn.createStatement()) {
        stmt.execute("DROP SCHEMA IF EXISTS %s CASCADE".formatted(CLOCK_SCHEMA));
      }
    }
  }

  /** Shifts the database's clock {@code skewHours} from the JVM's and launches DBOS on it. */
  private void launchSkewed(int skewHours) throws SQLException {
    var interval = "interval '%d hours'".formatted(skewHours);
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("CREATE SCHEMA IF NOT EXISTS %s".formatted(CLOCK_SCHEMA));
      stmt.execute(
          """
          CREATE OR REPLACE FUNCTION %s.now() RETURNS timestamptz LANGUAGE sql STABLE
          AS $$ SELECT pg_catalog.now() + %s $$
          """
              .formatted(CLOCK_SCHEMA, interval));
      stmt.execute(
          """
          CREATE OR REPLACE FUNCTION %s.clock_timestamp() RETURNS timestamptz LANGUAGE sql VOLATILE
          AS $$ SELECT pg_catalog.clock_timestamp() + %s $$
          """
              .formatted(CLOCK_SCHEMA, interval));
    }
    var url = pgContainer.jdbcUrl() + "?currentSchema=%s,pg_catalog,public".formatted(CLOCK_SCHEMA);
    dbos = new DBOS(pgContainer.dbosConfig().withDatabaseUrl(url));
    proxy = dbos.registerProxy(ClockService.class, new ClockServiceImpl(dbos));
    dbos.launch();
    dbos.registerQueue(QUEUE, new QueueOptions());

    // The shadow clock really is shifted. The stored times each test checks show that the SDK's
    // unqualified now() resolves to it.
    long skewMs = databaseNowMs() - System.currentTimeMillis();
    assertTrue(
        Math.abs(skewMs - Duration.ofHours(skewHours).toMillis()) < 60_000,
        "database clock is %d ms off the JVM's".formatted(skewMs));
  }

  /** The skewed clock the SDK reads, in epoch milliseconds. */
  private long databaseNowMs() throws SQLException {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement();
        var rs =
            stmt.executeQuery(
                "SELECT (EXTRACT(epoch FROM %s.now()) * 1000.0)::bigint".formatted(CLOCK_SCHEMA))) {
      rs.next();
      return rs.getLong(1);
    }
  }

  private static void assertBetween(long low, long high, long actual, String what) {
    assertTrue(
        low <= actual && actual <= high,
        "%s %d not in [%d, %d]".formatted(what, actual, low, high));
  }

  @ParameterizedTest
  @ValueSource(ints = {1, -1})
  void aDirectStartsDeadlineIsOnTheDatabaseClock(int skewHours) throws Exception {
    launchSkewed(skewHours);
    var id = UUID.randomUUID().toString();

    long before = databaseNowMs();
    var handle =
        dbos.startWorkflow(
            () -> proxy.quick(), new StartWorkflowOptions(id).withTimeout(Duration.ofSeconds(30)));
    long after = databaseNowMs();

    // Not cancelled on arrival, which a deadline an hour behind the database's clock would be.
    assertEquals("done", handle.getResult());
    var deadline = DBUtils.getWorkflowRow(dataSource, id).deadlineEpochMs();
    assertBetween(before + 30_000, after + 30_000, deadline, "deadline");
  }

  @ParameterizedTest
  @ValueSource(ints = {1, -1})
  void aTimeoutFiresOnTime(int skewHours) throws Exception {
    launchSkewed(skewHours);
    var id = UUID.randomUUID().toString();

    long start = System.nanoTime();
    var handle =
        dbos.startWorkflow(
            () -> proxy.sleeper(30_000),
            new StartWorkflowOptions(id).withTimeout(Duration.ofSeconds(1)));
    assertThrows(DBOSAwaitedWorkflowCancelledException.class, handle::getResult);
    long elapsedMs = Duration.ofNanos(System.nanoTime() - start).toMillis();

    // Neither at once nor an hour late.
    assertBetween(900, 10_000, elapsedMs, "timeout after ms");
    assertEquals(WorkflowState.CANCELLED, handle.getStatus().status());
  }

  @ParameterizedTest
  @ValueSource(ints = {1, -1})
  void aQueuedDeadlineIsOnTheDatabaseClock(int skewHours) throws Exception {
    launchSkewed(skewHours);
    var id = UUID.randomUUID().toString();

    long before = databaseNowMs();
    var handle =
        dbos.startWorkflow(
            () -> proxy.quick(),
            new StartWorkflowOptions(id).withQueue(QUEUE).withTimeout(Duration.ofSeconds(30)));
    assertEquals("done", handle.getResult());
    long after = databaseNowMs();

    // The timeout runs from the claim, on the database's clock.
    var row = DBUtils.getWorkflowRow(dataSource, id);
    assertBetween(before + 30_000, after + 30_000, row.deadlineEpochMs(), "deadline");
  }

  @ParameterizedTest
  @ValueSource(ints = {1, -1})
  void aQueuedTimeoutFiresOnTime(int skewHours) throws Exception {
    launchSkewed(skewHours);
    var id = UUID.randomUUID().toString();

    long start = System.nanoTime();
    var handle =
        dbos.startWorkflow(
            () -> proxy.sleeper(30_000),
            new StartWorkflowOptions(id).withQueue(QUEUE).withTimeout(Duration.ofSeconds(1)));
    assertThrows(DBOSAwaitedWorkflowCancelledException.class, handle::getResult);
    long elapsedMs = Duration.ofNanos(System.nanoTime() - start).toMillis();

    assertBetween(900, 10_000, elapsedMs, "timeout after ms");
    assertEquals(WorkflowState.CANCELLED, handle.getStatus().status());
  }
}
