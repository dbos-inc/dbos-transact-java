package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.utils.PgContainer;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

interface DelayClockService {
  String quick();
}

class DelayClockServiceImpl implements DelayClockService {
  @Override
  @Workflow
  public String quick() {
    return "done";
  }
}

/**
 * A delayed workflow is released against the database's clock, so a JVM whose clock disagrees with
 * the database's by far more than the delay still releases it on time.
 *
 * <p>The skew is the database's: a schema ahead of pg_catalog on the connections' search path
 * shadows now() and clock_timestamp() with copies shifted by an hour, which every unqualified call
 * in the SDK's SQL resolves to. Postgres only: CockroachDB does not let a function shadow a
 * builtin.
 */
public class DelayReleaseClockTest {

  private static final String CLOCK_SCHEMA = "dbos_test_skewed_release_clock";
  // The workflow's queue, which this executor does not dequeue, so a released row stays ENQUEUED
  // with the release's own updated_at.
  private static final String QUEUE = "release_clock_queue";
  private static final String LISTENED_QUEUE = "release_clock_other_queue";

  @AutoClose final PgContainer pgContainer = new PgContainer();
  @AutoClose HikariDataSource dataSource;
  @AutoClose DBOS dbos;
  DelayClockService proxy;

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
    dbos = new DBOS(pgContainer.dbosConfig().withDatabaseUrl(url).withListenQueues(LISTENED_QUEUE));
    proxy = dbos.registerProxy(DelayClockService.class, new DelayClockServiceImpl());
    dbos.launch();
    dbos.registerQueue(QUEUE, new QueueOptions());
    dbos.registerQueue(LISTENED_QUEUE, new QueueOptions());

    // The harness really did skew the clock the SDK reads.
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
  void aDelayIsReleasedOnTheDatabaseClock(int skewHours) throws Exception {
    launchSkewed(skewHours);
    var id = UUID.randomUUID().toString();

    // Delayed far beyond the skew either way, then moved to end a second from the database's now.
    dbos.startWorkflow(
        () -> proxy.quick(),
        new StartWorkflowOptions(id).withQueue(QUEUE).withDelay(Duration.ofDays(1)));
    assertEquals(WorkflowState.DELAYED.name(), DBUtils.getWorkflowRow(dataSource, id).status());
    long releaseAt = databaseNowMs() + 1_000;
    long start = System.nanoTime();
    dbos.setWorkflowDelay(id, Instant.ofEpochMilli(releaseAt));

    var row = DBUtils.getWorkflowRow(dataSource, id);
    while (WorkflowState.DELAYED.name().equals(row.status())
        && System.nanoTime() - start < Duration.ofSeconds(15).toNanos()) {
      Thread.sleep(50);
      row = DBUtils.getWorkflowRow(dataSource, id);
    }
    long elapsedMs = Duration.ofNanos(System.nanoTime() - start).toMillis();
    long after = databaseNowMs();

    // Released a second later, not an hour early or late, and stamped on the database's clock.
    assertEquals(WorkflowState.ENQUEUED.name(), row.status());
    assertBetween(900, 10_000, elapsedMs, "release took ms");
    assertBetween(releaseAt, after, row.updatedAt(), "updated_at");
  }
}
