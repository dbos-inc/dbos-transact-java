package dev.dbos.transact.utils;

import dev.dbos.transact.DBOSClient;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.migrations.MigrationManager;

import java.time.Duration;
import java.util.Objects;

import com.zaxxer.hikari.HikariDataSource;
import org.testcontainers.cockroachdb.CockroachContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * A database for one test.
 *
 * <p>Tests get a container of their own, started from an image whose DBOS schema is already
 * migrated, and it is thrown away when they finish. Nothing is shared, so nothing has to be cleaned
 * up between tests: no truncate, no delete, and no ordering constraint between tests that a shared
 * database would impose.
 *
 * <p>That is worth more on CockroachDB than the container cost it adds. CockroachDB runs every DDL
 * statement as an online schema change, so both halves of the old arrangement were expensive there
 * -- migrating a database cost about a minute, and the {@code TRUNCATE} that recycled one cost
 * roughly a second every time a test asked for a database. A prebaked image removes the first and
 * makes the second unnecessary.
 *
 * <p>How many run at once is not decided here. Each concurrent test holds a container, so the
 * ceiling is JUnit's parallelism, which {@link ResourceAwareParallelExecutionConfigurationStrategy}
 * sizes from the memory the machine actually has free.
 *
 * <p><b>Tests that must start from an empty server use {@link #createFresh()} instead</b>, which
 * gives them a stock image with no DBOS schema in it. Migration tests are the obvious case: they
 * exist to watch a schema being built, so handing them one already built would test nothing.
 */
public class PgContainer implements AutoCloseable {

  public static final boolean USE_COCKROACH_DB =
      Boolean.parseBoolean(System.getenv("DBOS_TEST_USE_COCKROACH_DB"));

  /**
   * The database a prebaked image already carries, migrated to the current schema. The images ship
   * several so that a suite pooling databases within one server can hand out more than one; taking
   * a container per test, this suite only ever needs the first.
   */
  private static final String PREBAKED_DB_NAME = "dbos_test_0";

  /** What the from-empty path uses, since it has to create its own database anyway. */
  private static final String FRESH_DB_NAME = "dbos_test_db";

  /** Where a prebaked CockroachDB image keeps its store, which is not the default location. */
  private static final String CRDB_STORE = "/cockroach/prebuilt";

  private static String image(String variable, String fallback) {
    var value = System.getenv(variable);
    return value == null || value.isBlank() ? fallback : value;
  }

  // Pinned to a migration version rather than floating, so a migration landing in dbos-ctl cannot
  // change what this suite runs against without a commit here saying so.
  private static final String PG_IMAGE = image("DBOS_TEST_POSTGRES_IMAGE", "postgres:18");
  private static final String CRDB_IMAGE =
      image("DBOS_TEST_COCKROACH_IMAGE", "cockroachdb/cockroach:latest-v26.2");
  private static final String PG_PREBAKED_IMAGE =
      image("DBOS_TEST_POSTGRES_PREBAKED_IMAGE", "ghcr.io/dbos-inc/dbos-test-postgres:18-m108");
  private static final String CRDB_PREBAKED_IMAGE =
      image("DBOS_TEST_COCKROACH_PREBAKED_IMAGE", "ghcr.io/dbos-inc/dbos-test-cockroach:26.2-m108");

  /** A stock PostgreSQL server with no DBOS schema. */
  public static PostgreSQLContainer getPG() {
    return new PostgreSQLContainer(PG_IMAGE);
  }

  /** A stock CockroachDB server with no DBOS schema. */
  public static CockroachContainer getCRDB() {
    return new CockroachContainer(CRDB_IMAGE);
  }

  /**
   * A PostgreSQL server whose schema is already migrated.
   *
   * <p>The wait strategy has to be replaced rather than inherited. The stock one waits for
   * "database system is ready to accept connections" <i>twice</i>, because a container that
   * initialises itself starts the server once to run initdb and again to serve. This image arrives
   * with its cluster already initialised, so the entrypoint skips that first pass and the message
   * appears once -- against the stock strategy, every container would sit there until the startup
   * timeout and then fail, having been ready the whole time.
   *
   * <p>The user has to be {@code postgres} for the same reason. {@code POSTGRES_USER} only takes
   * effect during initdb, so a container that skips it has whatever roles the image was built with.
   */
  private static PostgreSQLContainer prebakedPG() {
    return new PostgreSQLContainer(
            DockerImageName.parse(PG_PREBAKED_IMAGE).asCompatibleSubstituteFor("postgres"))
        .withDatabaseName(PREBAKED_DB_NAME)
        .withUsername("postgres")
        .withPassword("dbos")
        .waitingFor(
            Wait.forLogMessage(".*database system is ready to accept connections.*\\s", 1)
                .withStartupTimeout(Duration.ofMinutes(2)));
  }

  /**
   * A CockroachDB server whose schema is already migrated.
   *
   * <p>The command has to name the store. A prebaked image keeps its baked data outside the default
   * {@code cockroach-data} directory precisely so that a container which forgets to ask for it
   * starts an empty node rather than silently appearing to work, and the stock command does not
   * ask.
   *
   * <p>No password is set, and that is load-bearing: {@code CockroachContainer.configure()}
   * replaces the command with a bare {@code start-single-node} when a password is present, which
   * would drop both the store path and {@code --insecure}. Insecure is what the rest of the suite
   * already uses.
   */
  private static CockroachContainer prebakedCRDB() {
    return new CockroachContainer(
            DockerImageName.parse(CRDB_PREBAKED_IMAGE)
                .asCompatibleSubstituteFor("cockroachdb/cockroach"))
        .withDatabaseName(PREBAKED_DB_NAME)
        .withCommand("start-single-node", "--insecure", "--store=path=" + CRDB_STORE);
  }

  private static JdbcDatabaseContainer<?> start(boolean prebaked) {
    JdbcDatabaseContainer<?> container;
    if (prebaked) {
      container = USE_COCKROACH_DB ? prebakedCRDB() : prebakedPG();
    } else {
      container = USE_COCKROACH_DB ? getCRDB() : getPG();
    }
    container.start();
    return container;
  }

  private final JdbcDatabaseContainer<?> pgContainer;
  private final String jdbcUrl;

  /** A database with the DBOS schema already in it. */
  public PgContainer() {
    this(false);
  }

  private PgContainer(boolean requireFresh) {
    pgContainer = start(!requireFresh);
    var database = requireFresh ? FRESH_DB_NAME : PREBAKED_DB_NAME;
    jdbcUrl = pgContainer.getJdbcUrl().replaceFirst("/[^/]+$", "/" + database);
  }

  /**
   * A server with no DBOS schema, for tests that have to watch one being created.
   *
   * <p>These are left on the stock image deliberately. A prebaked image would defeat them.
   */
  public static PgContainer createFresh() {
    return new PgContainer(true);
  }

  @Override
  public void close() throws Exception {
    pgContainer.close();
  }

  public String jdbcUrl() {
    return jdbcUrl;
  }

  public String username() {
    return pgContainer.getUsername();
  }

  public String password() {
    return pgContainer.getPassword();
  }

  public DBOSConfig dbosConfig() {
    return dbosConfig(null);
  }

  public DBOSConfig dbosConfig(String appName) {
    return DBOSConfig.defaults(Objects.requireNonNullElse(appName, "transact-java-test"))
        .withDatabaseUrl(jdbcUrl())
        .withDbUser(username())
        .withDbPassword(password());
  }

  public HikariDataSource dataSource() {
    return SystemDatabase.createDataSource(jdbcUrl(), username(), password());
  }

  public DBOSClient dbosClient() {
    return new DBOSClient(jdbcUrl(), username(), password());
  }

  /** A client acting for a named application, for tests where several share this database. */
  public DBOSClient dbosClient(String applicationName) {
    return new DBOSClient(jdbcUrl(), username(), password(), null, null, true, applicationName);
  }

  /** No-op on a prebaked container, whose database the image already created. */
  public void createDatabase() {
    MigrationManager.createDatabaseIfNotExists(jdbcUrl(), username(), password());
  }
}
