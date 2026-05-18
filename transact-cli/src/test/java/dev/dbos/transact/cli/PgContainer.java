package dev.dbos.transact.cli;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;

import org.testcontainers.postgresql.PostgreSQLContainer;

public class PgContainer implements AutoCloseable {

  private static final int SIZE = Runtime.getRuntime().availableProcessors();
  private static final BlockingQueue<PostgreSQLContainer> POOL = new ArrayBlockingQueue<>(SIZE);
  private static final Semaphore PERMITS = new Semaphore(SIZE);

  static {
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  var containers = new ArrayList<PostgreSQLContainer>();
                  POOL.drainTo(containers);
                  containers.forEach(PostgreSQLContainer::stop);
                }));
  }

  static PostgreSQLContainer acquire() {
    try {
      PERMITS.acquire();
      var container = POOL.poll();
      if (container == null) {
        container = new PostgreSQLContainer("postgres:18");
        container.start();
      }
      return container;
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  static void release(PostgreSQLContainer c) {
    POOL.offer(c);
    PERMITS.release();
  }

  private final PostgreSQLContainer pgContainer;
  private final String jdbcUrl;
  private final String dbName;

  public PgContainer() {
    // take a container from the pool and create a new database for it
    pgContainer = acquire();
    dbName = "test_" + UUID.randomUUID().toString().replace("-", "");
    jdbcUrl = pgContainer.getJdbcUrl().replaceFirst("/[^/]+$", "/" + dbName);

    try (var conn =
            DriverManager.getConnection(
                pgContainer.getJdbcUrl(), pgContainer.getUsername(), pgContainer.getPassword());
        var stmt = conn.createStatement()) {
      stmt.execute("CREATE DATABASE " + dbName);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws Exception {
    // drop a database we created and return the container too the pool
    var _jdbcUrl = pgContainer.getJdbcUrl();
    try (var conn = DriverManager.getConnection(_jdbcUrl, username(), password());
        var stmt = conn.createStatement()) {
      var sql = "DROP DATABASE IF EXISTS %s WITH (FORCE)".formatted(dbName);
      stmt.execute(sql);
    }
    release(pgContainer);
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

  public Connection connection() throws SQLException {
    return DriverManager.getConnection(jdbcUrl(), username(), password());
  }

  public List<String> options() {
    return List.of(urlOption(), userOption(), passwordOption());
  }

  public String urlOption() {
    return "-D=" + jdbcUrl();
  }

  public String userOption() {
    return "-U=" + username();
  }

  public String passwordOption() {
    return "-P=" + password();
  }
}
