package dev.dbos.transact.spring;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;

import org.testcontainers.postgresql.PostgreSQLContainer;

class PgContainer implements AutoCloseable {

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

  private static PostgreSQLContainer acquire() {
    try {
      PERMITS.acquire();
      var container = POOL.poll();
      if (container == null) {
        container = new PostgreSQLContainer("postgres:latest");
        container.start();
      }
      return container;
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private static void release(PostgreSQLContainer c) {
    POOL.offer(c);
    PERMITS.release();
  }

  private final PostgreSQLContainer pgContainer;
  private final String jdbcUrl;
  private final String dbName;

  PgContainer() {
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
  public void close() {
    try (var conn = DriverManager.getConnection(pgContainer.getJdbcUrl(), username(), password());
        var stmt = conn.createStatement()) {
      stmt.execute("DROP DATABASE IF EXISTS %s WITH (FORCE)".formatted(dbName));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    release(pgContainer);
  }

  String jdbcUrl() {
    return jdbcUrl;
  }

  String username() {
    return pgContainer.getUsername();
  }

  String password() {
    return pgContainer.getPassword();
  }
}
