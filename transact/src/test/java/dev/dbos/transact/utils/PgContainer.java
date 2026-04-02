package dev.dbos.transact.utils;

import dev.dbos.transact.DBOSClient;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.SystemDatabase;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zaxxer.hikari.HikariDataSource;

import org.testcontainers.postgresql.PostgreSQLContainer;

public class PgContainer implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(PgContainer.class);

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
    var _jdbcUrl = pgContainer.getJdbcUrl();
    try (var conn = DriverManager.getConnection(_jdbcUrl, username(), password());
        var stmt = conn.createStatement()) {
      stmt.execute("DROP DATABASE IF EXISTS " + dbName);
    } catch (SQLException e) {
      logger.warn("Failed to drop database {}, possibly still in use: {}", dbName, e.getMessage());
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
}
