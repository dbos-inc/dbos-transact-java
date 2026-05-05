package dev.dbos.transact.txstep;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.json.DBOSSerializer;
import dev.dbos.transact.workflow.internal.StepResult;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;

import javax.sql.DataSource;

import org.jspecify.annotations.Nullable;

/**
 * A {@link PostgresStepFactory} implementation backed by plain JDBC {@link Connection} objects.
 *
 * <p>Construct one with a {@link DataSource} pointing at a PostgreSQL database. The constructor
 * verifies the datasource is PostgreSQL and creates the {@code tx_step_outputs} table if needed.
 * User lambdas passed to {@code txStep} receive a {@link Connection} with a transaction already
 * started; they should not call {@code commit} or {@code close} themselves.
 *
 * <pre>{@code
 * JdbcStepFactory factory = new JdbcStepFactory(dbos, dataSource);
 *
 * // inside a @Workflow method:
 * int count = factory.txStep(conn -> {
 *     try (var stmt = conn.prepareStatement("INSERT INTO ...")) { ... }
 *     return rowCount;
 * }, "myStep");
 * }</pre>
 */
public class JdbcStepFactory extends PostgresStepFactory<Connection> {

  private final DataSource dataSource;

  /** Creates a factory using the schema from the DBOS config. */
  public JdbcStepFactory(DBOS dbos, DataSource dataSource) {
    this(dbos, dataSource, null, null);
  }

  /** Creates a factory using a custom schema for {@code tx_step_outputs}. */
  public JdbcStepFactory(DBOS dbos, DataSource dataSource, String schema) {
    this(dbos, dataSource, schema, null);
  }

  /** Creates a factory using a custom serializer. */
  public JdbcStepFactory(DBOS dbos, DataSource dataSource, DBOSSerializer serializer) {
    this(dbos, dataSource, null, serializer);
  }

  /** Creates a factory with a custom schema and serializer. */
  public JdbcStepFactory(
      DBOS dbos, DataSource dataSource, String schema, DBOSSerializer serializer) {
    super(dbos, schema, serializer);
    this.dataSource = Objects.requireNonNull(dataSource);
    createTxOutputTable(dataSource, this.schema);
  }

  /**
   * Verifies the datasource is PostgreSQL and creates the {@code tx_step_outputs} table if it does
   * not exist. Useful when the {@code DataSource} is managed separately from the factory lifecycle.
   */
  public static void createTxOutputTable(DataSource dataSource, String schema) {
    try (var conn = dataSource.getConnection()) {
      ensurePostgres(conn);
      ensureSchema(conn, schema);
      ensureTxOutputTable(conn, schema);
    } catch (SQLException e) {
      throw new DBOSSqlException(e);
    }
  }

  private static class DBOSSqlException extends RuntimeException {
    public DBOSSqlException(SQLException wrappedException) {
      super(wrappedException.getMessage(), wrappedException);
    }
  }

  @Override
  protected Connection openTransaction() {
    try {
      var conn = dataSource.getConnection();
      conn.setAutoCommit(false);
      return conn;
    } catch (SQLException e) {
      throw new DBOSSqlException(e);
    }
  }

  @Override
  protected Connection openConnection() {
    try {
      return dataSource.getConnection();
    } catch (SQLException e) {
      throw new DBOSSqlException(e);
    }
  }

  @Override
  protected void commit(Connection conn) {
    try {
      conn.commit();
    } catch (SQLException e) {
      throw new DBOSSqlException(e);
    }
  }

  @Override
  protected void rollback(Connection conn) {
    try {
      conn.rollback();
    } catch (SQLException e) {
      throw new DBOSSqlException(e);
    }
  }

  @Override
  protected void close(Connection conn) {
    try {
      conn.close();
    } catch (SQLException e) {
      throw new DBOSSqlException(e);
    }
  }

  @Override
  protected @Nullable StepResult checkExecution(String workflowId, int stepId, String stepName) {
    var sql = CHECK_SQL_TEMPLATE.formatted(this.schema);
    try (var conn = dataSource.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, workflowId);
      stmt.setInt(2, stepId);
      try (var rs = stmt.executeQuery()) {
        if (rs.next()) {
          return new StepResult(
              workflowId,
              stepId,
              stepName,
              rs.getString("output"),
              rs.getString("error"),
              null,
              rs.getString("serialization"));
        }
        return null;
      }
    } catch (SQLException e) {
      throw new DBOSSqlException(e);
    }
  }

  @Override
  protected void recordResult(
      Connection conn,
      String workflowId,
      int stepId,
      String output,
      String error,
      String serialization) {
    if (output != null && error != null) {
      throw new IllegalArgumentException("attempted to record non null output and error result");
    }
    var sql = UPSERT_SQL_TEMPLATE.formatted(schema);
    try (var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, workflowId);
      stmt.setInt(2, stepId);
      stmt.setString(3, output);
      stmt.setString(4, error);
      stmt.setString(5, serialization);
      stmt.executeUpdate();
    } catch (SQLException e) {
      throw new DBOSSqlException(e);
    }
  }
}
