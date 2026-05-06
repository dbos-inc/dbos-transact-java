package dev.dbos.transact.txstep;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresStepFactoryHelpers {

  private static final Logger logger = LoggerFactory.getLogger(PostgresStepFactoryHelpers.class);

  public static final String CHECK_SQL_TEMPLATE =
      """
      SELECT output, error, serialization
      FROM "%s".tx_step_outputs
      WHERE workflow_id = ? AND step_id = ?
      """;

  public static final String UPSERT_SQL_TEMPLATE =
      """
      INSERT INTO "%s".tx_step_outputs
        (workflow_id, step_id, output, error, serialization)
      VALUES (?, ?, ?, ?, ?)
      ON CONFLICT DO NOTHING
      """;

  /**
   * Verifies that the given connection is to a PostgreSQL database.
   *
   * @throws IllegalArgumentException if the database is not PostgreSQL
   * @throws SQLException if database metadata cannot be read
   */
  public static void ensurePostgres(Connection conn) throws SQLException {
    var productName = conn.getMetaData().getDatabaseProductName();
    if (!productName.equalsIgnoreCase("PostgreSQL")) {
      throw new IllegalArgumentException(
          "PostgresStepFactory requires a PostgreSQL datasource, got: " + productName);
    }
  }

  /**
   * Returns {@code true} if the named schema exists in the database.
   *
   * @throws SQLException if the query fails
   */
  public static boolean schemaExists(Connection conn, String schema) throws SQLException {
    var sql = "SELECT schema_name FROM information_schema.schemata WHERE schema_name = ?";
    try (var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, Objects.requireNonNull(schema, "schema must not be null"));
      try (var rs = stmt.executeQuery()) {
        return rs.next();
      }
    }
  }

  /**
   * Creates the named schema if it does not already exist.
   *
   * @throws SQLException if the DDL fails
   */
  public static void ensureSchema(Connection conn, String schema) throws SQLException {
    Objects.requireNonNull(schema, "schema must not be null");
    if (!schemaExists(conn, schema)) {
      try (var stmt = conn.createStatement()) {
        stmt.execute("CREATE SCHEMA IF NOT EXISTS \"%s\"".formatted(schema));
      }
    }
  }

  /**
   * Returns {@code true} if the {@code tx_step_outputs} table exists in the named schema.
   *
   * @throws SQLException if the query fails
   */
  public static boolean tableExists(Connection conn, String schema) throws SQLException {
    var sql = "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?";
    try (var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, Objects.requireNonNull(schema, "schema must not be null"));
      stmt.setString(2, Objects.requireNonNull("tx_step_outputs", "tableName must not be null"));
      try (var rs = stmt.executeQuery()) {
        return rs.next();
      }
    }
  }

  /**
   * Creates the {@code tx_step_outputs} table in the named schema if it does not already exist.
   *
   * @throws SQLException if the DDL fails
   */
  public static void ensureTxOutputTable(Connection conn, String schema) throws SQLException {
    Objects.requireNonNull(schema, "schema must not be null");
    if (tableExists(conn, schema)) {
      return;
    }
    logger.debug("Creating tx_step_outputs table in schema={}", schema);
    try (var stmt = conn.createStatement()) {
      var ddlSql =
          """
          CREATE TABLE IF NOT EXISTS "%1$s".tx_step_outputs (
            workflow_id TEXT NOT NULL,
            step_id INT NOT NULL,
            output TEXT,
            error TEXT,
            serialization TEXT,
            created_at BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM now())*1000)::bigint,
            PRIMARY KEY (workflow_id, step_id)
          )"""
              .formatted(schema);
      stmt.execute(ddlSql);
    }
  }
}
