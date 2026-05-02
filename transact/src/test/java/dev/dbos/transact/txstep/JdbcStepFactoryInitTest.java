package dev.dbos.transact.txstep;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.Constants;
import dev.dbos.transact.DBOS;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.utils.PgContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;

import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.Test;

public class JdbcStepFactoryInitTest {
  @AutoClose final PgContainer pgContainer = new PgContainer();

  static boolean validateSchema(Connection conn, String schema) throws SQLException {
    Objects.requireNonNull(schema);
    try (var rs = conn.getMetaData().getSchemas()) {
      while (rs.next()) {
        if (schema.equalsIgnoreCase(rs.getString("TABLE_SCHEM"))) {
          return true;
        }
      }
    }
    return false;
  }

  static boolean validateTxStepOutputsTable(Connection conn, String schema) throws SQLException {
    try (var rs =
        conn.getMetaData()
            .getTables(null, Objects.requireNonNull(schema), "tx_step_outputs", null)) {
      if (rs.next()) {
        if (schema.equals(rs.getString("TABLE_SCHEM"))
            && "tx_step_outputs".equals(rs.getString("TABLE_NAME"))
            && "TABLE".equalsIgnoreCase(rs.getString("TABLE_TYPE"))) {
          return true;
        }
      }
    }
    return false;
  }

  @Test
  public void sameDbDefaultSchema() throws Exception {
    var config = pgContainer.dbosConfig();
    try (var dbos = new DBOS(config);
        var dataSource = pgContainer.dataSource()) {
      var schema = Constants.DB_SCHEMA;

      // ensure step factory schema/table do not exist
      try (var conn = dataSource.getConnection()) {
        assertFalse(validateSchema(conn, schema));
        assertFalse(validateTxStepOutputsTable(conn, schema));
      }

      // create step factory to initialize the app db tables
      new JdbcStepFactory(dbos, dataSource);

      // ensure step factory schema/table do exist
      try (var conn = dataSource.getConnection()) {
        assertTrue(validateSchema(conn, schema));
        assertTrue(validateTxStepOutputsTable(conn, schema));
      }
      dbos.launch();
    }
  }

  @Test
  public void sameDbCustomDbosSchema() throws Exception {
    var schema = "custom";
    var config = pgContainer.dbosConfig().withDatabaseSchema(schema);
    try (var dbos = new DBOS(config);
        var dataSource = pgContainer.dataSource()) {
      new JdbcStepFactory(dbos, dataSource);
      try (var conn = dataSource.getConnection()) {
        assertTrue(validateSchema(conn, schema));
        assertTrue(validateTxStepOutputsTable(conn, schema));
      }
      dbos.launch();
    }
  }

  @Test
  public void sameDbCustomFactorySchema() throws Exception {
    var schema = "custom";
    var config = pgContainer.dbosConfig();
    try (var dbos = new DBOS(config);
        var dataSource = pgContainer.dataSource()) {
      new JdbcStepFactory(dbos, dataSource, schema);
      try (var conn = dataSource.getConnection()) {
        assertFalse(validateSchema(conn, Constants.DB_SCHEMA));
        assertTrue(validateSchema(conn, schema));
        assertTrue(validateTxStepOutputsTable(conn, schema));
      }
      dbos.launch();
      try (var conn = dataSource.getConnection()) {
        assertTrue(validateSchema(conn, Constants.DB_SCHEMA));
      }
    }
  }

  @Test
  public void sameDbCustomDbosAndFactorySchema() throws Exception {
    var dbosSchema = "custom_a";
    var factorySchema = "custom_b";
    var config = pgContainer.dbosConfig().withDatabaseSchema(dbosSchema);
    try (var dbos = new DBOS(config);
        var dataSource = pgContainer.dataSource()) {
      new JdbcStepFactory(dbos, dataSource, factorySchema);
      try (var conn = dataSource.getConnection()) {
        assertFalse(validateSchema(conn, dbosSchema));
        assertTrue(validateSchema(conn, factorySchema));
        assertTrue(validateTxStepOutputsTable(conn, factorySchema));
      }
      dbos.launch();
      try (var conn = dataSource.getConnection()) {
        assertTrue(validateSchema(conn, dbosSchema));
      }
    }
  }

  @Test
  public void separateDBs() throws Exception {
    // create a 2nd database in the container's PG instance
    var appDbName = "factory_test_db";
    try (var conn =
            DriverManager.getConnection(
                pgContainer.jdbcUrl(), pgContainer.username(), pgContainer.password());
        var stmt = conn.createStatement()) {
      stmt.execute("CREATE DATABASE " + appDbName);
    }
    var appDbJdbcUrl = pgContainer.jdbcUrl().replaceFirst("/[^/]+$", "/" + appDbName);

    var config = pgContainer.dbosConfig();
    try (var dbos = new DBOS(config);
        var dataSource =
            SystemDatabase.createDataSource(
                appDbJdbcUrl, pgContainer.username(), pgContainer.password())) {
      new JdbcStepFactory(dbos, dataSource);
      dbos.launch();

      var appDbTables = DBUtils.getTables(dataSource, "dbos");
      assertEquals(1, appDbTables.size());
      assertTrue(appDbTables.contains("tx_step_outputs"));

      var sysDbTables = DBUtils.getTables(pgContainer, "dbos");
      assertTrue(sysDbTables.size() >= 10);
      assertFalse(sysDbTables.contains("tx_step_outputs"));
      assertTrue(sysDbTables.contains("dbos_migrations"));
      assertTrue(sysDbTables.contains("workflow_status"));
      assertTrue(sysDbTables.contains("operation_outputs"));
    }
  }
}
