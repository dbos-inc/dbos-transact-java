package dev.dbos.transact.spring.txstep;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.json.SerializationUtil;
import dev.dbos.transact.workflow.Workflow;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.hibernate.jpa.HibernatePersistenceProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.testcontainers.postgresql.PostgreSQLContainer;

public class TransactionalStepTest {

  // ---- Shared Postgres container pool ----

  private static final int POOL_SIZE = Runtime.getRuntime().availableProcessors();
  private static final BlockingQueue<PostgreSQLContainer> PG_POOL =
      new ArrayBlockingQueue<>(POOL_SIZE);
  private static final Semaphore PG_PERMITS = new Semaphore(POOL_SIZE);

  static {
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  var containers = new ArrayList<PostgreSQLContainer>();
                  PG_POOL.drainTo(containers);
                  containers.forEach(PostgreSQLContainer::stop);
                }));
  }

  private static PostgreSQLContainer acquireContainer() {
    try {
      PG_PERMITS.acquire();
      var c = PG_POOL.poll();
      if (c == null) {
        c = new PostgreSQLContainer("postgres:18");
        c.start();
      }
      return c;
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private static void releaseContainer(PostgreSQLContainer c) {
    PG_POOL.offer(c);
    PG_PERMITS.release();
  }

  // ---- Test helper: one isolated DB per test ----

  static class TestDatabase implements AutoCloseable {
    final PostgreSQLContainer container;
    final String dbName;
    final String jdbcUrl;
    final HikariDataSource dataSource;

    TestDatabase() {
      container = acquireContainer();
      dbName = "test_" + UUID.randomUUID().toString().replace("-", "");
      jdbcUrl = container.getJdbcUrl().replaceFirst("/[^/]+$", "/" + dbName);
      try (var conn =
              DriverManager.getConnection(
                  container.getJdbcUrl(), container.getUsername(), container.getPassword());
          var stmt = conn.createStatement()) {
        stmt.execute("CREATE DATABASE " + dbName);
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
      var cfg = new HikariConfig();
      cfg.setJdbcUrl(jdbcUrl);
      cfg.setUsername(container.getUsername());
      cfg.setPassword(container.getPassword());
      dataSource = new HikariDataSource(cfg);
    }

    DBOSConfig dbosConfig() {
      return DBOSConfig.defaults("txstep-test")
          .withDatabaseUrl(jdbcUrl)
          .withDbUser(container.getUsername())
          .withDbPassword(container.getPassword());
    }

    @Override
    public void close() {
      dataSource.close();
      try (var conn =
              DriverManager.getConnection(
                  container.getJdbcUrl(), container.getUsername(), container.getPassword());
          var stmt = conn.createStatement()) {
        stmt.execute("DROP DATABASE IF EXISTS " + dbName + " WITH (FORCE)");
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
      releaseContainer(container);
    }
  }

  // ---- DB query helpers ----

  record TxRow(String workflowId, int stepId, String output, String error) {}

  static List<TxRow> getTxRows(DataSource ds, String workflowId) throws SQLException {
    var schema = SystemDatabase.sanitizeSchema(null);
    var sql =
        "SELECT * FROM \"%s\".tx_step_outputs WHERE workflow_id = ? ORDER BY step_id"
            .formatted(schema);
    try (var conn = ds.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, workflowId);
      try (ResultSet rs = stmt.executeQuery()) {
        var rows = new ArrayList<TxRow>();
        while (rs.next()) {
          rows.add(
              new TxRow(
                  rs.getString("workflow_id"),
                  rs.getInt("step_id"),
                  rs.getString("output"),
                  rs.getString("error")));
        }
        return rows;
      }
    }
  }

  static boolean tableExists(DataSource ds, String schema, String table) throws SQLException {
    try (var conn = ds.getConnection();
        var rs = conn.getMetaData().getTables(null, schema, table, new String[] {"TABLE"})) {
      return rs.next();
    }
  }

  static int greetCount(DataSource ds, String name) throws SQLException {
    try (var conn = ds.getConnection();
        var stmt = conn.prepareStatement("SELECT count FROM greetings WHERE name = ?")) {
      stmt.setString(1, name);
      try (var rs = stmt.executeQuery()) {
        return rs.next() ? rs.getInt("count") : 0;
      }
    }
  }

  // ---- Test service shared by all variants ----

  interface GreetingService {
    String insert(String name);

    String error(String name);

    void voidStep();

    String conflictInsert(String name) throws SQLException;
  }

  static class GreetingServiceImpl implements GreetingService {
    private final TransactionalStepFactory factory;
    private final JdbcTemplate jdbc;

    GreetingServiceImpl(TransactionalStepFactory factory, JdbcTemplate jdbc) {
      this.factory = factory;
      this.jdbc = jdbc;
    }

    @Override
    @Workflow
    public String insert(String name) {
      return (String)
          factory.runTransactionalStep(
              () -> {
                jdbc.update(
                    "INSERT INTO greetings(name, count) VALUES (?, 1)"
                        + " ON CONFLICT(name) DO UPDATE SET count = greetings.count + 1",
                    name);
                return name;
              },
              "insert");
    }

    @Override
    @Workflow
    public String error(String name) {
      return (String)
          factory.runTransactionalStep(
              () -> {
                jdbc.update(
                    "INSERT INTO greetings(name, count) VALUES (?, 1)"
                        + " ON CONFLICT(name) DO UPDATE SET count = greetings.count + 1",
                    name);
                throw new RuntimeException("intentional failure");
              },
              "error");
    }

    @Override
    @Workflow
    public void voidStep() {
      factory.runTransactionalStep(() -> null, "voidStep");
    }

    // Simulates a concurrent winner committing a result while this executor's transaction is still
    // open. The separate autocommit connection represents the other executor — its INSERT persists
    // even when the Spring transaction manager rolls back the main transaction. When recordOutput
    // subsequently tries to INSERT the same (workflowId, stepId) key, it gets a 23505
    // unique-constraint violation. The factory rolls back and falls back to checkExecution.
    @Override
    @Workflow
    public String conflictInsert(String name) throws SQLException {
      return (String)
          factory.runTransactionalStep(
              () -> {
                var wfId = Objects.requireNonNull(DBOS.workflowId());
                var stepId = Objects.requireNonNull(DBOS.stepId());
                var schema = SystemDatabase.sanitizeSchema(null);
                var value = SerializationUtil.serializeValue("winner", null, null);
                var sql =
                    """
                    INSERT INTO "%s".tx_step_outputs(workflow_id, step_id, output, error, serialization)
                    VALUES (?, ?, ?, NULL, ?)
                    """
                        .formatted(schema);
                try (var conn2 = jdbc.getDataSource().getConnection();
                    var stmt = conn2.prepareStatement(sql)) {
                  stmt.setString(1, wfId);
                  stmt.setInt(2, stepId);
                  stmt.setString(3, value.serializedValue());
                  stmt.setString(4, value.serialization());
                  stmt.executeUpdate();
                }
                jdbc.update("INSERT INTO greetings(name, count) VALUES (?, 1)", name);
                return name;
              },
              "conflictInsert");
    }
  }

  // ---- DataSourceTransactionManager tests ----

  @Nested
  class WithDataSourceTransactionManager {

    @AutoClose TestDatabase db;
    @AutoClose DBOS dbos;
    JdbcTemplate jdbc;
    TransactionalStepFactory factory;
    GreetingService proxy;

    @BeforeEach
    void setup() throws SQLException {
      db = new TestDatabase();
      jdbc = new JdbcTemplate(db.dataSource);

      try (var conn = db.dataSource.getConnection();
          var stmt = conn.createStatement()) {
        stmt.execute(
            "CREATE TABLE greetings (name TEXT PRIMARY KEY, count INT NOT NULL DEFAULT 0)");
      }

      dbos = new DBOS(db.dbosConfig());
      var txManager = new DataSourceTransactionManager(db.dataSource);
      factory = new TransactionalStepFactory(dbos, db.dataSource, txManager, null);
      factory.initialize();

      var impl = new GreetingServiceImpl(factory, jdbc);
      proxy = dbos.registerProxy(GreetingService.class, impl);
      dbos.launch();
    }

    @AfterEach
    void teardown() {
      if (dbos != null) dbos.close();
    }

    @Test
    void goldenPath() throws SQLException {
      var wfid = "wf-golden";
      try (var _o = new WorkflowOptions(wfid).setContext()) {
        var result = proxy.insert("alice");
        assertThat(result).isEqualTo("alice");
      }

      assertThat(greetCount(db.dataSource, "alice")).isEqualTo(1);
      var rows = getTxRows(db.dataSource, wfid);
      assertThat(rows).hasSize(1);
      assertThat(rows.get(0).output()).isNotNull();
      assertThat(rows.get(0).error()).isNull();
    }

    @Test
    void idempotency() throws SQLException {
      var wfid = "wf-idem";

      try (var _o = new WorkflowOptions(wfid).setContext()) {
        proxy.insert("bob");
      }
      // second call with same workflow ID — must not re-execute
      try (var _o = new WorkflowOptions(wfid).setContext()) {
        var result = proxy.insert("bob");
        assertThat(result).isEqualTo("bob");
      }

      assertThat(greetCount(db.dataSource, "bob")).isEqualTo(1);
      assertThat(getTxRows(db.dataSource, wfid)).hasSize(1);
    }

    @Test
    void atomicityOnFailure() throws SQLException {
      var wfid = "wf-fail";
      try (var _o = new WorkflowOptions(wfid).setContext()) {
        assertThatThrownBy(() -> proxy.error("charlie")).isInstanceOf(RuntimeException.class);
      }

      // main transaction rolled back — no greeting inserted
      assertThat(greetCount(db.dataSource, "charlie")).isEqualTo(0);
      var rows = getTxRows(db.dataSource, wfid);
      assertThat(rows).hasSize(1);
      assertThat(rows.get(0).output()).isNull();
      assertThat(rows.get(0).error()).isNotNull();
    }

    @Test
    void voidMethods() throws SQLException {
      var wfid = "wf-void";
      try (var _o = new WorkflowOptions(wfid).setContext()) {
        proxy.voidStep();
      }

      var rows = getTxRows(db.dataSource, wfid);
      assertThat(rows).hasSize(1);
      assertThat(rows.get(0).error()).isNull();
    }

    // Two executors race to write the result for the same step. The loser detects the 23505
    // conflict on its INSERT, rolls back its transaction, and returns the winner's stored value.
    @Test
    void upsertConflict() throws SQLException {
      var wfid = "wf-conflict";

      try (var _o = new WorkflowOptions(wfid).setContext()) {
        var result = proxy.conflictInsert("diana");
        // Returns winner's sentinel value, not what the supplier would have returned
        assertThat(result).isEqualTo("winner");
      }

      // Main transaction was rolled back — INSERT into greetings never committed
      assertThat(greetCount(db.dataSource, "diana")).isEqualTo(0);

      // Exactly one tx_step_outputs row containing the winner's result
      var rows = getTxRows(db.dataSource, wfid);
      assertThat(rows).hasSize(1);
      assertThat(rows.get(0).output()).isNotNull();
      assertThat(rows.get(0).error()).isNull();
    }
  }

  // ---- Custom schema tests ----

  @Test
  void customSchema_explicitSchemaOverride_tableInCustomSchema() throws SQLException {
    try (var db = new TestDatabase()) {
      try (var dbos = new DBOS(db.dbosConfig())) {
        var txManager = new DataSourceTransactionManager(db.dataSource);
        var factory = new TransactionalStepFactory(dbos, db.dataSource, txManager, "app_schema");
        factory.initialize();
        dbos.launch();

        assertThat(tableExists(db.dataSource, "app_schema", "tx_step_outputs")).isTrue();
        assertThat(
                tableExists(db.dataSource, SystemDatabase.sanitizeSchema(null), "tx_step_outputs"))
            .isFalse();
      }
    }
  }

  @Test
  void customSchema_nullExplicit_fallsBackToDbosConfigSchema() throws SQLException {
    try (var db = new TestDatabase()) {
      try (var dbos = new DBOS(db.dbosConfig().withDatabaseSchema("cfg_schema"))) {
        var txManager = new DataSourceTransactionManager(db.dataSource);
        var factory = new TransactionalStepFactory(dbos, db.dataSource, txManager, null);
        factory.initialize();
        dbos.launch();

        assertThat(tableExists(db.dataSource, "cfg_schema", "tx_step_outputs")).isTrue();
        assertThat(
                tableExists(db.dataSource, SystemDatabase.sanitizeSchema(null), "tx_step_outputs"))
            .isFalse();
      }
    }
  }

  @Test
  void customSchema_bothNull_usesDefaultDbosSchema() throws SQLException {
    try (var db = new TestDatabase()) {
      try (var dbos = new DBOS(db.dbosConfig())) {
        var txManager = new DataSourceTransactionManager(db.dataSource);
        var factory = new TransactionalStepFactory(dbos, db.dataSource, txManager, null);
        factory.initialize();
        dbos.launch();

        var defaultSchema = SystemDatabase.sanitizeSchema(null);
        assertThat(tableExists(db.dataSource, defaultSchema, "tx_step_outputs")).isTrue();
      }
    }
  }

  // ---- Lazy initialization test ----

  @Test
  void lazyInitialization_noTransactionalStepMethods_tableNotCreated() throws SQLException {
    try (var db = new TestDatabase()) {
      var dbosConfig = db.dbosConfig();
      try (var dbos = new DBOS(dbosConfig)) {
        var txManager = new DataSourceTransactionManager(db.dataSource);
        new TransactionalStepFactory(dbos, db.dataSource, txManager, null);
        // initialize() is NOT called — simulating no @TransactionalStep methods found
        dbos.launch();

        var schema = SystemDatabase.sanitizeSchema(null);
        assertThat(tableExists(db.dataSource, schema, "tx_step_outputs")).isFalse();
      }
    }
  }

  // ---- JPA path tests ----

  @Nested
  class WithJpaTransactionManager {

    @AutoClose TestDatabase db;
    @AutoClose DBOS dbos;
    JdbcTemplate jdbc;
    TransactionalStepFactory factory;
    GreetingService proxy;

    @BeforeEach
    void setup() throws Exception {
      db = new TestDatabase();
      jdbc = new JdbcTemplate(db.dataSource);

      try (var conn = db.dataSource.getConnection();
          var stmt = conn.createStatement()) {
        stmt.execute(
            "CREATE TABLE greetings (name TEXT PRIMARY KEY, count INT NOT NULL DEFAULT 0)");
      }

      dbos = new DBOS(db.dbosConfig());

      var jpaTxManager = buildJpaTransactionManager(db.dataSource);
      factory = new TransactionalStepFactory(dbos, db.dataSource, jpaTxManager, null);
      factory.initialize();

      var impl = new GreetingServiceImpl(factory, jdbc);
      proxy = dbos.registerProxy(GreetingService.class, impl);
      dbos.launch();
    }

    @AfterEach
    void teardown() {
      if (dbos != null) dbos.close();
    }

    @Test
    void goldenPath() throws SQLException {
      var wfid = "wf-jpa-golden";
      try (var _o = new WorkflowOptions(wfid).setContext()) {
        var result = proxy.insert("dave");
        assertThat(result).isEqualTo("dave");
      }

      assertThat(greetCount(db.dataSource, "dave")).isEqualTo(1);
      var rows = getTxRows(db.dataSource, wfid);
      assertThat(rows).hasSize(1);
      assertThat(rows.get(0).output()).isNotNull();
      assertThat(rows.get(0).error()).isNull();
    }

    @Test
    void atomicityOnFailure() throws SQLException {
      var wfid = "wf-jpa-fail";
      try (var _o = new WorkflowOptions(wfid).setContext()) {
        assertThatThrownBy(() -> proxy.error("eve")).isInstanceOf(RuntimeException.class);
      }

      assertThat(greetCount(db.dataSource, "eve")).isEqualTo(0);
      var rows = getTxRows(db.dataSource, wfid);
      assertThat(rows).hasSize(1);
      assertThat(rows.get(0).output()).isNull();
      assertThat(rows.get(0).error()).isNotNull();
    }

    // Two executors race to write the result for the same step. The loser detects the 23505
    // conflict on its INSERT, rolls back its transaction, and returns the winner's stored value.
    @Test
    void upsertConflict() throws SQLException {
      var wfid = "wf-jpa-conflict";

      try (var _o = new WorkflowOptions(wfid).setContext()) {
        var result = proxy.conflictInsert("fiona");
        // Returns winner's sentinel value, not what the supplier would have returned
        assertThat(result).isEqualTo("winner");
      }

      // Main transaction was rolled back — INSERT into greetings never committed
      assertThat(greetCount(db.dataSource, "fiona")).isEqualTo(0);

      // Exactly one tx_step_outputs row containing the winner's result
      var rows = getTxRows(db.dataSource, wfid);
      assertThat(rows).hasSize(1);
      assertThat(rows.get(0).output()).isNotNull();
      assertThat(rows.get(0).error()).isNull();
    }

    private static JpaTransactionManager buildJpaTransactionManager(DataSource dataSource) {
      var emfBean = new LocalContainerEntityManagerFactoryBean();
      emfBean.setDataSource(dataSource);
      emfBean.setPackagesToScan(); // no entity classes
      emfBean.setJpaVendorAdapter(new HibernateJpaVendorAdapter());
      emfBean.setPersistenceProviderClass(HibernatePersistenceProvider.class);
      var props = new Properties();
      props.put("hibernate.hbm2ddl.auto", "none");
      props.put("hibernate.dialect", "org.hibernate.dialect.PostgreSQLDialect");
      emfBean.setJpaProperties(props);
      emfBean.afterPropertiesSet();

      var jpaTxManager = new JpaTransactionManager(emfBean.getObject());
      // Simulating what TransactionalStepAutoConfiguration.JpaBridgeConfiguration does:
      // set the dataSource so DataSourceUtils.getConnection() returns the tx-bound connection.
      jpaTxManager.setDataSource(dataSource);
      jpaTxManager.afterPropertiesSet();
      return jpaTxManager;
    }
  }
}
