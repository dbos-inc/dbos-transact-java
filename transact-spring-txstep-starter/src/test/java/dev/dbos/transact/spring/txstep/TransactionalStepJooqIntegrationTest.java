package dev.dbos.transact.spring.txstep;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.spring.DBOSAutoConfiguration;
import dev.dbos.transact.workflow.Workflow;

import java.sql.SQLException;

import javax.sql.DataSource;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.impl.DataSourceConnectionProvider;
import org.jooq.impl.DefaultConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;
import org.springframework.transaction.PlatformTransactionManager;

public class TransactionalStepJooqIntegrationTest {

  record Order(String id, String item, int qty) {}

  public static class OrderStepService {
    private final DSLContext dsl;

    OrderStepService(DSLContext dsl) {
      this.dsl = dsl;
    }

    @TransactionalStep
    public Order placeOrder(String orderId, String item, int qty) {
      dsl.execute("INSERT INTO orders(id, item, qty) VALUES (?, ?, ?)", orderId, item, qty);
      return new Order(orderId, item, qty);
    }

    @TransactionalStep
    public Order doError(String orderId, String item, int qty) {
      dsl.execute("INSERT INTO orders(id, item, qty) VALUES (?, ?, ?)", orderId, item, qty);
      throw new RuntimeException("intentional failure");
    }
  }

  public static class OrderWorkflowService {
    private final OrderStepService steps;

    OrderWorkflowService(OrderStepService steps) {
      this.steps = steps;
    }

    @Workflow
    public Order processOrder(String orderId, String item, int qty) {
      return steps.placeOrder(orderId, item, qty);
    }

    @Workflow
    public Order triggerError(String orderId, String item, int qty) {
      return steps.doError(orderId, item, qty);
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class OrderConfig {
    @Bean
    OrderStepService orderSteps(DSLContext dsl) {
      return new OrderStepService(dsl);
    }

    @Bean
    OrderWorkflowService orderWorkflow(OrderStepService steps) {
      return new OrderWorkflowService(steps);
    }
  }

  /**
   * Builds a DSLContext backed by a TransactionAwareDataSourceProxy so that JOOQ operations execute
   * on the Spring-transaction-bound connection inside {@code @TransactionalStep} methods.
   */
  private static DSLContext buildDsl(DataSource dataSource) {
    var config =
        new DefaultConfiguration()
            .set(new DataSourceConnectionProvider(new TransactionAwareDataSourceProxy(dataSource)))
            .set(SQLDialect.POSTGRES);
    return DSL.using(config);
  }

  private static ApplicationContextRunner runner(TransactionalStepTest.TestDatabase db) {
    new JdbcTemplate(db.dataSource)
        .execute(
            "CREATE TABLE IF NOT EXISTS orders"
                + " (id TEXT PRIMARY KEY, item TEXT NOT NULL, qty INT NOT NULL)");
    return new ApplicationContextRunner()
        .withConfiguration(
            AutoConfigurations.of(
                DBOSAutoConfiguration.class, TransactionalStepAutoConfiguration.class))
        .withPropertyValues("dbos.application.name=txstep-jooq-test")
        .withBean("dataSource", DataSource.class, () -> db.dataSource)
        .withBean(
            "transactionManager",
            PlatformTransactionManager.class,
            () -> new DataSourceTransactionManager(db.dataSource))
        .withBean("dslContext", DSLContext.class, () -> buildDsl(db.dataSource))
        .withUserConfiguration(OrderConfig.class);
  }

  private static int orderCount(DataSource ds, String orderId) throws SQLException {
    try (var conn = ds.getConnection();
        var stmt = conn.prepareStatement("SELECT COUNT(*) FROM orders WHERE id = ?")) {
      stmt.setString(1, orderId);
      try (var rs = stmt.executeQuery()) {
        return rs.next() ? rs.getInt(1) : 0;
      }
    }
  }

  @Test
  void autoConfig_createsExpectedBeans() {
    try (var db = new TransactionalStepTest.TestDatabase()) {
      runner(db)
          .run(
              ctx -> {
                assertThat(ctx).hasNotFailed();
                assertThat(ctx).hasSingleBean(DBOS.class);
                assertThat(ctx).hasSingleBean(TransactionalStepFactory.class);
                assertThat(ctx).hasSingleBean(TransactionalStepAspect.class);
                assertThat(ctx).hasSingleBean(TransactionalStepRegistrar.class);
              });
    }
  }

  @Test
  void goldenPath() throws SQLException {
    try (var db = new TransactionalStepTest.TestDatabase()) {
      runner(db)
          .run(
              ctx -> {
                assertThat(ctx).hasNotFailed();
                var workflow = ctx.getBean(OrderWorkflowService.class);
                var wfid = "wf-jooq-int-golden";

                try (var _o = new WorkflowOptions(wfid).setContext()) {
                  assertThat(workflow.processOrder("ord-1", "Widget", 5))
                      .isEqualTo(new Order("ord-1", "Widget", 5));
                }

                assertThat(orderCount(db.dataSource, "ord-1")).isEqualTo(1);
                var rows = TransactionalStepTest.getTxRows(db.dataSource, wfid);
                assertThat(rows).hasSize(1);
                assertThat(rows.get(0).output()).isNotNull();
                assertThat(rows.get(0).error()).isNull();
              });
    }
  }

  @Test
  void idempotency() throws SQLException {
    try (var db = new TransactionalStepTest.TestDatabase()) {
      runner(db)
          .run(
              ctx -> {
                assertThat(ctx).hasNotFailed();
                var workflow = ctx.getBean(OrderWorkflowService.class);
                var wfid = "wf-jooq-int-idem";

                try (var _o = new WorkflowOptions(wfid).setContext()) {
                  workflow.processOrder("ord-2", "Gadget", 3);
                }
                try (var _o = new WorkflowOptions(wfid).setContext()) {
                  assertThat(workflow.processOrder("ord-2", "Gadget", 3))
                      .isEqualTo(new Order("ord-2", "Gadget", 3));
                }

                assertThat(orderCount(db.dataSource, "ord-2")).isEqualTo(1);
                assertThat(TransactionalStepTest.getTxRows(db.dataSource, wfid)).hasSize(1);
              });
    }
  }

  @Test
  void atomicityOnFailure() throws SQLException {
    try (var db = new TransactionalStepTest.TestDatabase()) {
      runner(db)
          .run(
              ctx -> {
                assertThat(ctx).hasNotFailed();
                var workflow = ctx.getBean(OrderWorkflowService.class);
                var wfid = "wf-jooq-int-fail";

                try (var _o = new WorkflowOptions(wfid).setContext()) {
                  assertThatThrownBy(() -> workflow.triggerError("ord-3", "Thing", 1))
                      .isInstanceOf(RuntimeException.class);
                }

                assertThat(orderCount(db.dataSource, "ord-3")).isEqualTo(0);
                var rows = TransactionalStepTest.getTxRows(db.dataSource, wfid);
                assertThat(rows).hasSize(1);
                assertThat(rows.get(0).output()).isNull();
                assertThat(rows.get(0).error()).isNotNull();
              });
    }
  }

  @Test
  void customSchema_property_tableCreatedInCustomSchema() throws SQLException {
    try (var db = new TransactionalStepTest.TestDatabase()) {
      runner(db)
          .withPropertyValues("dbos.txstep.schema=custom_schema")
          .run(
              ctx -> {
                assertThat(ctx).hasNotFailed();
                assertThat(
                        TransactionalStepTest.tableExists(
                            db.dataSource, "custom_schema", "tx_step_outputs"))
                    .isTrue();
                assertThat(
                        TransactionalStepTest.tableExists(
                            db.dataSource, SystemDatabase.sanitizeSchema(null), "tx_step_outputs"))
                    .isFalse();
              });
    }
  }
}
