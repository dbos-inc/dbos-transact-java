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

import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.spring.JdbiFactoryBean;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

public class TransactionalStepJdbiIntegrationTest {

  record Order(String id, String item, int qty) {}

  public static class OrderStepService {
    private final Jdbi jdbi;

    OrderStepService(Jdbi jdbi) {
      this.jdbi = jdbi;
    }

    @TransactionalStep
    public Order placeOrder(String orderId, String item, int qty) {
      jdbi.withHandle(
          h -> h.execute("INSERT INTO orders(id, item, qty) VALUES (?, ?, ?)", orderId, item, qty));
      return new Order(orderId, item, qty);
    }

    @TransactionalStep
    public Order doError(String orderId, String item, int qty) {
      jdbi.withHandle(
          h -> h.execute("INSERT INTO orders(id, item, qty) VALUES (?, ?, ?)", orderId, item, qty));
      throw new RuntimeException("intentional failure");
    }
  }

  // Outer @TransactionalStep that calls an inner @TransactionalStep via proxy
  public static class OrderOuterStepService {
    private final Jdbi jdbi;
    private final OrderStepService inner;

    OrderOuterStepService(Jdbi jdbi, OrderStepService inner) {
      this.jdbi = jdbi;
      this.inner = inner;
    }

    @TransactionalStep
    public Order placeOrderWithNested(
        String outerOrderId, String innerOrderId, String item, int qty) {
      jdbi.withHandle(
          h ->
              h.execute(
                  "INSERT INTO orders(id, item, qty) VALUES (?, ?, ?)", outerOrderId, item, qty));
      return inner.placeOrder(innerOrderId, item, qty);
    }
  }

  public static class OrderWorkflowService {
    private final DBOS dbos;
    private final OrderStepService steps;
    private final OrderOuterStepService outerSteps;

    OrderWorkflowService(DBOS dbos, OrderStepService steps, OrderOuterStepService outerSteps) {
      this.dbos = dbos;
      this.steps = steps;
      this.outerSteps = outerSteps;
    }

    @Workflow
    public Order processOrder(String orderId, String item, int qty) {
      return steps.placeOrder(orderId, item, qty);
    }

    @Workflow
    public Order triggerError(String orderId, String item, int qty) {
      return steps.doError(orderId, item, qty);
    }

    @Workflow
    public Order processOrderWithNested(
        String outerOrderId, String innerOrderId, String item, int qty) {
      return outerSteps.placeOrderWithNested(outerOrderId, innerOrderId, item, qty);
    }

    @Workflow
    public Order processOrderViaDbosStep(String orderId, String item, int qty) {
      return dbos.runStep(() -> steps.placeOrder(orderId, item, qty), "processOrderViaDbosStep");
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class JdbiInfraConfig {
    @Bean
    DataSourceTransactionManager transactionManager(DataSource dataSource) {
      return new DataSourceTransactionManager(dataSource);
    }

    @Bean
    JdbiFactoryBean jdbi(DataSource dataSource) {
      return new JdbiFactoryBean(dataSource);
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class OrderConfig {
    @Bean
    OrderStepService orderSteps(Jdbi jdbi) {
      return new OrderStepService(jdbi);
    }

    @Bean
    OrderOuterStepService orderOuterSteps(Jdbi jdbi, OrderStepService steps) {
      return new OrderOuterStepService(jdbi, steps);
    }

    @Bean
    OrderWorkflowService orderWorkflow(
        DBOS dbos, OrderStepService steps, OrderOuterStepService outerSteps) {
      return new OrderWorkflowService(dbos, steps, outerSteps);
    }
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
        .withPropertyValues("dbos.application.name=txstep-jdbi-test")
        .withBean("dataSource", DataSource.class, () -> db.dataSource)
        .withUserConfiguration(JdbiInfraConfig.class, OrderConfig.class);
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
                var wfid = "wf-jdbi-int-golden";

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
                var wfid = "wf-jdbi-int-idem";

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
                var wfid = "wf-jdbi-int-fail";

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

  @Test
  void outsideWorkflow_stepRunsAndCommits() throws SQLException {
    try (var db = new TransactionalStepTest.TestDatabase()) {
      runner(db)
          .run(
              ctx -> {
                assertThat(ctx).hasNotFailed();
                var steps = ctx.getBean(OrderStepService.class);

                assertThat(steps.placeOrder("ord-out-1", "Widget", 5))
                    .isEqualTo(new Order("ord-out-1", "Widget", 5));

                assertThat(orderCount(db.dataSource, "ord-out-1")).isEqualTo(1);
                assertThat(TransactionalStepTest.totalTxRows(db.dataSource)).isEqualTo(0);
              });
    }
  }

  @Test
  void outsideWorkflow_runtimeException_rollsBack() throws SQLException {
    try (var db = new TransactionalStepTest.TestDatabase()) {
      runner(db)
          .run(
              ctx -> {
                assertThat(ctx).hasNotFailed();
                var steps = ctx.getBean(OrderStepService.class);

                assertThatThrownBy(() -> steps.doError("ord-out-2", "Gadget", 1))
                    .isInstanceOf(RuntimeException.class);

                assertThat(orderCount(db.dataSource, "ord-out-2")).isEqualTo(0);
                assertThat(TransactionalStepTest.totalTxRows(db.dataSource)).isEqualTo(0);
              });
    }
  }

  @Test
  void nestedTxStep_innerJoinsOuterTransaction() throws SQLException {
    try (var db = new TransactionalStepTest.TestDatabase()) {
      runner(db)
          .run(
              ctx -> {
                assertThat(ctx).hasNotFailed();
                var workflow = ctx.getBean(OrderWorkflowService.class);
                var wfid = "wf-jdbi-nested";

                try (var _o = new WorkflowOptions(wfid).setContext()) {
                  assertThat(
                          workflow.processOrderWithNested("ord-outer", "ord-inner", "Sprocket", 2))
                      .isEqualTo(new Order("ord-inner", "Sprocket", 2));
                }

                assertThat(orderCount(db.dataSource, "ord-outer")).isEqualTo(1);
                assertThat(orderCount(db.dataSource, "ord-inner")).isEqualTo(1);
                // Only the outer step writes to tx_step_outputs; inner runs in passthrough mode
                var rows = TransactionalStepTest.getTxRows(db.dataSource, wfid);
                assertThat(rows).hasSize(1);
                assertThat(rows.get(0).output()).isNotNull();
                assertThat(rows.get(0).error()).isNull();
              });
    }
  }

  @Test
  void insideDbosStep_innerJoinsOuter() throws SQLException {
    try (var db = new TransactionalStepTest.TestDatabase()) {
      runner(db)
          .run(
              ctx -> {
                assertThat(ctx).hasNotFailed();
                var workflow = ctx.getBean(OrderWorkflowService.class);
                var wfid = "wf-jdbi-dbosstep";

                try (var _o = new WorkflowOptions(wfid).setContext()) {
                  assertThat(workflow.processOrderViaDbosStep("ord-dbos", "Thing", 3))
                      .isEqualTo(new Order("ord-dbos", "Thing", 3));
                }

                assertThat(orderCount(db.dataSource, "ord-dbos")).isEqualTo(1);
                // Inner @TransactionalStep ran in passthrough mode — no tx_step_outputs entry
                assertThat(TransactionalStepTest.getTxRows(db.dataSource, wfid)).isEmpty();
              });
    }
  }
}
