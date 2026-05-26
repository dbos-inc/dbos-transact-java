package dev.dbos.transact.spring.txstep;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.spring.DBOSAutoConfiguration;
import dev.dbos.transact.workflow.Workflow;

import java.sql.SQLException;
import java.util.Properties;

import javax.sql.DataSource;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.jpa.HibernatePersistenceProvider;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.orm.jpa.EntityManagerFactoryUtils;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.transaction.PlatformTransactionManager;

public class TransactionalStepJpaIntegrationTest {

  // ---- JPA entity ----

  @Entity
  @Table(name = "greetings")
  public static class Greeting {
    @Id private String name;

    @Column(nullable = false)
    private int count;

    protected Greeting() {}

    Greeting(String name, int count) {
      this.name = name;
      this.count = count;
    }

    int count() {
      return count;
    }
  }

  // ---- Step bean: @TransactionalStep methods using JPA EntityManager ----

  public static class GreetingSteps {
    private final EntityManagerFactory emf;

    GreetingSteps(EntityManagerFactory emf) {
      this.emf = emf;
    }

    @TransactionalStep
    public String doInsert(String name) {
      var em = EntityManagerFactoryUtils.getTransactionalEntityManager(emf);
      var g = em.find(Greeting.class, name);
      if (g == null) {
        em.persist(new Greeting(name, 1));
      } else {
        g.count++;
      }
      return name;
    }

    @TransactionalStep
    public String doError(String name) {
      var em = EntityManagerFactoryUtils.getTransactionalEntityManager(emf);
      var g = em.find(Greeting.class, name);
      if (g == null) {
        em.persist(new Greeting(name, 1));
      } else {
        g.count++;
      }
      throw new RuntimeException("intentional failure");
    }
  }

  // ---- Workflow bean: @Workflow methods calling the step bean through its Spring proxy ----

  public static class GreetingWorkflow {
    private final GreetingSteps steps;

    GreetingWorkflow(GreetingSteps steps) {
      this.steps = steps;
    }

    @Workflow
    public String insert(String name) {
      return steps.doInsert(name);
    }

    @Workflow
    public String error(String name) {
      return steps.doError(name);
    }
  }

  // ---- Spring configuration registering the two beans ----

  @Configuration(proxyBeanMethods = false)
  static class GreetingConfig {
    @Bean
    GreetingSteps greetingSteps(EntityManagerFactory emf) {
      return new GreetingSteps(emf);
    }

    @Bean
    GreetingWorkflow greetingWorkflow(GreetingSteps steps) {
      return new GreetingWorkflow(steps);
    }
  }

  // ---- Infrastructure helpers ----

  private static EntityManagerFactory buildEmf(DataSource dataSource) {
    var emfBean = new LocalContainerEntityManagerFactoryBean();
    emfBean.setDataSource(dataSource);
    emfBean.setPackagesToScan("dev.dbos.transact.spring.txstep");
    emfBean.setJpaVendorAdapter(new HibernateJpaVendorAdapter());
    emfBean.setPersistenceProviderClass(HibernatePersistenceProvider.class);
    var props = new Properties();
    props.put("hibernate.hbm2ddl.auto", "update");
    props.put("hibernate.dialect", "org.hibernate.dialect.PostgreSQLDialect");
    emfBean.setJpaProperties(props);
    emfBean.afterPropertiesSet();
    return emfBean.getObject();
  }

  private static JpaTransactionManager buildJpaTransactionManager(
      DataSource dataSource, EntityManagerFactory emf) {
    var txManager = new JpaTransactionManager(emf);
    txManager.setDataSource(dataSource);
    txManager.afterPropertiesSet();
    return txManager;
  }

  private static ApplicationContextRunner runner(
      TransactionalStepTest.TestDatabase db,
      EntityManagerFactory emf,
      JpaTransactionManager jpaManager) {
    return new ApplicationContextRunner()
        .withConfiguration(
            AutoConfigurations.of(
                DBOSAutoConfiguration.class, TransactionalStepAutoConfiguration.class))
        .withPropertyValues("dbos.application.name=txstep-jpa-test")
        .withBean("dataSource", DataSource.class, () -> db.dataSource)
        .withBean("entityManagerFactory", EntityManagerFactory.class, () -> emf)
        .withBean("transactionManager", PlatformTransactionManager.class, () -> jpaManager)
        .withUserConfiguration(GreetingConfig.class);
  }

  // ---- Tests ----

  @Test
  void autoConfig_createsExpectedBeans() {
    try (var db = new TransactionalStepTest.TestDatabase()) {
      var emf = buildEmf(db.dataSource);
      var jpaManager = buildJpaTransactionManager(db.dataSource, emf);
      runner(db, emf, jpaManager)
          .run(
              ctx -> {
                assertThat(ctx).hasNotFailed();
                assertThat(ctx).hasSingleBean(DBOS.class);
                assertThat(ctx).hasSingleBean(TransactionalStepFactory.class);
                assertThat(ctx).hasSingleBean(TransactionalStepAspect.class);
                assertThat(ctx).hasSingleBean(TransactionalStepRegistrar.class);
              });
      emf.close();
    }
  }

  @Test
  void goldenPath() throws SQLException {
    try (var db = new TransactionalStepTest.TestDatabase()) {
      var emf = buildEmf(db.dataSource);
      var jpaManager = buildJpaTransactionManager(db.dataSource, emf);
      runner(db, emf, jpaManager)
          .run(
              ctx -> {
                assertThat(ctx).hasNotFailed();
                var workflow = ctx.getBean(GreetingWorkflow.class);
                var wfid = "wf-jpa-int-golden";

                try (var _o = new WorkflowOptions(wfid).setContext()) {
                  assertThat(workflow.insert("alice")).isEqualTo("alice");
                }

                assertThat(TransactionalStepTest.greetCount(db.dataSource, "alice")).isEqualTo(1);
                var rows = TransactionalStepTest.getTxRows(db.dataSource, wfid);
                assertThat(rows).hasSize(1);
                assertThat(rows.get(0).output()).isNotNull();
                assertThat(rows.get(0).error()).isNull();
              });
      emf.close();
    }
  }

  @Test
  void idempotency() throws SQLException {
    try (var db = new TransactionalStepTest.TestDatabase()) {
      var emf = buildEmf(db.dataSource);
      var jpaManager = buildJpaTransactionManager(db.dataSource, emf);
      runner(db, emf, jpaManager)
          .run(
              ctx -> {
                assertThat(ctx).hasNotFailed();
                var workflow = ctx.getBean(GreetingWorkflow.class);
                var wfid = "wf-jpa-int-idem";

                try (var _o = new WorkflowOptions(wfid).setContext()) {
                  workflow.insert("bob");
                }
                try (var _o = new WorkflowOptions(wfid).setContext()) {
                  var result = workflow.insert("bob");
                  assertThat(result).isEqualTo("bob");
                }

                assertThat(TransactionalStepTest.greetCount(db.dataSource, "bob")).isEqualTo(1);
                assertThat(TransactionalStepTest.getTxRows(db.dataSource, wfid)).hasSize(1);
              });
      emf.close();
    }
  }

  @Test
  void atomicityOnFailure() throws SQLException {
    try (var db = new TransactionalStepTest.TestDatabase()) {
      var emf = buildEmf(db.dataSource);
      var jpaManager = buildJpaTransactionManager(db.dataSource, emf);
      runner(db, emf, jpaManager)
          .run(
              ctx -> {
                assertThat(ctx).hasNotFailed();
                var workflow = ctx.getBean(GreetingWorkflow.class);
                var wfid = "wf-jpa-int-fail";

                try (var _o = new WorkflowOptions(wfid).setContext()) {
                  assertThatThrownBy(() -> workflow.error("charlie"))
                      .isInstanceOf(RuntimeException.class);
                }

                assertThat(TransactionalStepTest.greetCount(db.dataSource, "charlie")).isEqualTo(0);
                var rows = TransactionalStepTest.getTxRows(db.dataSource, wfid);
                assertThat(rows).hasSize(1);
                assertThat(rows.get(0).output()).isNull();
                assertThat(rows.get(0).error()).isNotNull();
              });
      emf.close();
    }
  }

  @Test
  void customSchema_property_tableCreatedInCustomSchema() throws SQLException {
    try (var db = new TransactionalStepTest.TestDatabase()) {
      var emf = buildEmf(db.dataSource);
      var jpaManager = buildJpaTransactionManager(db.dataSource, emf);
      runner(db, emf, jpaManager)
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
      emf.close();
    }
  }
}
