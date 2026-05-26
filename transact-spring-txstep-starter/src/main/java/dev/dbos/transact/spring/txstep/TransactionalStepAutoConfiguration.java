package dev.dbos.transact.spring.txstep;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.spring.DBOSAutoConfiguration;

import javax.sql.DataSource;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * Spring Boot auto-configuration for {@link TransactionalStep @TransactionalStep} support.
 *
 * <p>Activates when both a {@link DBOS} bean and a {@link PlatformTransactionManager} bean are
 * present. Creates {@link TransactionalStepFactory}, {@link TransactionalStepAspect}, and {@link
 * TransactionalStepRegistrar} beans.
 *
 * <p>If {@code JpaTransactionManager} is detected and its {@code dataSource} property is not yet
 * set, it is set automatically so that {@code DataSourceUtils.getConnection()} returns the
 * transaction-bound connection inside {@code @TransactionalStep} methods.
 */
@AutoConfiguration(after = DBOSAutoConfiguration.class)
@ConditionalOnBean({DBOS.class, PlatformTransactionManager.class})
@EnableConfigurationProperties(TransactionalStepProperties.class)
public class TransactionalStepAutoConfiguration {

  @Bean
  public TransactionalStepFactory springTransactionalStepFactory(
      DBOS dbos,
      DataSource dataSource,
      PlatformTransactionManager txManager,
      TransactionalStepProperties properties) {
    return new TransactionalStepFactory(dbos, dataSource, txManager, properties.getSchema());
  }

  @Bean
  public TransactionalStepAspect transactionalStepAspect(TransactionalStepFactory factory) {
    return new TransactionalStepAspect(factory);
  }

  @Bean
  public TransactionalStepRegistrar transactionalStepRegistrar(TransactionalStepFactory factory) {
    return new TransactionalStepRegistrar(factory);
  }

  /**
   * Applies the JPA bridge when {@code spring-orm} is on the classpath. If a {@code
   * JpaTransactionManager} is in use and its {@code dataSource} property is not set, sets it so
   * that {@code DataSourceUtils.getConnection(dataSource)} returns the transaction-bound connection
   * inside {@code @TransactionalStep} methods.
   */
  @Configuration(proxyBeanMethods = false)
  @ConditionalOnClass(name = "org.springframework.orm.jpa.JpaTransactionManager")
  static class JpaBridgeConfiguration {
    @Bean
    JpaBridgeApplier jpaBridgeApplier(PlatformTransactionManager txManager, DataSource dataSource) {
      if (txManager instanceof org.springframework.orm.jpa.JpaTransactionManager jpa
          && jpa.getDataSource() == null) {
        jpa.setDataSource(dataSource);
      }
      return new JpaBridgeApplier();
    }

    record JpaBridgeApplier() {}
  }
}
