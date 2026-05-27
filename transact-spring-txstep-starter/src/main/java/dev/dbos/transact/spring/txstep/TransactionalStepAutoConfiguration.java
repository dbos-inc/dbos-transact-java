package dev.dbos.transact.spring.txstep;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.spring.DBOSAutoConfiguration;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * Spring Boot auto-configuration for {@link TransactionalStep @TransactionalStep} support.
 *
 * <p>Activates when a {@link DBOS} bean, a {@link PlatformTransactionManager} bean, and a {@link
 * DataSource} bean are all present. Creates {@link TransactionalStepFactory}, {@link
 * TransactionalStepAspect}, and {@link TransactionalStepRegistrar} beans.
 */
@AutoConfiguration(
    after = DBOSAutoConfiguration.class,
    afterName = {
      // Ensure the JDBC and JPA transaction manager auto-configurations have registered their
      // PlatformTransactionManager bean definitions before our @ConditionalOnBean check runs.
      // Spring Boot 3.x class names:
      "org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration",
      "org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration",
      // Spring Boot 4.x class names (auto-configurations moved to dedicated modules):
      "org.springframework.boot.jdbc.autoconfigure.DataSourceTransactionManagerAutoConfiguration",
      "org.springframework.boot.hibernate.autoconfigure.HibernateJpaAutoConfiguration"
    })
@ConditionalOnBean({DBOS.class, PlatformTransactionManager.class, DataSource.class})
@EnableConfigurationProperties(TransactionalStepProperties.class)
public class TransactionalStepAutoConfiguration {

  private static final Logger logger =
      LoggerFactory.getLogger(TransactionalStepAutoConfiguration.class);

  @Bean
  public TransactionalStepFactory springTransactionalStepFactory(
      DBOS dbos,
      DataSource dataSource,
      PlatformTransactionManager txManager,
      TransactionalStepProperties properties) {
    logger.info(
        "TransactionalStepAutoConfiguration activated; txManager={} dataSource={}",
        txManager.getClass().getName(),
        dataSource.getClass().getName());
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
}
