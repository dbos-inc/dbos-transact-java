package dev.dbos.transact.spring.txstep;

import dev.dbos.transact.txstep.SpringTransactionalStepFactory;

import java.lang.reflect.Method;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

/**
 * Scans all Spring beans after singleton initialization for {@link
 * TransactionalStep @TransactionalStep} annotated methods. If at least one is found, calls {@link
 * SpringTransactionalStepFactory#initialize()} to verify PostgreSQL and create the {@code
 * tx_step_outputs} table. If none are found, no database contact occurs.
 *
 * <p>This bean is registered by {@link TransactionalStepAutoConfiguration}.
 */
public class TransactionalStepRegistrar
    implements SmartInitializingSingleton, ApplicationContextAware {

  private static final Logger logger = LoggerFactory.getLogger(TransactionalStepRegistrar.class);

  private final SpringTransactionalStepFactory factory;
  private ApplicationContext applicationContext;

  public TransactionalStepRegistrar(SpringTransactionalStepFactory factory) {
    this.factory = factory;
  }

  @Override
  public void setApplicationContext(ApplicationContext applicationContext) {
    this.applicationContext = applicationContext;
  }

  @Override
  public void afterSingletonsInstantiated() {
    for (String beanName : applicationContext.getBeanDefinitionNames()) {
      Object bean;
      try {
        bean = applicationContext.getBean(beanName);
      } catch (Exception e) {
        logger.warn(
            "Skipping bean '{}' during @TransactionalStep scan: {}", beanName, e.getMessage());
        continue;
      }
      if (hasTransactionalStep(AopUtils.getTargetClass(bean))) {
        logger.debug(
            "Found @TransactionalStep in bean '{}'; initializing tx_step_outputs", beanName);
        factory.initialize();
        return;
      }
    }
    logger.debug("No @TransactionalStep methods found; skipping tx_step_outputs initialization");
  }

  private static boolean hasTransactionalStep(Class<?> targetClass) {
    for (Class<?> c = targetClass; c != null && c != Object.class; c = c.getSuperclass()) {
      for (Method method : c.getDeclaredMethods()) {
        if (method.isAnnotationPresent(TransactionalStep.class)) {
          return true;
        }
      }
    }
    return false;
  }
}
