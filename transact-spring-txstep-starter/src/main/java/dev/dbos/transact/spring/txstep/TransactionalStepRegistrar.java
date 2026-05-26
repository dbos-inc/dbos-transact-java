package dev.dbos.transact.spring.txstep;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.annotation.AnnotatedElementUtils;

/**
 * Scans all Spring beans after singleton initialization for {@link
 * TransactionalStep @TransactionalStep} annotated methods. If at least one is found, calls {@link
 * TransactionalStepFactory#initialize()} to verify PostgreSQL and create the {@code
 * tx_step_outputs} table. If none are found, no database contact occurs.
 *
 * <p>Bean types are resolved without triggering instantiation: already-created singletons are
 * looked up directly from the singleton registry; beans not yet instantiated (e.g. lazy beans) are
 * inspected via {@link ConfigurableListableBeanFactory#getType(String, boolean)} with {@code
 * allowFactoryBeanInit=false}.
 *
 * <p>This bean is registered by {@link TransactionalStepAutoConfiguration}.
 */
public class TransactionalStepRegistrar implements SmartInitializingSingleton, BeanFactoryAware {

  private static final Logger logger = LoggerFactory.getLogger(TransactionalStepRegistrar.class);

  private final TransactionalStepFactory factory;
  private ConfigurableListableBeanFactory beanFactory;

  public TransactionalStepRegistrar(TransactionalStepFactory factory) {
    this.factory = factory;
  }

  @Override
  public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
    if (!(beanFactory instanceof ConfigurableListableBeanFactory clbf)) {
      throw new IllegalArgumentException(
          "TransactionalStepRegistrar requires a ConfigurableListableBeanFactory");
    }
    this.beanFactory = clbf;
  }

  @Override
  public void afterSingletonsInstantiated() {
    for (String beanName : beanFactory.getBeanDefinitionNames()) {
      Class<?> typeToCheck;

      // If the singleton is already created, use its actual class (unwrapping any AOP proxy).
      // This avoids re-instantiation and correctly handles both CGLIB and JDK dynamic proxies.
      Object existing = beanFactory.getSingleton(beanName);
      if (existing != null) {
        typeToCheck = AopUtils.getTargetClass(existing);
      } else {
        // Bean not yet instantiated (e.g. lazy). Resolve from the bean definition without
        // creating the bean; null means the type cannot be determined statically — skip it.
        typeToCheck = beanFactory.getType(beanName, false);
      }

      if (typeToCheck != null && hasTransactionalStep(typeToCheck)) {
        logger.debug(
            "Found @TransactionalStep in bean '{}'; initializing tx_step_outputs", beanName);
        factory.initialize();
        return;
      }
    }
    logger.debug("No @TransactionalStep methods found; skipping tx_step_outputs initialization");
  }

  private static boolean hasTransactionalStep(Class<?> targetClass) {
    return !MethodIntrospector.selectMethods(
            targetClass,
            (MethodIntrospector.MetadataLookup<?>)
                method ->
                    AnnotatedElementUtils.hasAnnotation(method, TransactionalStep.class)
                        ? Boolean.TRUE
                        : null)
        .isEmpty();
  }
}
