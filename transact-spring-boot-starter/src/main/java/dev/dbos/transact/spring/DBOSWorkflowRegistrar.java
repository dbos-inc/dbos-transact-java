package dev.dbos.transact.spring;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.workflow.Workflow;

import java.lang.reflect.Method;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;

/**
 * Scans all Spring beans after singleton initialization and registers those containing {@link
 * Workflow @Workflow} annotated methods with the DBOS workflow registry.
 *
 * <p>This runs via {@link SmartInitializingSingleton#afterSingletonsInstantiated()}, which is
 * guaranteed to fire before any {@link org.springframework.context.SmartLifecycle} beans start.
 * That ordering ensures workflows are registered before {@link DBOS#launch()} is called by {@link
 * DBOSAutoConfiguration.DBOSLifecycle}.
 *
 * <p>The raw (unwrapped) bean target is registered with DBOS rather than the Spring proxy, because
 * DBOS invokes workflow methods directly on the stored target during execution and recovery. Step
 * and child-workflow calls within a workflow body must therefore go through a self-injected Spring
 * proxy (see {@link DBOSAspect}) to be intercepted.
 *
 * <p>This bean is registered by {@link DBOSAutoConfiguration}; declare your own {@code @Bean
 * DBOSWorkflowRegistrar} to replace it.
 */
public class DBOSWorkflowRegistrar implements SmartInitializingSingleton {

  private static final Logger logger = LoggerFactory.getLogger(DBOSWorkflowRegistrar.class);

  private final DBOS dbos;
  private final ApplicationContext applicationContext;

  public DBOSWorkflowRegistrar(DBOS dbos, ApplicationContext applicationContext) {
    this.dbos = dbos;
    this.applicationContext = applicationContext;
  }

  // TODO: handle named workflow instances

  @Override
  public void afterSingletonsInstantiated() {
    for (String beanName : applicationContext.getBeanDefinitionNames()) {
      Object bean;
      try {
        bean = applicationContext.getBean(beanName);
      } catch (Exception e) {
        logger.debug("Skipping bean '{}' during DBOS workflow scan: {}", beanName, e.getMessage());
        continue;
      }

      Class<?> targetClass = AopUtils.getTargetClass(bean);
      if (!hasWorkflowMethods(targetClass)) {
        continue;
      }

      // Unwrap the Spring AOP proxy to obtain the raw target. DBOS stores this reference and
      // calls workflow methods directly on it during execution and recovery, which means those
      // invocations bypass Spring AOP and are never seen by DBOSAspect. Callers inside the
      // workflow body must use a self-injected proxy to reach @Step / @Workflow methods durably.
      Object rawTarget = AopProxyUtils.getSingletonTarget(bean);
      if (rawTarget == null) {
        rawTarget = bean;
      }

      logger.debug(
          "Registering DBOS workflows from bean '{}' ({})", beanName, targetClass.getName());

      dbos.registerClassWorkflows(rawTarget, "");
    }
  }

  private static boolean hasWorkflowMethods(Class<?> targetClass) {
    for (Method method : targetClass.getDeclaredMethods()) {
      if (method.isAnnotationPresent(Workflow.class)) {
        return true;
      }
    }
    return false;
  }
}
