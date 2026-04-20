package dev.dbos.transact.spring;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.workflow.Step;
import dev.dbos.transact.workflow.Workflow;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;

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

  @Override
  public void afterSingletonsInstantiated() {
    // Collect beans with workflow methods, grouped by target class, preserving insertion order.
    Map<Class<?>, List<String>> beanNamesByClass = new LinkedHashMap<>();
    Map<String, Object> rawTargetByName = new LinkedHashMap<>();

    for (String beanName : applicationContext.getBeanDefinitionNames()) {
      Object bean;
      try {
        bean = applicationContext.getBean(beanName);
      } catch (Exception e) {
        logger.warn("Skipping bean '{}' during DBOS workflow scan: {}", beanName, e.getMessage());
        continue;
      }

      Class<?> targetClass = AopUtils.getTargetClass(bean);
      if (!hasWorkflowsOrSteps(targetClass)) {
        continue;
      }

      if (!applicationContext.isSingleton(beanName)) {
        throw new IllegalStateException(
            "Bean '"
                + beanName
                + "' ("
                + targetClass.getName()
                + ") has @Workflow or @Step methods but is not a singleton. "
                + "DBOS requires singleton beans for durable execution and workflow recovery.");
      }

      // Unwrap the Spring AOP proxy to obtain the raw target. DBOS stores this reference and
      // calls workflow methods directly on it during execution and recovery, which means those
      // invocations bypass Spring AOP and are never seen by DBOSAspect. Callers inside the
      // workflow body must use a self-injected proxy to reach @Step / @Workflow methods durably.
      Object rawTarget = AopProxyUtils.getSingletonTarget(bean);
      if (rawTarget == null) {
        rawTarget = bean;
      }

      beanNamesByClass.computeIfAbsent(targetClass, k -> new ArrayList<>()).add(beanName);
      rawTargetByName.put(beanName, rawTarget);
    }

    ConfigurableListableBeanFactory beanFactory = null;
    if (applicationContext instanceof ConfigurableApplicationContext configurableCtx) {
      beanFactory = configurableCtx.getBeanFactory();
    }

    for (Map.Entry<Class<?>, List<String>> entry : beanNamesByClass.entrySet()) {
      Class<?> targetClass = entry.getKey();
      List<String> beanNames = entry.getValue();

      // The primary bean (sole instance, or explicitly @Primary) is registered with an empty name.
      // Non-primary beans of the same class are registered under their Spring bean name.
      String primaryBeanName = findPrimaryBeanName(beanNames, beanFactory);

      for (String beanName : beanNames) {
        Object rawTarget = rawTargetByName.get(beanName);
        String registerName = beanName.equals(primaryBeanName) ? null : beanName;

        logger.debug(
            "Registering DBOS workflows from bean '{}' ({}) as '{}'",
            beanName,
            targetClass.getName(),
            registerName);

        Set<String> seen = new HashSet<>();
        for (Class<?> c = targetClass; c != null && c != Object.class; c = c.getSuperclass()) {
          for (var method : c.getDeclaredMethods()) {
            String sig = method.getName() + Arrays.toString(method.getParameterTypes());
            if (!seen.add(sig)) {
              continue;
            }
            var wfTag = method.getAnnotation(Workflow.class);
            if (wfTag != null) {
              dbos.registerWorkflow(wfTag, rawTarget, method, registerName);
            }
          }
        }
      }
    }
  }

  /**
   * Returns the bean name that should be treated as primary (registered with an empty instance
   * name). If there is only one bean for the class it is always primary. Among multiple beans the
   * one carrying {@code @Primary} wins; if none does, {@code null} is returned and every bean is
   * registered under its own name.
   */
  private static String findPrimaryBeanName(
      List<String> beanNames, ConfigurableListableBeanFactory beanFactory) {
    if (beanNames.size() == 1) {
      return beanNames.get(0);
    }
    if (beanFactory != null) {
      for (String name : beanNames) {
        try {
          if (beanFactory.getBeanDefinition(name).isPrimary()) {
            return name;
          }
        } catch (NoSuchBeanDefinitionException e) {
          // bean may be registered without a definition (e.g. manually); skip
        }
      }
    }
    return null;
  }

  private static boolean hasWorkflowsOrSteps(Class<?> targetClass) {
    for (Class<?> c = Objects.requireNonNull(targetClass);
        c != null && c != Object.class;
        c = c.getSuperclass()) {
      for (Method method : c.getDeclaredMethods()) {
        if (method.isAnnotationPresent(Workflow.class) || method.isAnnotationPresent(Step.class)) {
          return true;
        }
      }
    }
    return false;
  }
}
