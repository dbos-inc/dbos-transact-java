package dev.dbos.transact.spring;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.execution.RegisteredWorkflow;
import dev.dbos.transact.workflow.Step;
import dev.dbos.transact.workflow.StepOptions;
import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.WorkflowClassName;

import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.support.AopUtils;

/**
 * Spring AOP aspect that intercepts {@link Workflow @Workflow} and {@link Step @Step} annotated
 * methods on Spring-managed beans and delegates execution to DBOS.
 *
 * <p>When a {@code @Workflow} method is called through a Spring proxy, this aspect intercepts the
 * call and routes it through {@code DBOS.invokeWorkflow()} so the execution is durably recorded and
 * recoverable. Step calls made with {@code @Step} inside a workflow body are similarly intercepted
 * and executed as DBOS steps.
 *
 * <p><strong>Important:</strong> Spring AOP only intercepts calls made through the Spring proxy.
 * Calls to {@code this.someStep()} inside a workflow method body bypass the proxy and are therefore
 * not intercepted. To ensure step and child-workflow calls are durable, inject a self-reference via
 * {@code @Autowired} and call through it:
 *
 * <pre>{@code
 * @Service
 * public class MyService {
 *     @Autowired MyService self;
 *
 *     @Workflow
 *     public String myWorkflow() {
 *         return self.myStep(); // intercepted by DBOSAspect
 *     }
 *
 *     @Step
 *     public String myStep() { ... }
 * }
 * }</pre>
 *
 * <p>This bean is registered by {@link DBOSAutoConfiguration}; declare your own {@code @Bean
 * DBOSAspect} to replace it.
 */
@Aspect
public class DBOSAspect {

  private static final Logger logger = LoggerFactory.getLogger(DBOSAspect.class);

  private final DBOS dbos;
  private final ConcurrentHashMap<Method, RegisteredWorkflow> workflowCache =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<Method, StepOptions> stepCache = new ConcurrentHashMap<>();

  public DBOSAspect(DBOS dbos) {
    this.dbos = dbos;
  }

  static Method getMethod(ProceedingJoinPoint pjp) {
    return AopUtils.getMostSpecificMethod(
        ((MethodSignature) pjp.getSignature()).getMethod(), pjp.getTarget().getClass());
  }

  static String getMethodName(ProceedingJoinPoint pjp) {
    return ((MethodSignature) pjp.getSignature()).getName();
  }

  // TODO: handle named workflow instances

  /**
   * Intercepts {@link Workflow @Workflow} annotated methods and routes them through DBOS for
   * durable execution. The workflow is looked up by the target's class name (or its {@link
   * WorkflowClassName} alias) and the method name (or the annotation's {@code name} attribute).
   */
  @Around("@annotation(workflow)")
  public Object aroundWorkflow(ProceedingJoinPoint pjp, Workflow workflow) throws Throwable {
    var regWf =
        workflowCache.computeIfAbsent(
            getMethod(pjp),
            m -> {
              var klass = pjp.getTarget().getClass();
              var classTag = klass.getAnnotation(WorkflowClassName.class);
              var className =
                  (classTag == null || classTag.value().isEmpty())
                      ? klass.getName()
                      : classTag.value();
              var workflowName = workflow.name().isEmpty() ? getMethodName(pjp) : workflow.name();
              return dbos.getRegisteredWorkflow(workflowName, className)
                  .orElseThrow(
                      () ->
                          new IllegalStateException(
                              "No registered workflow found for %s.%s"
                                  .formatted(className, workflowName)));
            });

    logger.debug("Intercepting @Workflow {}", regWf.fullyQualifiedName());
    var handle = dbos.startWorkflow(regWf, pjp.getArgs(), null);
    return handle.getResult();
  }

  /**
   * Intercepts {@link Step @Step} annotated methods. When called inside a workflow context the
   * execution is delegated to DBOS so it is recorded as a durable step. When called outside a
   * workflow context the method is executed directly without DBOS involvement.
   */
  @Around("@annotation(step)")
  public Object aroundStep(ProceedingJoinPoint pjp, Step step) throws Throwable {
    var stepOptions =
        stepCache.computeIfAbsent(
            getMethod(pjp),
            m ->
                new StepOptions(
                    step.name().isEmpty() ? getMethodName(pjp) : step.name(),
                    step.retriesAllowed(),
                    step.maxAttempts(),
                    step.intervalSeconds(),
                    step.backOffRate()));

    logger.debug("Intercepting @Step {}", stepOptions.name());
    try {
      return dbos.runStep(
          () -> {
            try {
              return pjp.proceed();
            } catch (Exception e) {
              throw e;
            } catch (Throwable t) {
              throw new WrappedThrowableException(t);
            }
          },
          stepOptions);
    } catch (WrappedThrowableException e) {
      // Unwrap and rethrow the original non-Exception Throwable
      throw e.getWrappedThrowable();
    }
  }
}
