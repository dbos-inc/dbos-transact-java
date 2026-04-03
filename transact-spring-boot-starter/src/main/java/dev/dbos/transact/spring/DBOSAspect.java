package dev.dbos.transact.spring;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.workflow.Step;
import dev.dbos.transact.workflow.StepOptions;
import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.WorkflowClassName;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  public DBOSAspect(DBOS dbos) {
    this.dbos = dbos;
  }

  /**
   * Intercepts {@link Workflow @Workflow} annotated methods and routes them through DBOS for
   * durable execution. The workflow is looked up by the target's class name (or its {@link
   * WorkflowClassName} alias) and the method name (or the annotation's {@code name} attribute).
   */
  @Around("@annotation(workflow)")
  public Object aroundWorkflow(ProceedingJoinPoint pjp, Workflow workflow) throws Throwable {
    Object target = pjp.getTarget();
    MethodSignature sig = (MethodSignature) pjp.getSignature();

    WorkflowClassName classNameAnn = target.getClass().getAnnotation(WorkflowClassName.class);
    String className =
        (classNameAnn != null && !classNameAnn.value().isEmpty())
            ? classNameAnn.value()
            : target.getClass().getName();

    String workflowName = workflow.name().isEmpty() ? sig.getName() : workflow.name();

    logger.debug("Intercepting @Workflow {}.{}", className, workflowName);

    // TODO: requires DBOS.invokeWorkflow(String className, String instanceName,
    //       String workflowName, Object[] args) to be added as a public method.
    // var handle = dbos.invokeWorkflow(className, "", workflowName, pjp.getArgs());
    // return handle.getResult();
    throw new RuntimeException("not impl");
  }

  /**
   * Intercepts {@link Step @Step} annotated methods. When called inside a workflow context the
   * execution is delegated to DBOS so it is recorded as a durable step. When called outside a
   * workflow context the method is executed directly without DBOS involvement.
   */
  @Around("@annotation(step)")
  public Object aroundStep(ProceedingJoinPoint pjp, Step step) throws Throwable {
    MethodSignature sig = (MethodSignature) pjp.getSignature();
    String stepName = step.name().isEmpty() ? sig.getName() : step.name();
    StepOptions opts =
        new StepOptions(
            stepName,
            step.retriesAllowed(),
            step.maxAttempts(),
            step.intervalSeconds(),
            step.backOffRate());

    logger.debug("Intercepting @Step {}", stepName);

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
          opts);
    } catch (WrappedThrowableException e) {
      // Unwrap and rethrow the original non-Exception Throwable
      throw e.getWrappedThrowable();
    }
  }
}
