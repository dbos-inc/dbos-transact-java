package dev.dbos.transact.spring.txstep;

import dev.dbos.transact.spring.WrappedThrowableException;
import dev.dbos.transact.txstep.SpringTransactionalStepFactory;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;

/**
 * AOP aspect that intercepts {@link TransactionalStep @TransactionalStep} annotated methods and
 * delegates execution to {@link SpringTransactionalStepFactory}.
 *
 * <p>This bean is registered by {@link TransactionalStepAutoConfiguration}.
 */
@Aspect
public class TransactionalStepAspect {

  private final SpringTransactionalStepFactory factory;

  public TransactionalStepAspect(SpringTransactionalStepFactory factory) {
    this.factory = factory;
  }

  @Around("@annotation(transactionalStep)")
  public Object aroundTransactionalStep(
      ProceedingJoinPoint pjp, TransactionalStep transactionalStep) throws Throwable {
    String stepName = transactionalStep.name();
    if (stepName.isEmpty()) {
      stepName = ((MethodSignature) pjp.getSignature()).getName();
    }
    String resolvedName = stepName;
    try {
      return factory.runTransactionalStep(
          () -> {
            try {
              return pjp.proceed();
            } catch (Exception e) {
              throw e;
            } catch (Throwable t) {
              throw new WrappedThrowableException(t);
            }
          },
          resolvedName);
    } catch (WrappedThrowableException e) {
      throw e.getWrappedThrowable();
    }
  }
}
