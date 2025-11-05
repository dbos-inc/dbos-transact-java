package dev.dbos.transact.workflow;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Step {
  String name() default "";

  boolean retriesAllowed() default false;

  double intervalSeconds() default StepOptions.DEFAULT_INTERVAL_SECONDS;

  int maxAttempts() default 3;

  double backOffRate() default StepOptions.DEFAULT_BACKOFF;
}
