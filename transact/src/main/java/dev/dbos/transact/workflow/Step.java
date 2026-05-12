package dev.dbos.transact.workflow;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Step {
  String name() default "";

  int maxAttempts() default 1;

  double intervalSeconds() default StepOptions.DEFAULT_INTERVAL_SECONDS;

  double backOffRate() default StepOptions.DEFAULT_BACKOFF;
}
