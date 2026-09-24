package dev.dbos.transact.workflow;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Workflow {
  String name() default "";

  int maxRecoveryAttempts() default -1;

  /**
   * The format this workflow's arguments and result are recorded in.
   *
   * <p>Ignored for a scheduled workflow: a schedule fires inside one application, so its runs are
   * always recorded with the application's own serializer, whichever path starts them.
   */
  SerializationStrategy serializationStrategy() default SerializationStrategy.DEFAULT;
}
