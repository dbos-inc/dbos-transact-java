package dev.dbos.transact.scheduled;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Scheduled {
  String cron();

  String queueName() default "";

  SchedulerMode mode() default SchedulerMode.ExactlyOncePerIntervalWhenActive;
}
