package dev.dbos.transact.workflow;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Scheduled {
  String cron();

  // TODO: add scheduler mode enum + queueName params
  //       https://github.com/dbos-inc/dbos-transact-java/issues/87
}
