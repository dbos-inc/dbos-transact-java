package dev.dbos.transact.workflow;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Step {
    String name() default "";

    boolean retriesAllowed() default true;

    int maxAttempts() default 1;

    float backOffRate() default 1.0f;
}
