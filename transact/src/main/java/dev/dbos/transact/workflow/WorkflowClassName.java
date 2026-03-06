package dev.dbos.transact.workflow;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to specify a custom class name for workflow registration. This allows workflows to be
 * registered with a portable, language-agnostic name instead of the Java class name.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * @WorkflowClassName("MyService")
 * public class MyServiceImpl implements MyService {
 *     @Workflow
 *     public String myWorkflow() { ... }
 * }
 * }</pre>
 *
 * <p>This workflow would be registered as "MyService//myWorkflow" instead of
 * "com.example.MyServiceImpl//myWorkflow".
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface WorkflowClassName {
  /** The custom class name to use for workflow registration. */
  String value();
}
