package dev.dbos.transact.queue;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.dbos.transact.workflow.Workflow;
import java.time.Instant;

public class ServiceQImpl implements ServiceQ {

  @Workflow(name = "simpleQWorkflow")
  public String simpleQWorkflow(String input) {
    return input + input;
  }

  @Workflow(name = "limitWorkflow")
  public Double limitWorkflow(String var1, String var2) {
    // Assertions as in Python test
    assertEquals("abc", var1, "var1 should be 'abc'");
    assertEquals("123", var2, "var2 should be '123'");

    // Return current time in seconds (float equivalent)
    return Instant.now().toEpochMilli() / 1000.0;
  }
}
