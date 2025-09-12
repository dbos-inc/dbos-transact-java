package dev.dbos.transact.scheduled;

import dev.dbos.transact.workflow.Scheduled;
import dev.dbos.transact.workflow.Workflow;

import java.time.Instant;

public class TimedWorkflow {
  volatile Instant scheduled;
  volatile Instant actual;

  public TimedWorkflow() {}

  @Workflow(name = "everyfourth")
  @Scheduled(cron = "0/4 * * * * ?")
  public void everyfourth(Instant scheduled, Instant actual) {
    this.scheduled = scheduled;
    this.actual = actual;
    System.out.println("Execute " + scheduled.toString() + "   " + actual.toString());
  }
}
