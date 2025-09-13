package dev.dbos.transact.scheduled;

import dev.dbos.transact.workflow.Scheduled;
import dev.dbos.transact.workflow.Workflow;

import java.time.Instant;

public class MultipleWorkflows {

  public volatile int wfCounter = 0;
  public volatile int wfCounter3 = 0;

  public MultipleWorkflows() {}

  @Workflow(name = "everySecond")
  @Scheduled(cron = "0/1 * * * * ?")
  public void everySecond(Instant schedule, Instant actual) {
    ++wfCounter;
    System.out.println(
        "Execute count " + wfCounter + "  " + schedule.toString() + "   " + actual.toString());
  }

  @Workflow(name = "everyThird")
  @Scheduled(cron = "0/3 * * * * ?")
  public void every(Instant schedule, Instant actual) {
    ++wfCounter3;
    System.out.println(
        "Execute count " + wfCounter3 + "  " + schedule.toString() + "   " + actual.toString());
  }
}
