package dev.dbos.transact.scheduled;

import dev.dbos.transact.workflow.Scheduled;
import dev.dbos.transact.workflow.Workflow;
import java.time.Instant;

public class EveryMinute {

  @Workflow(name = "everyMinute")
  @Scheduled(cron = "0 * * * * ?")
  public void everyMinute(Instant schedule, Instant actual) {
    System.out.println("Executing everyMinute " + schedule.toString() + "   " + actual.toString());
  }
}
