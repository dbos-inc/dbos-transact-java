package dev.dbos.transact.scheduled;

import dev.dbos.transact.workflow.Scheduled;
import dev.dbos.transact.workflow.Workflow;

import java.time.Instant;

public class EveryThirdSec {
    public volatile int wfCounter = 0;

    public EveryThirdSec() {
    }

    @Workflow(name = "everyThird")
    @Scheduled(cron = "0/3 * * * * ?")
    public void everyThird(Instant schedule, Instant actual) {
        ++wfCounter;
        System.out.println("Execute count " + wfCounter + "  " + schedule.toString()
                + "   " + actual.toString());
    }
}
