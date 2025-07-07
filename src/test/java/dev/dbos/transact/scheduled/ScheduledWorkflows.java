package dev.dbos.transact.scheduled;

import dev.dbos.transact.workflow.Scheduled;
import dev.dbos.transact.workflow.Workflow;

import java.time.Instant;

public class ScheduledWorkflows {

    public volatile int wfCounter = 0 ;

    public ScheduledWorkflows() {


    }

    @Workflow(name = "everySecond")
    @Scheduled(cron = "0/1 * * * * ?")
    public void everySecond(Instant schedule , Instant actual) {
        ++wfCounter;
        System.out.println("Execute count "+wfCounter + "  " + schedule.toString() + "   " + actual.toString()) ;
    }
}
