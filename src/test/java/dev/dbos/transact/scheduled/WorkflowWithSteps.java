package dev.dbos.transact.scheduled;

import dev.dbos.transact.workflow.Scheduled;
import dev.dbos.transact.workflow.Workflow;

import java.time.Instant;

public class WorkflowWithSteps {

    private Steps stepService;

    public WorkflowWithSteps(Steps steps) {
        stepService = steps;
    }

    public void setStepService(Steps s) {
        stepService = s;
    }


    @Workflow(name = "everySecond")
    @Scheduled(cron = "0/4 * * * * ?")
    public void every4Second(Instant schedule , Instant actual) {
        System.out.println("Execute every 4th Second  " + schedule.toString() + "  " + actual.toString()) ;
        stepService.stepOne();
        stepService.stepTwo();
    }




}
