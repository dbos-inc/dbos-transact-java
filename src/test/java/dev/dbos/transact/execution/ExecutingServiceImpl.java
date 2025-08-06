package dev.dbos.transact.execution;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.workflow.Step;
import dev.dbos.transact.workflow.Workflow;

public class ExecutingServiceImpl implements ExecutingService {

    private ExecutingService executingService;
    public static int step1Count = 0;
    public static int step2Count = 0;
    public final DBOS dbos;

    public ExecutingServiceImpl(DBOS dbos) {
        this.dbos = dbos;
    }

    public ExecutingServiceImpl() {
        dbos = null;
    }

    public void setExecutingService(ExecutingService service) {
        this.executingService = service;
    }

    @Workflow(name = "workflowMethod")
    public String workflowMethod(String input) {
        return input + input;
    }

    @Workflow(name = "workflowMethodWithStep")
    public String workflowMethodWithStep(String input) {
        String step1Response = executingService.stepOne("stepOne");
        String step2Response = executingService.stepTwo("stepTwo");
        return input + step1Response + step2Response;
    }

    @Step(name = "stepOne")
    public String stepOne(String input) {
        ++step1Count;
        return input;
    }

    @Step(name = "stepTwo")
    public String stepTwo(String input) {
        ++step2Count;
        return input;
    }

    @Workflow(name = "sleepingWorkflow")
    public void sleepingWorkflow(float seconds) {
        dbos.sleep(seconds);
    }
}
