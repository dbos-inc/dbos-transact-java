package dev.dbos.transact.execution;

import dev.dbos.transact.workflow.Step;
import dev.dbos.transact.workflow.Workflow;

public class ExecutingServiceImpl implements ExecutingService {

    private ExecutingService executingService;

    public void setExecutingService(ExecutingService service) {
        this.executingService = service ;
    }

    @Workflow(name = "workflowMethod")
    public String workflowMethod(String input) {
        return input+input;
    }

    @Workflow(name = "workflowMethodWithStep")
    public String workflowMethodWithStep(String input) {
        String stepResponse = executingService.simpleStep("stepOne");
        return input+stepResponse;
    }

    @Step(name = "simpleStep")
    public String simpleStep(String input)  {
        return input ;
    }

}
