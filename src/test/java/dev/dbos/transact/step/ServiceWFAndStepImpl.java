package dev.dbos.transact.step;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.workflow.Step;
import dev.dbos.transact.workflow.StepOptions;
import dev.dbos.transact.workflow.Workflow;

public class ServiceWFAndStepImpl implements ServiceWFAndStep {

    private ServiceWFAndStep self;
    private DBOS dbos;

    public void setSelf(ServiceWFAndStep serviceWFAndStep) {
        self = serviceWFAndStep;
    }

    public void setDbos(DBOS dbos) {
        this.dbos = dbos;
    }

    @Workflow(name = "myworkflow")
    public String aWorkflow(String input) {

        String s1 = self.stepOne("one");
        String s2 = self.stepTwo("two");
        return input + s1 + s2;
    }

    @Step(name = "step1")
    public String stepOne(String input) {
        return input;
    }

    @Step(name = "step2")
    public String stepTwo(String input) {
        return input;
    }

    @Workflow(name = "aWorkflowWithInlineSteps")
    public String aWorkflowWithInlineSteps(String input) {
        var len = dbos.runStep(() -> input.length(), new StepOptions("stringLength"));
        return (input + len);
    }
}
