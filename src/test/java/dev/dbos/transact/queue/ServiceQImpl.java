package dev.dbos.transact.queue;

import dev.dbos.transact.workflow.Workflow;

public class ServiceQImpl implements ServiceQ {

    @Workflow(name = "simpleQWorkflow")
    public String simpleQWorkflow(String input) {
        return input+input;
    }
}
