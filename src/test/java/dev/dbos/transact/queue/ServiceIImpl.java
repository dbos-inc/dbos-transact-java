package dev.dbos.transact.queue;

import dev.dbos.transact.workflow.Workflow;

public class ServiceIImpl implements ServiceI {

    @Workflow(name = "workflowI")
    public Integer workflowI(int number) {
        return Integer.valueOf(number*2);
    }
}
