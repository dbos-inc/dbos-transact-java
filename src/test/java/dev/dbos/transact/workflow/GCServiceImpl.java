package dev.dbos.transact.workflow;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.context.DBOSContextHolder;
import dev.dbos.transact.utils.ManualResetEvent;

public class GCServiceImpl implements GCService {

    GCService self;
    ManualResetEvent event = new ManualResetEvent(false);

    public void setGCService(GCService service) {
        this.self = service;
    }

    @Step
    public int testStep(int x) {
        return x;
    }

    @Workflow
    public int testWorkflow(int x) {
        self.testStep(x);
        return x;
    }

    @Workflow
    public String blockedWorkflow() {
        event.waitOne();
        // TODO: should we be retrieving this directly from DBOS Context in Java?
        //       in TS & Python there is workflowID getter off the DBOS global static
        //       not sure what the plan is for Java
        return DBOSContextHolder.get().getWorkflowId();
    }
    
}
