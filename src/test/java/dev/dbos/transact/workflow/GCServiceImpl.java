package dev.dbos.transact.workflow;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.context.DBOSContextHolder;

import java.util.concurrent.CountDownLatch;

public class GCServiceImpl implements GCService {

    GCService self;
    CountDownLatch gcLatch = new CountDownLatch(1);
    CountDownLatch timeoutLatch = new CountDownLatch(1);

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
    public String gcBlockedWorkflow() {
        try {
            gcLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }

        // TODO: should we be retrieving this directly from DBOS Context in Java?
        // in TS & Python there is workflowID getter off the DBOS global static
        // not sure what the plan is for Java
        return DBOSContextHolder.get().getWorkflowId();
    }

    @Workflow
    public String timeoutBlockedWorkflow() {
        while (timeoutLatch.getCount() > 0) {
            DBOS.getInstance().sleep(0.1f);
        }
        return DBOSContextHolder.get().getWorkflowId();
    }

}
