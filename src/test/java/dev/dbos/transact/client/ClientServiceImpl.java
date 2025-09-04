package dev.dbos.transact.client;

import dev.dbos.transact.context.DBOSContext;
import dev.dbos.transact.workflow.Workflow;

public class ClientServiceImpl implements ClientService {

    @Workflow
    public String enqueueTest(int i, String s) {
        return String.format("%d-%s", i, s);
    }

    @Workflow
    public String sendTest(int i) {
        var dbos = DBOSContext.dbosInstance().get();
        var message = dbos.recv("test-topic", 10);
        return String.format("%d-%s", i, message);
    }
}
