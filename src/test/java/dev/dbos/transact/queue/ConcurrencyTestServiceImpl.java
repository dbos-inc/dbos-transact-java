package dev.dbos.transact.queue;

import dev.dbos.transact.utils.ManualResetEvent;
import dev.dbos.transact.workflow.Workflow;

import java.util.List;

public class ConcurrencyTestServiceImpl implements ConcurrencyTestService {

    public ManualResetEvent event = new ManualResetEvent(false);
    public List<ManualResetEvent> wfEvents = List.of(new ManualResetEvent(false), new ManualResetEvent(false));
    public int counter = 0;

    @Workflow(name = "noopWorkflow")
    public int noopWorkflow(int i) {
        return i;
    }

    @Workflow(name = "blockedWorkflow")
    public int blockedWorkflow(int i) throws InterruptedException {
        wfEvents.get(i).set();
        counter++;
        event.waitOne();
        return i;
    }

}
