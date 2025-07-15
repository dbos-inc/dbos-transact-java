package dev.dbos.transact.notifications;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.workflow.Workflow;

public class NotServiceImpl {

    private final DBOS dbos;

    public NotServiceImpl(DBOS dbos) {
        this.dbos = dbos;
    }

    @Workflow(name = "sendWorkflow")
    public void sendWorkflow(String target, String topic, String msg) {
        dbos.send(target, msg, topic);
    }

    @Workflow(name = "recvWorkflow")
    public String recvWorkflow(String topic) {

        return (String)dbos.recv(topic, 5) ;
        /* Object[] received = (Object[])dbos.recv(topic, 5);
        System.out.println(received[0]);
        return (String)received[0] ; */
    }
}
