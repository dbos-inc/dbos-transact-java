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
    }

    @Workflow(name = "recvMultiple")
    public String recvMultiple(String topic) {
        String msg1 = (String)dbos.recv(topic, 5) ;
        String msg2 = (String)dbos.recv(topic, 5) ;
        String msg3 = (String)dbos.recv(topic, 5) ;
        return msg1+msg2+msg3 ;

    }


}
