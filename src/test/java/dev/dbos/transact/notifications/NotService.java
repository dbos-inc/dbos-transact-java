package dev.dbos.transact.notifications;

public interface NotService {

    void sendWorkflow(String target, String topic, String msg) ;

    String recvWorkflow(String topic) ;

    String recvMultiple(String topic) ;
}
