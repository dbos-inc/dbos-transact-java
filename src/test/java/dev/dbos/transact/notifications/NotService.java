package dev.dbos.transact.notifications;

public interface NotService {

  void sendWorkflow(String target, String topic, String msg);

  String recvWorkflow(String topic, float timeoutSeconds);

  String recvMultiple(String topic);

  String concWorkflow(String topic);
}
