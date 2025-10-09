package dev.dbos.transact.notifications;

import java.time.Duration;

public interface NotService {

  void sendWorkflow(String target, String topic, String msg);

  String recvWorkflow(String topic, Duration timeout);

  String recvMultiple(String topic);

  String concWorkflow(String topic);
}
