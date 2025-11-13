package dev.dbos.transact.client;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.workflow.Workflow;

import java.time.Duration;

public class ClientServiceImpl implements ClientService {

  @Workflow
  public String enqueueTest(int i, String s) {
    return String.format("%d-%s", i, s);
  }

  @Workflow
  public String sendTest(int i) {
    var message = DBOS.recv("test-topic", Duration.ofSeconds(10));
    return String.format("%d-%s", i, message);
  }

  @Workflow
  public void sleep(int ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
