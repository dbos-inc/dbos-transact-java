package dev.dbos.transact.client;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.WorkflowClassName;

import java.time.Duration;

interface ClientService {
  String enqueueTest(int i, String s);

  String sendTest(int i);

  void sleep(int ms);
}

@WorkflowClassName("ClientServiceImpl")
class ClientServiceImpl implements ClientService {

  private final DBOS dbos;

  public ClientServiceImpl(DBOS dbos) {
    this.dbos = dbos;
  }

  @Workflow
  public String enqueueTest(int i, String s) {
    return String.format("%d-%s", i, s);
  }

  @Workflow
  public String sendTest(int i) {
    var message = dbos.<String>recv("test-topic", Duration.ofSeconds(10)).orElseThrow();
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
