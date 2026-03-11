package dev.dbos.transact.client;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.WorkflowClassName;

import java.time.Duration;

@WorkflowClassName("ClientServiceImpl")
public class ClientServiceImpl implements ClientService {

  private final DBOS.Instance dbos;

  public ClientServiceImpl(DBOS.Instance dbos) {
    this.dbos = dbos;
  }

  @Workflow
  public String enqueueTest(int i, String s) {
    return String.format("%d-%s", i, s);
  }

  @Workflow
  public String sendTest(int i) {
    var message = dbos.recv("test-topic", Duration.ofSeconds(10));
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
