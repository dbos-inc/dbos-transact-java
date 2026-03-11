package dev.dbos.transact.invocation;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.Step;
import dev.dbos.transact.workflow.Workflow;

import java.time.Instant;

public class BearServiceImpl implements BearService {
  private final DBOS.Instance dbos;
  public int nWfCalls = 0;
  private BearService proxy;

  public BearServiceImpl(DBOS.Instance dbos) {
    this.dbos = dbos;
  }

  public void setProxy(BearService proxy) {
    this.proxy = proxy;
  }

  @Override
  public String getName() {
    return "Bear";
  }

  @Step
  @Override
  public Instant nowStep() {
    return Instant.now();
  }

  @Workflow
  @Override
  public Instant stepWorkflow() {
    ++nWfCalls;
    return proxy.nowStep();
  }

  @Workflow
  @Override
  public String listSteps(String wfid) {
    var ll1 = dbos.listWorkflows(new ListWorkflowsInput().withWorkflowId(wfid)).size();
    var ll2 = dbos.listWorkflowSteps(wfid).size();

    return String.format("%d %d", ll1, ll2);
  }
}
