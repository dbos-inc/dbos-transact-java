package dev.dbos.transact.invocation;

import dev.dbos.transact.context.DBOSContext;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.workflow.Step;
import dev.dbos.transact.workflow.Workflow;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class HawkServiceImpl implements HawkService {
  private HawkService proxy;

  public void setProxy(HawkService proxy) {
    this.proxy = proxy;
  }

  @Workflow
  @Override
  public String simpleWorkflow() {
    return LocalDate.now().format(DateTimeFormatter.ISO_DATE);
  }

  @Workflow
  @Override
  public String sleepWorkflow(long sleepSec) {
    var duration = Duration.ofSeconds(sleepSec);
    try {
      Thread.sleep(duration.toMillis());
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    return LocalDate.now().format(DateTimeFormatter.ISO_DATE);
  }

  @Workflow
  @Override
  public String parentWorkflow() {
    return proxy.simpleWorkflow();
  }

  @Workflow
  @Override
  public String parentStartWorkflow() {
    var dbos = DBOSContext.dbosInstance().get();
    var handle = dbos.startWorkflow(() -> proxy.simpleWorkflow());
    return handle.getResult();
  }

  @Workflow
  @Override
  public String parentSleepWorkflow(Long timeoutSec, long sleepSec) {
    var duration = timeoutSec == null ? null : Duration.ofSeconds(timeoutSec);
    var options = new WorkflowOptions(null, duration);
    try (var o = options.setContext()) {
      return proxy.sleepWorkflow(sleepSec);
    }
  }

  @Step
  @Override
  public Instant nowStep() {
    return Instant.now();
  }

  @Workflow
  @Override
  public Instant stepWorkflow() {
    return proxy.nowStep();
  }

  @Step
  @Override
  public String illegalStep() {
    return proxy.simpleWorkflow();
  }

  @Workflow
  @Override
  public String illegalWorkflow() {
    return proxy.illegalStep();
  }
}
