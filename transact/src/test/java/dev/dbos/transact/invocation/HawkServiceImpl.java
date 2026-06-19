package dev.dbos.transact.invocation;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.workflow.Step;
import dev.dbos.transact.workflow.Timeout;
import dev.dbos.transact.workflow.Workflow;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class HawkServiceImpl implements HawkService {
  private final DBOS dbos;
  private HawkService proxy;

  public HawkServiceImpl(DBOS dbos) {
    this.dbos = dbos;
  }

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
      Thread.currentThread().interrupt();
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
    var handle = dbos.startWorkflow(() -> proxy.simpleWorkflow());
    return handle.getResult();
  }

  @Workflow
  @Override
  public String parentSleepWorkflow(Long timeoutSec, long sleepSec) {
    var duration =
        timeoutSec == null
            ? Timeout.inherit()
            : timeoutSec == 0L ? Timeout.none() : Timeout.of(Duration.ofSeconds(timeoutSec));
    var options = new WorkflowOptions().withTimeout(duration);
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

  @Workflow
  @Override
  public String authContextWorkflow() {
    String user = DBOS.authenticatedUser();
    String role = DBOS.assumedRole();
    var roles = DBOS.authenticatedRoles();
    String rolesStr = roles == null ? "null" : String.join(",", roles);
    return user + "|" + role + "|" + rolesStr;
  }

  @Workflow
  @Override
  public String parentWorkflowWithAuthOverride() {
    try (var _i =
        new WorkflowOptions().withAuthentication("override-user", "override-role").setContext()) {
      return proxy.authContextWorkflow();
    }
  }

  @Workflow
  @Override
  public String parentWorkflowWithAuthCleared() {
    try (var _i = new WorkflowOptions().withNoAuthentication().setContext()) {
      return proxy.authContextWorkflow();
    }
  }

  @Workflow
  @Override
  public String startChildInheritAuth(String childId) {
    var handle =
        dbos.startWorkflow(() -> proxy.authContextWorkflow(), new StartWorkflowOptions(childId));
    return handle.getResult();
  }

  @Workflow
  @Override
  public String startChildOverrideAuth(String childId) {
    var handle =
        dbos.startWorkflow(
            () -> proxy.authContextWorkflow(),
            new StartWorkflowOptions(childId).withAuthentication("override-user", "override-role"));
    return handle.getResult();
  }

  @Workflow
  @Override
  public String startChildClearAllAuth(String childId) {
    var handle =
        dbos.startWorkflow(
            () -> proxy.authContextWorkflow(),
            new StartWorkflowOptions(childId).withNoAuthentication());
    return handle.getResult();
  }

  @Workflow
  @Override
  public String startChildClearUserAndRoles(String childId) {
    var handle =
        dbos.startWorkflow(
            () -> proxy.authContextWorkflow(),
            new StartWorkflowOptions(childId).withNoAuthenticatedUser().withNoAuthenticatedRoles());
    return handle.getResult();
  }

  @Workflow
  @Override
  public String startChildClearAssumedRoleOnly(String childId) {
    var handle =
        dbos.startWorkflow(
            () -> proxy.authContextWorkflow(),
            new StartWorkflowOptions(childId).withNoAssumedRole());
    return handle.getResult();
  }
}
