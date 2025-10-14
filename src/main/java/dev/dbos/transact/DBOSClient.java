package dev.dbos.transact;

import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.execution.DBOSExecutor.ExecuteWorkflowOptions;
import dev.dbos.transact.workflow.ForkOptions;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.StepInfo;
import dev.dbos.transact.workflow.WorkflowHandle;
import dev.dbos.transact.workflow.WorkflowState;
import dev.dbos.transact.workflow.WorkflowStatus;
import dev.dbos.transact.workflow.internal.WorkflowHandleDBPoll;
import dev.dbos.transact.workflow.internal.WorkflowStatusInternal;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;

public class DBOSClient implements AutoCloseable {

  private final SystemDatabase systemDatabase;

  public DBOSClient(String url, String user, String password) {
    var dataSource = SystemDatabase.createDataSource(url, user, password, 0, 0);
    systemDatabase = new SystemDatabase(dataSource);
  }

  @Override
  public void close() {
    systemDatabase.close();
  }

  public record EnqueueOptions(
      String workflowName,
      String queueName,
      String className,
      String instanceName,
      String workflowId,
      String appVersion,
      Duration timeout,
      String deduplicationId,
      OptionalInt priority) {

    public EnqueueOptions {
      if (Objects.requireNonNull(workflowName, "EnqueueOptions workflowName must not be null")
          .isEmpty()) {
        throw new IllegalArgumentException("workflowName must not be empty");
      }

      if (Objects.requireNonNull(queueName, "EnqueueOptions queueName must not be null")
          .isEmpty()) {
        throw new IllegalArgumentException("queueName must not be empty");
      }

      if (Objects.requireNonNull(className, "EnqueueOptions className must not be null")
          .isEmpty()) {
        throw new IllegalArgumentException("className must not be empty");
      }

      if (instanceName == null) instanceName = "";

      if (timeout != null && timeout.isNegative()) {
        throw new IllegalArgumentException("timeout must not be negative");
      }
    }

    public EnqueueOptions(String className, String workflowName, String queueName) {
      this(workflowName, queueName, className, "", null, null, null, null, OptionalInt.empty());
    }

    public EnqueueOptions withClassName(String className) {
      return new EnqueueOptions(
          this.workflowName,
          this.queueName,
          className,
          this.instanceName,
          this.workflowId,
          this.appVersion,
          this.timeout,
          this.deduplicationId,
          this.priority);
    }

    public EnqueueOptions withWorkflowId(String workflowId) {
      return new EnqueueOptions(
          this.workflowName,
          this.queueName,
          this.className,
          this.instanceName,
          workflowId,
          this.appVersion,
          this.timeout,
          this.deduplicationId,
          this.priority);
    }

    public EnqueueOptions withAppVersion(String appVersion) {
      return new EnqueueOptions(
          this.workflowName,
          this.queueName,
          this.className,
          this.instanceName,
          this.workflowId,
          appVersion,
          this.timeout,
          this.deduplicationId,
          this.priority);
    }

    public EnqueueOptions withTimeout(Duration timeout) {
      return new EnqueueOptions(
          this.workflowName,
          this.queueName,
          this.className,
          this.instanceName,
          this.workflowId,
          this.appVersion,
          timeout,
          this.deduplicationId,
          this.priority);
    }

    public EnqueueOptions withDeduplicationId(String deduplicationId) {
      return new EnqueueOptions(
          this.workflowName,
          this.queueName,
          this.className,
          this.instanceName,
          this.workflowId,
          this.appVersion,
          this.timeout,
          deduplicationId,
          this.priority);
    }

    public EnqueueOptions withInstanceName(String instName) {
      return new EnqueueOptions(
          this.workflowName,
          this.queueName,
          this.className,
          instName,
          this.workflowId,
          this.appVersion,
          this.timeout,
          this.deduplicationId,
          this.priority);
    }

    public EnqueueOptions withPriority(int priority) {
      return new EnqueueOptions(
          this.workflowName,
          this.queueName,
          this.className,
          this.instanceName,
          this.workflowId,
          this.appVersion,
          this.timeout,
          this.deduplicationId,
          OptionalInt.of(priority));
    }

    @Override
    public String workflowId() {
      return workflowId != null && workflowId.isEmpty() ? null : workflowId;
    }
  }

  public <T, E extends Exception> WorkflowHandle<T, E> enqueueWorkflow(
      EnqueueOptions options, Object[] args) throws Exception {

    return DBOSExecutor.enqueueWorkflow(
        Objects.requireNonNull(
            options.workflowName(), "EnqueueOptions workflowName must not be null"),
        Objects.requireNonNull(options.className(), "EnqueueOptions className must not be null"),
        Objects.requireNonNullElse(options.instanceName(), ""),
        args,
        new ExecuteWorkflowOptions(
            Objects.requireNonNullElseGet(options.workflowId(), () -> UUID.randomUUID().toString()),
            options.timeout(),
            null,
            Objects.requireNonNull(
                options.queueName(), "EnqueueOptions queueName must not be null"),
            options.deduplicationId,
            options.priority),
        null,
        null,
        options.appVersion,
        systemDatabase,
        null);
  }

  public void send(String destinationId, Object message, String topic, String idempotencyKey) {
    var workflowId = "%s-%s".formatted(destinationId, idempotencyKey);
    var now = System.currentTimeMillis();
    if (idempotencyKey == null) {
      idempotencyKey = UUID.randomUUID().toString();
    }

    var status =
        new WorkflowStatusInternal(
            workflowId,
            WorkflowState.SUCCESS,
            "temp_workflow-send-client",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            now,
            now,
            null,
            null,
            null,
            null,
            0,
            null,
            null,
            null,
            0,
            null);
    systemDatabase.initWorkflowStatus(status, null);
    systemDatabase.send(status.getWorkflowUUID(), 0, destinationId, message, topic);
  }

  public Object getEvent(String targetId, String key, Duration timeout) {
    return systemDatabase.getEvent(targetId, key, timeout, null);
  }

  public <T, E extends Exception> WorkflowHandle<T, E> retrieveWorkflow(String workflowId) {
    return new WorkflowHandleDBPoll<T, E>(workflowId, systemDatabase);
  }

  public void cancelWorkflow(String workflowId) {
    systemDatabase.cancelWorkflow(workflowId);
  }

  public <T, E extends Exception> WorkflowHandle<T, E> resumeWorkflow(String workflowId) {
    systemDatabase.resumeWorkflow(workflowId);
    return retrieveWorkflow(workflowId);
  }

  public <T, E extends Exception> WorkflowHandle<T, E> forkWorkflow(
      String originalWorkflowId, int startStep, ForkOptions options) {
    var forkedWorkflowId = systemDatabase.forkWorkflow(originalWorkflowId, startStep, options);
    return retrieveWorkflow(forkedWorkflowId);
  }

  public Optional<WorkflowStatus> getWorkflowStatus(String workflowId) {
    return systemDatabase.getWorkflowStatus(workflowId);
  }

  public List<WorkflowStatus> listWorkflows(ListWorkflowsInput input) {
    return systemDatabase.listWorkflows(input);
  }

  public List<StepInfo> listWorkflowSteps(String workflowId) {
    return systemDatabase.listWorkflowSteps(workflowId);
  }

  // readStream

}
