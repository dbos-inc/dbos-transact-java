package dev.dbos.transact;

import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.execution.DBOSExecutor.ExecuteWorkflowOptions;
import dev.dbos.transact.queue.ListQueuedWorkflowsInput;
import dev.dbos.transact.workflow.ForkOptions;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.StepInfo;
import dev.dbos.transact.workflow.WorkflowHandle;
import dev.dbos.transact.workflow.WorkflowState;
import dev.dbos.transact.workflow.WorkflowStatus;
import dev.dbos.transact.workflow.internal.WorkflowHandleDBPoll;
import dev.dbos.transact.workflow.internal.WorkflowStatusInternal;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.UUID;

public class DBOSClient implements AutoCloseable {

  private final SystemDatabase systemDatabase;

  public DBOSClient(String url, String user, String password) {
    var dataSource = SystemDatabase.createDataSource(url, user, password, 0, 0);
    systemDatabase = new SystemDatabase(dataSource);
  }

  @Override
  public void close() throws Exception {
    systemDatabase.close();
  }

  // TODO: add priority + deduplicationId options
  // (https://github.com/dbos-inc/dbos-transact-java/issues/67)
  public record EnqueueOptions(
      String workflowName,
      String queueName,
      String className,
      String workflowId,
      String appVersion,
      Duration timeout,
      String deduplicationId,
      OptionalInt priority) {

    public EnqueueOptions {
      Objects.requireNonNull(workflowId);
      if (workflowId.isEmpty()) {
        throw new IllegalArgumentException("workflowId must not be empty");
      }

      Objects.requireNonNull(queueName);
      if (queueName.isEmpty()) {
        throw new IllegalArgumentException("workflowId must not be empty");
      }

      if (timeout != null && timeout.isNegative()) {
        throw new IllegalArgumentException("timeout must not be negative");
      }
    }

    public EnqueueOptions(String workflowName, String queueName) {
      this(workflowName, queueName, null, null, null, null, null, OptionalInt.empty());
    }

    public EnqueueOptions withClassName(String className) {
      return new EnqueueOptions(
          this.workflowName,
          this.queueName,
          className,
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
          this.workflowId,
          this.appVersion,
          this.timeout,
          deduplicationId,
          this.priority);
    }

    public EnqueueOptions withPriority(int priority) {
      return new EnqueueOptions(
          this.workflowName,
          this.queueName,
          this.className,
          this.workflowId,
          this.appVersion,
          this.timeout,
          this.deduplicationId,
          OptionalInt.of(priority));
    }
  }

  public <T> WorkflowHandle<T, ?> enqueueWorkflow(EnqueueOptions options, Object[] args)
      throws Exception {

    return DBOSExecutor.enqueueWorkflow(
        Objects.requireNonNull(options.workflowName),
        options.className,
        args,
        new ExecuteWorkflowOptions(
            Objects.requireNonNullElseGet(options.workflowId(), () -> UUID.randomUUID().toString()),
            options.timeout(),
            null,
            Objects.requireNonNull(options.queueName),
            options.deduplicationId,
            options.priority),
        null,
        null,
        options.appVersion,
        systemDatabase,
        null);
  }

  public void send(String destinationId, Object message, String topic, String idempotencyKey)
      throws SQLException {
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

  public Object getEvent(String targetId, String key, double timeoutSeconds) {
    return systemDatabase.getEvent(targetId, key, timeoutSeconds, null);
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

  public WorkflowStatus getWorkflowStatus(String workflowId) {
    return systemDatabase.getWorkflowStatus(workflowId);
  }

  public List<WorkflowStatus> listWorkflows(ListWorkflowsInput input) {
    try {
      return systemDatabase.listWorkflows(input);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public List<WorkflowStatus> listQueuedWorkflows(
      ListQueuedWorkflowsInput input, boolean loadInput) {
    try {
      return systemDatabase.listQueuedWorkflows(input, loadInput);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public List<StepInfo> listWorkflowSteps(String workflowId) {
    try {
      return systemDatabase.listWorkflowSteps(workflowId);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  // readStream

}
