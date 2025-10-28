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
import dev.dbos.transact.workflow.internal.WorkflowStatusInternal;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * DBOSClient allows external programs to interact with DBOS apps via direct system database access.
 * Example interactions: Start/enqueue a workflow, and get the result Get events and send messages
 * to the workflow Manage workflows - list, fork, cancel, etc.
 */
public class DBOSClient implements AutoCloseable {
  private class WorkflowHandleClient<T, E extends Exception> implements WorkflowHandle<T, E> {
    private String workflowId;

    public WorkflowHandleClient(String workflowId) {
      this.workflowId = workflowId;
    }

    @Override
    public String workflowId() {
      return workflowId;
    }

    @Override
    public T getResult() throws E {
      return systemDatabase.awaitWorkflowResult(workflowId);
    }

    @Override
    public WorkflowStatus getStatus() {
      return systemDatabase.getWorkflowStatus(workflowId);
    }
  }

  private final SystemDatabase systemDatabase;

  /**
   * Construct a DBOSClient, by providing system database access credentials
   *
   * @param url System database JDBC URL
   * @param user System database user
   * @param password System database credential / password
   */
  public DBOSClient(String url, String user, String password) {
    var dataSource = SystemDatabase.createDataSource(url, user, password, 0, 0);
    systemDatabase = new SystemDatabase(dataSource);
  }

  @Override
  public void close() {
    systemDatabase.close();
  }

  /**
   * Options for enqueuing a workflow. It is necessary to specify the class and name of the workflow
   * to enqueue, as well as the queue to use. Other options, such as the workflow ID, queue options,
   * and app version, are optional, and should be set with `with` functions.
   */
  public record EnqueueOptions(
      String workflowName,
      String queueName,
      String className,
      String instanceName,
      String workflowId,
      String appVersion,
      Duration timeout,
      String deduplicationId,
      Integer priority) {

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

    /** Construct `EnqueueOptions` with a minimum set of required options */
    public EnqueueOptions(String className, String workflowName, String queueName) {
      this(workflowName, queueName, className, "", null, null, null, null, null);
    }

    /**
     * Specify the Java classname for the class containing the workflow to enqueue
     *
     * @param className Class containing the workflow to enqueue
     * @return New `EnqueueOptions` with the class name set
     */
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

    /**
     * Specify the workflow ID for the workflow to be enqueued. This is an idempotency key for
     * running the workflow.
     *
     * @param workflowId Workflow idempotency ID to use
     * @return New `EnqueueOptions` with the workflow ID set
     */
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

    /**
     * Specify the app version for the workflow to be enqueued. The workflow will be executed by an
     * executor with this app version. If not specified, the current app version will be used.
     *
     * @param appVersion Application version to use for executing the workflow
     * @return New `EnqueueOptions` with the app version set
     */
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

    /**
     * Specify a timeout for the workflow to be enqueued. Timeout begins once the workflow is
     * running; if it exceeds this it will be canceled.
     *
     * @param timeout Duration of time, from start, before the workflow is canceled.
     * @return New `EnqueueOptions` with the timeout set
     */
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

    /**
     * Specify a queue deduplication ID for the workflow to be enqueued. Queue requests with the
     * same deduplication ID will be rejected.
     *
     * @param deduplicationId Queue deduplication ID
     * @return New `EnqueueOptions` with the deduplication ID set
     */
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

    /**
     * Specify an object instance name to execute the workflow. If workflow objects are named, this
     * must be specified to direct processing to the correct instance.
     *
     * @param instName Instance name registered within `DBOS.registerWorkflows`
     * @return New `EnqueueOptions` with the target instance name set
     */
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

    /**
     * Specify priority. Priority must be enabled on the queue for this to be effective.
     *
     * @param priority Queue priority; if `null`, priority '0' will be used.
     * @return New `EnqueueOptions` with the priority set
     */
    public EnqueueOptions withPriority(Integer priority) {
      return new EnqueueOptions(
          this.workflowName,
          this.queueName,
          this.className,
          this.instanceName,
          this.workflowId,
          this.appVersion,
          this.timeout,
          this.deduplicationId,
          priority);
    }

    /**
     * Get the workflow ID that will be used
     *
     * @return The workflow idemptence ID
     */
    @Override
    public String workflowId() {
      return workflowId != null && workflowId.isEmpty() ? null : workflowId;
    }
  }

  /**
   * Enqueue a workflow.
   *
   * @param <T> Return type of workflow function
   * @param <E> Exception thrown by workflow function
   * @param options `DBOSClient.EnqueueOptions` for enqueuing the workflow
   * @param args Arguments to pass to the workflow function
   * @return WorkflowHandle for retrieving workflow ID, status, and results
   */
  public <T, E extends Exception> WorkflowHandle<T, E> enqueueWorkflow(
      EnqueueOptions options, Object[] args) {

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

  /**
   * Send a message to a workflow
   *
   * @param destinationId workflowId of the workflow to receive the message
   * @param message Message contents
   * @param topic Topic for the message
   * @param idempotencyKey If specified, use the value to ensure exactly-once send semantics
   */
  public void send(String destinationId, Object message, String topic, String idempotencyKey) {
    if (idempotencyKey == null) {
      idempotencyKey = UUID.randomUUID().toString();
    }
    var workflowId = "%s-%s".formatted(destinationId, idempotencyKey);

    var status =
        new WorkflowStatusInternal(workflowId, WorkflowState.SUCCESS)
            .withName("temp_workflow-send-client");
    systemDatabase.initWorkflowStatus(status, null);
    systemDatabase.send(status.workflowId(), 0, destinationId, message, topic);
  }

  /**
   * Get event from a workflow, or null if the operation times out
   *
   * @param targetId ID of the workflow setting the event
   * @param key Key for the event
   * @param timeout Maximum time duration to wait before returning `null`
   * @return Workflow event value, or `null` if the timeout is hit.
   */
  public Object getEvent(String targetId, String key, Duration timeout) {
    return systemDatabase.getEvent(targetId, key, timeout, null);
  }

  /**
   * Create a handle for a workflow. This call does not ensure that the workflow exists; use the
   * returned handle's `getStatus()`.
   *
   * @param <T> Type of the workflow's return value
   * @param <E> Type of any checked exception thrown by the workflow
   * @param workflowId ID of the workflow to retrieve
   * @return A `WorkflowHandle` for the specified worflow ID
   */
  public <T, E extends Exception> WorkflowHandle<T, E> retrieveWorkflow(String workflowId) {
    return new WorkflowHandleClient<T, E>(workflowId);
  }

  /**
   * Cancel a worflow
   *
   * @param workflowId ID of the workflow to cancel
   */
  public void cancelWorkflow(String workflowId) {
    systemDatabase.cancelWorkflow(workflowId);
  }

  /**
   * Resume a canceled workflow, providing a handle to the workflow
   *
   * @param <T> Type of the workflow's return value
   * @param <E> Type of any checked exception thrown by the workflow
   * @param workflowId ID of the workflow to resume
   * @return `WorkflowHandle` for the resumed workflow
   */
  public <T, E extends Exception> WorkflowHandle<T, E> resumeWorkflow(String workflowId) {
    systemDatabase.resumeWorkflow(workflowId);
    return retrieveWorkflow(workflowId);
  }

  /**
   * Fork a workflow, providing a handle to the new workflow
   *
   * @param <T> Type of the workflow's return value
   * @param <E> Type of any checked exception thrown by the workflow
   * @param originalWorkflowId ID of the workflow to fork
   * @param startStep Step number for starting the new fork of the workflow; if zero start from the
   *     beginning
   * @param options Options for forking;
   * @return `WorkflowHandle` for the new workflow
   */
  public <T, E extends Exception> WorkflowHandle<T, E> forkWorkflow(
      String originalWorkflowId, int startStep, ForkOptions options) {
    var forkedWorkflowId = systemDatabase.forkWorkflow(originalWorkflowId, startStep, options);
    return retrieveWorkflow(forkedWorkflowId);
  }

  /**
   * Get the status of a workflow
   *
   * @param workflowId ID of the workflow to query for status
   * @return WorkflowStatus of the workflow, or empty if the workflow does not exist
   */
  public Optional<WorkflowStatus> getWorkflowStatus(String workflowId) {
    return Optional.ofNullable(systemDatabase.getWorkflowStatus(workflowId));
  }

  /**
   * List workflows matching the supplied input filter criteria
   *
   * @param input Filter criteria to use for listing workflows
   * @return list of workflows matching the `ListWorkflowsInput` criteria
   */
  public List<WorkflowStatus> listWorkflows(ListWorkflowsInput input) {
    return systemDatabase.listWorkflows(input);
  }

  /**
   * List the steps executed by a workflow
   *
   * @param workflowId ID of the workflow to list
   * @return List of steps executed by the workflow
   */
  public List<StepInfo> listWorkflowSteps(String workflowId) {
    return systemDatabase.listWorkflowSteps(workflowId);
  }
}
