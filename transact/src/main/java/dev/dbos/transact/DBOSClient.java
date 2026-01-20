package dev.dbos.transact;

import dev.dbos.transact.database.Result;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.workflow.ForkOptions;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.StepInfo;
import dev.dbos.transact.workflow.Timeout;
import dev.dbos.transact.workflow.WorkflowHandle;
import dev.dbos.transact.workflow.WorkflowState;
import dev.dbos.transact.workflow.WorkflowStatus;
import dev.dbos.transact.workflow.internal.WorkflowStatusInternal;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import javax.sql.DataSource;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * DBOSClient allows external programs to interact with DBOS apps via direct system database access.
 * Example interactions: Start/enqueue a workflow, and get the result Get events and send messages
 * to the workflow Manage workflows - list, fork, cancel, etc.
 */
public class DBOSClient implements AutoCloseable {
  private class WorkflowHandleClient<T, E extends Exception> implements WorkflowHandle<T, E> {
    private @NonNull String workflowId;

    public WorkflowHandleClient(@NonNull String workflowId) {
      this.workflowId = workflowId;
    }

    @Override
    public @NonNull String workflowId() {
      return workflowId;
    }

    @Override
    public T getResult() throws E {
      var result = systemDatabase.<T>awaitWorkflowResult(workflowId);
      return Result.<T, E>process(result);
    }

    @Override
    public @Nullable WorkflowStatus getStatus() {
      return systemDatabase.getWorkflowStatus(workflowId);
    }
  }

  private final @NonNull SystemDatabase systemDatabase;

  /**
   * Construct a DBOSClient, by providing system database access credentials
   *
   * @param url System database JDBC URL
   * @param user System database user
   * @param password System database credential / password
   */
  public DBOSClient(@NonNull String url, @NonNull String user, @NonNull String password) {
    this(url, user, password, null);
  }

  /**
   * Construct a DBOSClient, by providing system database access credentials
   *
   * @param url System database JDBC URL
   * @param user System database user
   * @param password System database credential / password
   * @param schema Database schema for DBOS tables
   */
  public DBOSClient(
      @NonNull String url,
      @NonNull String user,
      @NonNull String password,
      @Nullable String schema) {
    systemDatabase = new SystemDatabase(url, user, password, schema);
  }

  /**
   * Construct a DBOSClient, by providing a configured data source
   *
   * @param dataSource System database data source
   */
  public DBOSClient(@NonNull DataSource dataSource) {
    this(dataSource, null);
  }

  /**
   * Construct a DBOSClient, by providing a configured data source
   *
   * @param dataSource System database data source
   * @param schema Database schema for DBOS tables
   */
  public DBOSClient(@NonNull DataSource dataSource, @Nullable String schema) {
    systemDatabase = new SystemDatabase(dataSource, schema);
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
      @NonNull String workflowName,
      @NonNull String queueName,
      @NonNull String className,
      @NonNull String instanceName,
      @Nullable String workflowId,
      @Nullable String appVersion,
      @Nullable Duration timeout,
      @Nullable Instant deadline,
      @Nullable String deduplicationId,
      @Nullable Integer priority,
      @Nullable String queuePartitionKey) {

    public EnqueueOptions {
      if (Objects.requireNonNull(workflowName, "EnqueueOptions workflowName must not be null")
          .isEmpty()) {
        throw new IllegalArgumentException("EnqueueOptions workflowName must not be empty");
      }

      if (Objects.requireNonNull(queueName, "EnqueueOptions queueName must not be null")
          .isEmpty()) {
        throw new IllegalArgumentException("EnqueueOptions queueName must not be empty");
      }

      if (Objects.requireNonNull(className, "EnqueueOptions className must not be null")
          .isEmpty()) {
        throw new IllegalArgumentException("EnqueueOptions className must not be empty");
      }

      if (queuePartitionKey != null && queuePartitionKey.isEmpty()) {
        throw new IllegalArgumentException(
            "EnqueueOptions queuePartitionKey must not be empty if not null");
      }

      if (deduplicationId != null && deduplicationId.isEmpty()) {
        throw new IllegalArgumentException(
            "EnqueueOptions deduplicationId must not be empty if not null");
      }

      if (instanceName == null) instanceName = "";

      if (timeout != null) {
        if (timeout.isNegative() || timeout.isZero()) {
          throw new IllegalArgumentException(
              "EnqueueOptions timeout must be a positive non-zero duration");
        }

        if (deadline != null) {
          throw new IllegalArgumentException(
              "EnqueueOptions timeout and deadline cannot both be set");
        }
      }
    }

    /** Construct `EnqueueOptions` with a minimum set of required options */
    public EnqueueOptions(
        @NonNull String className, @NonNull String workflowName, @NonNull String queueName) {
      this(workflowName, queueName, className, "", null, null, null, null, null, null, null);
    }

    /**
     * Specify the Java classname for the class containing the workflow to enqueue
     *
     * @param className Class containing the workflow to enqueue
     * @return New `EnqueueOptions` with the class name set
     */
    public @NonNull EnqueueOptions withClassName(@NonNull String className) {
      return new EnqueueOptions(
          this.workflowName,
          this.queueName,
          className,
          this.instanceName,
          this.workflowId,
          this.appVersion,
          this.timeout,
          this.deadline,
          this.deduplicationId,
          this.priority,
          this.queuePartitionKey);
    }

    /**
     * Specify the workflow ID for the workflow to be enqueued. This is an idempotency key for
     * running the workflow.
     *
     * @param workflowId Workflow idempotency ID to use
     * @return New `EnqueueOptions` with the workflow ID set
     */
    public @NonNull EnqueueOptions withWorkflowId(@Nullable String workflowId) {
      return new EnqueueOptions(
          this.workflowName,
          this.queueName,
          this.className,
          this.instanceName,
          workflowId,
          this.appVersion,
          this.timeout,
          this.deadline,
          this.deduplicationId,
          this.priority,
          this.queuePartitionKey);
    }

    /**
     * Specify the app version for the workflow to be enqueued. The workflow will be executed by an
     * executor with this app version. If not specified, the current app version will be used.
     *
     * @param appVersion Application version to use for executing the workflow
     * @return New `EnqueueOptions` with the app version set
     */
    public @NonNull EnqueueOptions withAppVersion(@Nullable String appVersion) {
      return new EnqueueOptions(
          this.workflowName,
          this.queueName,
          this.className,
          this.instanceName,
          this.workflowId,
          appVersion,
          this.timeout,
          this.deadline,
          this.deduplicationId,
          this.priority,
          this.queuePartitionKey);
    }

    /**
     * Specify a timeout for the workflow to be enqueued. Timeout begins once the workflow is
     * running; if it exceeds this it will be canceled.
     *
     * @param timeout Duration of time, from start, before the workflow is canceled.
     * @return New `EnqueueOptions` with the timeout set
     */
    public @NonNull EnqueueOptions withTimeout(@Nullable Duration timeout) {
      return new EnqueueOptions(
          this.workflowName,
          this.queueName,
          this.className,
          this.instanceName,
          this.workflowId,
          this.appVersion,
          timeout,
          this.deadline,
          this.deduplicationId,
          this.priority,
          this.queuePartitionKey);
    }

    /**
     * Specify a deadline for the workflow. This is an absolute time, regardless of when the
     * workflow starts.
     *
     * @param deadline Instant after which the workflow will be canceled.
     * @return New `EnqueueOptions` with the deadline set
     */
    public @NonNull EnqueueOptions withDeadline(@Nullable Instant deadline) {
      return new EnqueueOptions(
          this.workflowName,
          this.queueName,
          this.className,
          this.instanceName,
          this.workflowId,
          this.appVersion,
          this.timeout,
          deadline,
          this.deduplicationId,
          this.priority,
          this.queuePartitionKey);
    }

    /**
     * Specify a queue deduplication ID for the workflow to be enqueued. Queue requests with the
     * same deduplication ID will be rejected.
     *
     * @param deduplicationId Queue deduplication ID
     * @return New `EnqueueOptions` with the deduplication ID set
     */
    public @NonNull EnqueueOptions withDeduplicationId(@Nullable String deduplicationId) {
      return new EnqueueOptions(
          this.workflowName,
          this.queueName,
          this.className,
          this.instanceName,
          this.workflowId,
          this.appVersion,
          this.timeout,
          this.deadline,
          deduplicationId,
          this.priority,
          this.queuePartitionKey);
    }

    /**
     * Specify an object instance name to execute the workflow. If workflow objects are named, this
     * must be specified to direct processing to the correct instance.
     *
     * @param instName Instance name registered within `DBOS.registerWorkflows`
     * @return New `EnqueueOptions` with the target instance name set
     */
    public @NonNull EnqueueOptions withInstanceName(@Nullable String instName) {
      return new EnqueueOptions(
          this.workflowName,
          this.queueName,
          this.className,
          instName,
          this.workflowId,
          this.appVersion,
          this.timeout,
          this.deadline,
          this.deduplicationId,
          this.priority,
          this.queuePartitionKey);
    }

    /**
     * Specify priority. Priority must be enabled on the queue for this to be effective.
     *
     * @param priority Queue priority; if `null`, priority '0' will be used.
     * @return New `EnqueueOptions` with the priority set
     */
    public @NonNull EnqueueOptions withPriority(@Nullable Integer priority) {
      return new EnqueueOptions(
          this.workflowName,
          this.queueName,
          this.className,
          this.instanceName,
          this.workflowId,
          this.appVersion,
          this.timeout,
          this.deadline,
          this.deduplicationId,
          priority,
          this.queuePartitionKey);
    }

    /**
     * Creates a new EnqueueOptions instance with the specified queue partition key. The partition
     * key is used to determine which partition of the queue the workflow should be enqueued to,
     * allowing for better load distribution and ordering guarantees.
     *
     * @param partitionKey the partition key to use for queue partitioning, can be null
     * @return a new EnqueueOptions instance with the specified partition key
     */
    public @NonNull EnqueueOptions withQueuePartitionKey(@Nullable String partitionKey) {
      return new EnqueueOptions(
          this.workflowName,
          this.queueName,
          this.className,
          this.instanceName,
          this.workflowId,
          this.appVersion,
          this.timeout,
          this.deadline,
          this.deduplicationId,
          this.priority,
          partitionKey);
    }

    /**
     * Get the workflow ID that will be used
     *
     * @return The workflow idemptence ID
     */
    @Override
    public @Nullable String workflowId() {
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
  public <T, E extends Exception> @NonNull WorkflowHandle<T, E> enqueueWorkflow(
      @NonNull EnqueueOptions options, @Nullable Object[] args) {

    return DBOSExecutor.enqueueWorkflow(
        Objects.requireNonNull(
            options.workflowName(), "EnqueueOptions workflowName must not be null"),
        Objects.requireNonNull(options.className(), "EnqueueOptions className must not be null"),
        Objects.requireNonNullElse(options.instanceName(), ""),
        null,
        args,
        new DBOSExecutor.ExecutionOptions(
            Objects.requireNonNullElseGet(options.workflowId(), () -> UUID.randomUUID().toString()),
            Timeout.of(options.timeout()),
            options.deadline,
            Objects.requireNonNull(
                options.queueName(), "EnqueueOptions queueName must not be null"),
            options.deduplicationId,
            options.priority,
            options.queuePartitionKey,
            false,
            false),
        null,
        null,
        null,
        options.appVersion,
        systemDatabase);
  }

  /**
   * Send a message to a workflow
   *
   * @param destinationId workflowId of the workflow to receive the message
   * @param message Message contents
   * @param topic Topic for the message
   * @param idempotencyKey If specified, use the value to ensure exactly-once send semantics
   */
  public void send(
      @NonNull String destinationId,
      @NonNull Object message,
      @NonNull String topic,
      @Nullable String idempotencyKey) {
    if (idempotencyKey == null) {
      idempotencyKey = UUID.randomUUID().toString();
    }
    var workflowId = "%s-%s".formatted(destinationId, idempotencyKey);

    var status =
        WorkflowStatusInternal.builder(workflowId, WorkflowState.SUCCESS)
            .name("temp_workflow-send-client")
            .build();
    systemDatabase.initWorkflowStatus(status, null, false, false);
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
  public @Nullable Object getEvent(
      @NonNull String targetId, @NonNull String key, @NonNull Duration timeout) {
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
  public <T, E extends Exception> @NonNull WorkflowHandle<T, E> retrieveWorkflow(
      @NonNull String workflowId) {
    return new WorkflowHandleClient<T, E>(workflowId);
  }

  /**
   * Cancel a worflow
   *
   * @param workflowId ID of the workflow to cancel
   */
  public void cancelWorkflow(@NonNull String workflowId) {
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
  public <T, E extends Exception> @NonNull WorkflowHandle<T, E> resumeWorkflow(
      @NonNull String workflowId) {
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
  public <T, E extends Exception> @NonNull WorkflowHandle<T, E> forkWorkflow(
      @NonNull String originalWorkflowId, int startStep, @NonNull ForkOptions options) {
    var forkedWorkflowId = systemDatabase.forkWorkflow(originalWorkflowId, startStep, options);
    return retrieveWorkflow(forkedWorkflowId);
  }

  /**
   * Get the status of a workflow
   *
   * @param workflowId ID of the workflow to query for status
   * @return WorkflowStatus of the workflow, or empty if the workflow does not exist
   */
  public @NonNull Optional<WorkflowStatus> getWorkflowStatus(@NonNull String workflowId) {
    return Optional.ofNullable(systemDatabase.getWorkflowStatus(workflowId));
  }

  /**
   * List workflows matching the supplied input filter criteria
   *
   * @param input Filter criteria to use for listing workflows
   * @return list of workflows matching the `ListWorkflowsInput` criteria
   */
  public @NonNull List<WorkflowStatus> listWorkflows(@NonNull ListWorkflowsInput input) {
    return systemDatabase.listWorkflows(input);
  }

  /**
   * List the steps executed by a workflow
   *
   * @param workflowId ID of the workflow to list
   * @return List of steps executed by the workflow
   */
  public @NonNull List<StepInfo> listWorkflowSteps(@NonNull String workflowId) {
    return systemDatabase.listWorkflowSteps(workflowId);
  }
}
