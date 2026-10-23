package dev.dbos.transact.conductor;

import dev.dbos.transact.AlertHandler;
import dev.dbos.transact.database.MetricData;
import dev.dbos.transact.workflow.ExportedWorkflow;
import dev.dbos.transact.workflow.ForkFromFailureOptions;
import dev.dbos.transact.workflow.ForkOptions;
import dev.dbos.transact.workflow.GetStepAggregatesInput;
import dev.dbos.transact.workflow.GetWorkflowAggregatesInput;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.NotificationInfo;
import dev.dbos.transact.workflow.Queue;
import dev.dbos.transact.workflow.RewindOptions;
import dev.dbos.transact.workflow.ScheduleStatus;
import dev.dbos.transact.workflow.StepAggregateRow;
import dev.dbos.transact.workflow.StepInfo;
import dev.dbos.transact.workflow.VersionInfo;
import dev.dbos.transact.workflow.WorkflowAggregateRow;
import dev.dbos.transact.workflow.WorkflowSchedule;
import dev.dbos.transact.workflow.WorkflowStatus;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

/**
 * Everything the Conductor client in {@code dev.dbos:transact-enterprise} may call on a launched
 * DBOS instance. It is not application API.
 *
 * <p>An enterprise build runs against any later DBOS Transact build of the same minor version, so
 * within a minor this interface only gains methods: none is changed or removed, and neither are the
 * types its methods take and return. DBOS Transact implements it over its internal classes, which
 * absorb their own changes behind it.
 *
 * <p>The methods grouped at the end are the exception. They are Conductor-only operations moving
 * into {@code dev.dbos:transact-enterprise}, and each leaves this interface during the 1.3 previews
 * as it moves, before the 1.3.0 release. An enterprise preview that still calls one fails against a
 * core preview without it, so the enterprise build that stops calling it also raises its minimum
 * DBOS Transact version past the build that removed it.
 */
public interface ConductorEngine {
  /** The application name. */
  String appName();

  /** The application version this executor runs. */
  String appVersion();

  /** This executor's ID. */
  String executorId();

  /** The metadata this executor reports to Conductor, or {@code null} if none is configured. */
  @Nullable Map<String, Object> executorMetadata();

  /**
   * Recovers the pending workflows of the given executors.
   *
   * @param executorIds the executors whose workflows to recover
   */
  void recoverPendingWorkflows(List<String> executorIds);

  /**
   * Cancels workflows.
   *
   * @param workflowIds the workflows to cancel
   * @param cancelChildren whether to cancel their child workflows too
   */
  void cancelWorkflows(List<String> workflowIds, boolean cancelChildren);

  /**
   * Resumes workflows.
   *
   * @param workflowIds the workflows to resume
   * @param queueName the queue to resume them on; {@code null} means the internal queue
   */
  void resumeWorkflows(List<String> workflowIds, @Nullable String queueName);

  /**
   * Deletes workflows.
   *
   * @param workflowIds the workflows to delete
   * @param deleteChildren whether to delete their child workflows too
   */
  void deleteWorkflows(List<String> workflowIds, boolean deleteChildren);

  /**
   * Forks a workflow.
   *
   * @param workflowId the workflow to fork
   * @param startStep the step the fork starts from
   * @param options the fork's options
   * @return the forked workflow's ID
   */
  String forkWorkflow(String workflowId, int startStep, ForkOptions options);

  /**
   * Forks workflows in one transaction, each under its own ID and from its own step: all of them
   * are forked, or, if any does not exist, none is.
   *
   * <p>The caller chooses the forks' IDs, so a retried call writes the same forks: one that follows
   * a committed attempt fails on the rows that attempt wrote rather than forking a second time.
   *
   * @param forks the forks to write
   * @param applicationVersion the forks' application version, or {@code null} for the originals'
   * @param queueName the queue the forks are enqueued on, or {@code null} for DBOS's internal queue
   * @param queuePartitionKey the forks' partition key on a partitioned queue, or {@code null}
   * @throws dev.dbos.transact.exceptions.DBOSNonExistentWorkflowException if a workflow does not
   *     exist
   * @throws IllegalArgumentException if the queue is not registered, or the partition key does not
   *     match whether the queue is partitioned
   */
  void forkWorkflows(
      List<WorkflowFork> forks,
      @Nullable String applicationVersion,
      @Nullable String queueName,
      @Nullable String queuePartitionKey);

  /**
   * Rewinds a workflow.
   *
   * @param workflowId the workflow to rewind
   * @param startStep the step to rewind to
   * @param options the rewind's options
   * @return the rewound workflow's ID
   */
  String rewindWorkflow(String workflowId, int startStep, RewindOptions options);

  /**
   * Lists workflows.
   *
   * @param input the filters
   * @return the matching workflows
   */
  List<WorkflowStatus> listWorkflows(ListWorkflowsInput input);

  /**
   * Lists a workflow's steps.
   *
   * @param workflowId the workflow
   * @param loadOutput whether to load each step's output; {@code null} means {@code true}
   * @param limit the most steps to return; {@code null} means all
   * @param offset the steps to skip; {@code null} means none
   * @return the steps
   */
  List<StepInfo> listWorkflowSteps(
      String workflowId,
      @Nullable Boolean loadOutput,
      @Nullable Integer limit,
      @Nullable Integer offset);

  /**
   * Returns a workflow's events.
   *
   * @param workflowId the workflow
   * @return its events, by key
   */
  Map<String, Object> getAllEvents(String workflowId);

  /**
   * Finds a queue.
   *
   * @param name the queue's name
   * @return the queue, if there is one by that name
   */
  Optional<Queue> findQueue(String name);

  /**
   * Lists queues.
   *
   * @param applicationNames the applications whose queues to list; {@code null} means all
   * @return the queues
   */
  List<Queue> listQueues(@Nullable List<String> applicationNames);

  /**
   * Returns a schedule.
   *
   * @param name the schedule's name
   * @return the schedule, if there is one by that name
   */
  Optional<WorkflowSchedule> getSchedule(String name);

  /**
   * Lists schedules. A {@code null} filter matches everything.
   *
   * @param statuses the statuses to match
   * @param workflowNames the workflow names to match
   * @param scheduleNamePrefixes the schedule-name prefixes to match
   * @param applicationNames the applications to match
   * @return the schedules
   */
  List<WorkflowSchedule> listSchedules(
      @Nullable List<ScheduleStatus> statuses,
      @Nullable List<String> workflowNames,
      @Nullable List<String> scheduleNamePrefixes,
      @Nullable List<String> applicationNames);

  /**
   * Pauses a schedule.
   *
   * @param name the schedule's name
   */
  void pauseSchedule(String name);

  /**
   * Resumes a schedule.
   *
   * @param name the schedule's name
   */
  void resumeSchedule(String name);

  /**
   * Enqueues a schedule's workflow for every time it would have fired after {@code start} and up to
   * and including {@code end}. Times already run are not run again, as the IDs match the live
   * scheduler's.
   *
   * @param scheduleName the schedule's name
   * @param start the start of the window, exclusive
   * @param end the end of the window, inclusive
   * @return the workflow IDs of every firing in the window, including any that already ran
   */
  List<String> backfillSchedule(String scheduleName, Instant start, Instant end);

  /**
   * Enqueues a schedule's workflow for the current time.
   *
   * @param scheduleName the schedule's name
   * @return the ID of the workflow enqueued
   */
  String triggerSchedule(String scheduleName);

  /**
   * Lists the application's versions.
   *
   * @return the versions
   */
  List<VersionInfo> listApplicationVersions();

  /**
   * Makes a version the latest.
   *
   * @param versionName the version
   */
  void setLatestApplicationVersion(String versionName);

  /** The alert handler the application registered, or {@code null} if it registered none. */
  @Nullable AlertHandler alertHandler();

  /** The system database, for the Conductor-only operations that run their own SQL. */
  ConductorSystemDatabase systemDatabase();

  // Conductor-only operations moving into dev.dbos:transact-enterprise before the 1.3.0 release.
  // Unlike the methods above, each is removed from this interface as it moves; see the class
  // documentation.

  /**
   * Exports a workflow.
   *
   * @param workflowId the workflow
   * @param exportChildren whether to export its child workflows too
   * @return the exported workflows
   */
  List<ExportedWorkflow> exportWorkflow(String workflowId, boolean exportChildren);

  /**
   * Imports workflows.
   *
   * @param workflows the workflows to import
   */
  void importWorkflow(List<ExportedWorkflow> workflows);

  /**
   * Forks workflows from their first failed step.
   *
   * @param workflowIds the workflows to fork
   * @param options the forks' options
   * @return the forked workflows' IDs
   */
  List<String> forkFromFailure(List<String> workflowIds, ForkFromFailureOptions options);

  /**
   * Runs one retention round over the whole system database, deleting finished workflows and then
   * the payloads they leave behind. Does nothing if both limits are {@code null}, or if another
   * round holds the retention lock.
   *
   * @param cutoff delete workflows that finished before this time; {@code null} means no age limit
   * @param rowsThreshold the workflow count retention keeps to; {@code null} means no count limit
   * @param batchSize the most rows to delete in one statement; must be positive
   * @throws IllegalArgumentException if {@code batchSize} is not positive
   */
  void garbageCollect(@Nullable Instant cutoff, @Nullable Long rowsThreshold, int batchSize);

  /**
   * Cancels unfinished workflows created before a time.
   *
   * @param endTime cancel workflows created before this time
   */
  void globalTimeout(Instant endTime);

  /**
   * Returns metrics for the Conductor dashboard.
   *
   * @param startTime the start of the period
   * @param endTime the end of the period
   * @param applicationNames the applications to include; {@code null} means all
   * @return the metrics
   */
  List<MetricData> getMetrics(
      Instant startTime, Instant endTime, @Nullable List<String> applicationNames);

  /**
   * Returns workflow aggregates for the Conductor dashboard.
   *
   * @param input the filters and grouping
   * @return the aggregate rows
   */
  List<WorkflowAggregateRow> getWorkflowAggregates(GetWorkflowAggregatesInput input);

  /**
   * Returns step aggregates for the Conductor dashboard.
   *
   * @param input the filters and grouping
   * @return the aggregate rows
   */
  List<StepAggregateRow> getStepAggregates(GetStepAggregatesInput input);

  /**
   * Returns the messages sent to a workflow.
   *
   * @param workflowId the workflow
   * @return the messages
   */
  List<NotificationInfo> getAllNotifications(String workflowId);

  /**
   * Returns a workflow's streams.
   *
   * @param workflowId the workflow
   * @return each stream's values, by key
   */
  Map<String, List<Object>> getAllStreamEntries(String workflowId);
}
