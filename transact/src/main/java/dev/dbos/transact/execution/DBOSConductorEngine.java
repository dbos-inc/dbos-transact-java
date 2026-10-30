package dev.dbos.transact.execution;

import dev.dbos.transact.AlertHandler;
import dev.dbos.transact.conductor.ConductorEngine;
import dev.dbos.transact.conductor.ConductorSystemDatabase;
import dev.dbos.transact.conductor.WorkflowFork;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.workflow.ForkOptions;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.Queue;
import dev.dbos.transact.workflow.RewindOptions;
import dev.dbos.transact.workflow.ScheduleStatus;
import dev.dbos.transact.workflow.StepInfo;
import dev.dbos.transact.workflow.VersionInfo;
import dev.dbos.transact.workflow.WorkflowSchedule;
import dev.dbos.transact.workflow.WorkflowStatus;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

/**
 * Serves the Conductor client from a launched DBOS instance. When a method of DBOSExecutor or
 * SystemDatabase changes, the delegation here changes with it and {@link ConductorEngine} keeps its
 * signatures.
 */
final class DBOSConductorEngine implements ConductorEngine {
  private final DBOSExecutor executor;
  private final SystemDatabase systemDatabase;

  DBOSConductorEngine(DBOSExecutor executor, SystemDatabase systemDatabase) {
    this.executor = executor;
    this.systemDatabase = systemDatabase;
  }

  @Override
  public String appName() {
    return executor.appName();
  }

  @Override
  public String appVersion() {
    return executor.appVersion();
  }

  @Override
  public String executorId() {
    return executor.executorId();
  }

  @Override
  public @Nullable Map<String, Object> executorMetadata() {
    return executor.executorMetadata();
  }

  @Override
  public void recoverPendingWorkflows(List<String> executorIds) {
    executor.recoverPendingWorkflows(executorIds);
  }

  @Override
  public void cancelWorkflows(List<String> workflowIds, boolean cancelChildren) {
    executor.cancelWorkflows(workflowIds, cancelChildren);
  }

  @Override
  public void resumeWorkflows(List<String> workflowIds, @Nullable String queueName) {
    executor.resumeWorkflows(workflowIds, queueName);
  }

  @Override
  public void deleteWorkflows(List<String> workflowIds, boolean deleteChildren) {
    systemDatabase.deleteWorkflows(workflowIds, deleteChildren, null);
  }

  @Override
  public String forkWorkflow(String workflowId, int startStep, ForkOptions options) {
    return executor.forkWorkflow(workflowId, startStep, options).workflowId();
  }

  @Override
  public void forkWorkflows(
      List<WorkflowFork> forks,
      @Nullable String applicationVersion,
      @Nullable String queueName,
      @Nullable String queuePartitionKey) {
    executor.forkWorkflows(forks, applicationVersion, queueName, queuePartitionKey);
  }

  @Override
  public String rewindWorkflow(String workflowId, int startStep, RewindOptions options) {
    return executor.rewindWorkflow(workflowId, startStep, options).workflowId();
  }

  @Override
  public List<WorkflowStatus> listWorkflows(ListWorkflowsInput input) {
    return systemDatabase.listWorkflows(input);
  }

  @Override
  public List<StepInfo> listWorkflowSteps(
      String workflowId,
      @Nullable Boolean loadOutput,
      @Nullable Integer limit,
      @Nullable Integer offset) {
    return executor.listWorkflowSteps(workflowId, loadOutput, limit, offset);
  }

  @Override
  public Map<String, Object> getAllEvents(String workflowId) {
    return systemDatabase.getAllEvents(workflowId);
  }

  @Override
  public Optional<Queue> findQueue(String name) {
    return systemDatabase.findQueue(name);
  }

  @Override
  public List<Queue> listQueues(@Nullable List<String> applicationNames) {
    return systemDatabase.listQueues(applicationNames);
  }

  @Override
  public Optional<WorkflowSchedule> getSchedule(String name) {
    return systemDatabase.getSchedule(name);
  }

  @Override
  public List<WorkflowSchedule> listSchedules(
      @Nullable List<ScheduleStatus> statuses,
      @Nullable List<String> workflowNames,
      @Nullable List<String> scheduleNamePrefixes,
      @Nullable List<String> applicationNames) {
    return systemDatabase.listSchedules(
        statuses, workflowNames, scheduleNamePrefixes, applicationNames);
  }

  @Override
  public void pauseSchedule(String name) {
    systemDatabase.pauseSchedule(name);
  }

  @Override
  public void resumeSchedule(String name) {
    systemDatabase.resumeSchedule(name);
  }

  @Override
  public List<String> backfillSchedule(String scheduleName, Instant start, Instant end) {
    return DBOSExecutor.backfillSchedule(scheduleName, start, end, systemDatabase);
  }

  @Override
  public String triggerSchedule(String scheduleName) {
    return DBOSExecutor.triggerSchedule(scheduleName, systemDatabase);
  }

  @Override
  public List<VersionInfo> listApplicationVersions() {
    return systemDatabase.listApplicationVersions();
  }

  @Override
  public void setLatestApplicationVersion(String versionName) {
    executor.setLatestApplicationVersion(versionName);
  }

  @Override
  public @Nullable AlertHandler alertHandler() {
    return executor.alertHandler();
  }

  @Override
  public ConductorSystemDatabase systemDatabase() {
    return systemDatabase.forConductor();
  }
}
