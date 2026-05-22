package dev.dbos.transact.execution;

import dev.dbos.transact.Constants;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.workflow.WorkflowSchedule;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchedulerService implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(SchedulerService.class);

  public static final CronParser CRON_PARSER =
      new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.SPRING53));
  private static final Class<?>[] EXPECTED_PARAMETERS =
      new Class<?>[] {Instant.class, Object.class};
  private static final Duration MAX_JITTER = Duration.ofSeconds(10);

  private final DBOSExecutor dbosExecutor;
  private final SystemDatabase systemDatabase;
  private final Duration pollingInterval;
  private final AtomicReference<ScheduledExecutorService> execServiceRef = new AtomicReference<>();
  private final AtomicBoolean paused = new AtomicBoolean(false);
  private final ConcurrentHashMap<String, ScheduledFuture<?>> workflowScheduleFutures =
      new ConcurrentHashMap<>();

  public SchedulerService(
      DBOSExecutor dbosExecutor, SystemDatabase systemDatabase, Duration pollingInterval) {
    this.dbosExecutor = dbosExecutor;
    this.systemDatabase = systemDatabase;
    this.pollingInterval = pollingInterval;
  }

  public void start() {
    if (this.execServiceRef.get() == null) {
      var procCount = Runtime.getRuntime().availableProcessors();
      var scheduler = Executors.newScheduledThreadPool(procCount);
      if (this.execServiceRef.compareAndSet(null, scheduler)) {
        scheduler.scheduleAtFixedRate(
            this::pollWorkflowSchedules, 0, pollingInterval.toMillis(), TimeUnit.MILLISECONDS);
      }
    }
  }

  @Override
  public void close() {
    var scheduler = this.execServiceRef.getAndSet(null);
    if (scheduler != null) {
      var notRun = scheduler.shutdownNow();
      logger.debug("Shutting down scheduler service. {} task(s) not run.", notRun.size());
    }
  }

  public void pause() {
    paused.set(true);
  }

  public void unpause() {
    paused.set(false);
  }

  private void pollWorkflowSchedules() {
    try {
      // if execServiceRef is null, the scheduler service was shut down so don't poll schedules
      if (execServiceRef.get() == null) {
        return;
      }

      var schedules = dbosExecutor.listSchedules(null, null, null);
      if (logger.isDebugEnabled()) {
        logger.debug("pollWorkflowSchedules found {} schedules", schedules.size());
        for (var s : schedules) {
          logger.debug(
              "  schedule: {} workflow: {} cron: {}", s.scheduleName(), s.workflowName(), s.cron());
        }
      }

      // shut down any scheduled future that isn't in the list of current schedules
      var currentIds = schedules.stream().map(WorkflowSchedule::id).collect(Collectors.toSet());
      for (var key : workflowScheduleFutures.keySet()) {
        if (!currentIds.contains(key)) {
          cancelWorkflowSchedule(key);
        }
      }

      for (var schedule : schedules) {
        var scheduleRunning = workflowScheduleFutures.containsKey(schedule.id());
        if (!schedule.isActive()) {
          // if the schedule is no longer active but we still have a scheduled future for it, cancel
          // it
          if (scheduleRunning) {
            cancelWorkflowSchedule(schedule.id());
          }
        } else if (!scheduleRunning) {
          // if the schedule is active but we don't yet have a scheduled future for it, schedule it
          // now
          var optRegWf =
              dbosExecutor.getRegisteredWorkflow(schedule.workflowName(), schedule.className(), "");
          if (optRegWf.isEmpty()) {
            logger.error(
                "Workflow schedule {} has missing workflow function {}",
                schedule.scheduleName(),
                RegisteredWorkflow.fullyQualifiedName(
                    schedule.workflowName(), schedule.className()));
            continue;
          }

          var regWorkflow = optRegWf.orElseThrow();
          if (!Arrays.equals(
              regWorkflow.workflowMethod().getParameterTypes(), EXPECTED_PARAMETERS)) {
            logger.error(
                "Workflow schedule {} workflow {} has invalid signature, signature must be (Instant, Object)",
                schedule.scheduleName(),
                regWorkflow.fullyQualifiedName());
            continue;
          }

          final String queueName =
              Objects.requireNonNullElse(schedule.queueName(), Constants.DBOS_INTERNAL_QUEUE);
          if (dbosExecutor.findQueue(queueName).isEmpty()) {
            logger.error(
                "Workflow schedule {} has invalid queue {}", schedule.scheduleName(), queueName);
            continue;
          }

          Cron cron;
          try {
            cron = CRON_PARSER.parse(schedule.cron()).validate();
          } catch (Exception e) {
            logger.error(
                "Workflow schedule {} has invalid cron expression {}",
                schedule.scheduleName(),
                schedule.cron(),
                e);
            continue;
          }

          if (schedule.automaticBackfill()
              && schedule.lastFiredAt() != null
              && schedule.lastFiredAt().isBefore(Instant.now())) {
            dbosExecutor.backfillSchedule(
                schedule.scheduleName(), schedule.lastFiredAt(), Instant.now());
          }

          var task =
              new Runnable() {

                final ZoneId timeZone =
                    Objects.requireNonNullElseGet(
                        schedule.cronTimezone(), () -> ZoneId.systemDefault());
                final WorkflowSchedule wfSchedule = schedule;
                final ExecutionTime executionTime = ExecutionTime.forCron(cron);

                ZonedDateTime nextTime = ZonedDateTime.now(timeZone);

                public void schedule() {
                  executionTime
                      .nextExecution(nextTime)
                      .ifPresent(
                          cronTime -> {
                            this.nextTime = cronTime.truncatedTo(ChronoUnit.SECONDS);
                            var prevFuture =
                                workflowScheduleFutures.put(
                                    wfSchedule.id(), scheduleTask(this.nextTime, this));
                            // prevFuture should be null or a scheduled task that already fired.
                            // cancel it anyway just to be sure
                            if (prevFuture != null) {
                              if (!prevFuture.isDone()) {
                                logger.debug(
                                    "Previous scheduled task for {} has not yet completed",
                                    wfSchedule.scheduleName());
                              }
                              prevFuture.cancel(false);
                            }
                          });
                }

                @Override
                public void run() {
                  // if execServiceRef is null, the scheduler service was shut down so don't start
                  // the workflow or schedule the next execution
                  if (execServiceRef.get() == null) {
                    return;
                  }

                  try {
                    if (paused.get()) {
                      logger.debug(
                          "Skipping scheduled workflow {} schedule {} because scheduler is paused",
                          regWorkflow.fullyQualifiedName(),
                          wfSchedule.scheduleName());
                      return;
                    }
                    var args = new Object[] {nextTime.toInstant(), wfSchedule.context()};
                    var workflowId =
                        "sched-%s-%s"
                            .formatted(wfSchedule.scheduleName(), nextTime.toOffsetDateTime());
                    logger.debug(
                        "Queuing scheduled workflow {} schedule {} workflowId {}",
                        regWorkflow.fullyQualifiedName(),
                        wfSchedule.scheduleName(),
                        workflowId);
                    var appVersion = dbosExecutor.getLatestApplicationVersion().versionName();
                    var options =
                        new StartWorkflowOptions(workflowId)
                            .withQueue(queueName)
                            .withAppVersion(appVersion);
                    dbosExecutor.startRegisteredWorkflow(regWorkflow, args, options);
                    systemDatabase.updateScheduleLastFiredAt(
                        wfSchedule.scheduleName(), nextTime.toInstant());
                  } catch (Exception e) {
                    logger.error("Scheduled task {} exception", schedule.scheduleName(), e);
                  } finally {
                    schedule();
                  }
                }
              };

          task.schedule();
        }
      }
    } catch (Exception e) {
      // Catch all exceptions to prevent scheduleAtFixedRate from permanently suppressing future
      // poll invocations. A transient DB failure should not permanently disable the scheduler.
      logger.error("pollWorkflowSchedules failed", e);
    }
  }

  private ScheduledFuture<?> scheduleTask(ZonedDateTime nextTime, Runnable task) {
    // to prevent the "thundering herd" problem in a distributed setting,
    // apply a jitter of up to 10% of the sleep time, capped at 10 seconds
    var sleepTime = Duration.between(ZonedDateTime.now(), Objects.requireNonNull(nextTime));
    sleepTime = sleepTime.isNegative() ? Duration.ZERO : sleepTime;
    var tenPctSleepTime = sleepTime.dividedBy(10);
    var maxJitter = tenPctSleepTime.compareTo(MAX_JITTER) > 0 ? MAX_JITTER : tenPctSleepTime;
    if (!maxJitter.isZero()) {
      var jitterNanos = ThreadLocalRandom.current().nextLong(maxJitter.toNanos());
      sleepTime = sleepTime.plus(Duration.ofNanos(jitterNanos));
    }

    var execService = execServiceRef.get();
    return execService == null
        ? null
        : execService.schedule(task, sleepTime.toMillis(), TimeUnit.MILLISECONDS);
  }

  private void cancelWorkflowSchedule(String scheduleId) {
    var future = workflowScheduleFutures.remove(scheduleId);
    if (future != null) {
      future.cancel(false);
    }
  }
}
