package dev.dbos.transact.execution;

import dev.dbos.transact.Constants;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.database.ExternalState;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.workflow.Scheduled;
import dev.dbos.transact.workflow.WorkflowSchedule;

import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
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

  record AnnotatedScheduledWorkflow(
      RegisteredWorkflow workflow, Cron cron, String queue, boolean ignoreMissed) {}

  public static final CronParser CRON_PARSER =
      new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.SPRING53));
  private static final Class<?>[] ANNOTATED_EXPECTED_PARAMETERS =
      new Class<?>[] {Instant.class, Instant.class};
  private static final Class<?>[] EXPECTED_PARAMETERS =
      new Class<?>[] {Instant.class, Object.class};
  private static final Duration MAX_JITTER = Duration.ofSeconds(10);

  public static void validateAnnotatedWorkflowSchedule(RegisteredWorkflow workflow) {
    var method = workflow.workflowMethod();
    var skedTag = method.getAnnotation(Scheduled.class);
    if (skedTag != null) {
      var paramTypes = method.getParameterTypes();
      if (!Arrays.equals(paramTypes, ANNOTATED_EXPECTED_PARAMETERS)) {
        throw new IllegalArgumentException(
            "Invalid signature for annotated workflow schedule %s. Signature must be (Instant, Instant)"
                .formatted(workflow.fullyQualifiedName()));
      }

      CRON_PARSER.parse(skedTag.cron()).validate();
    }
  }

  private final DBOSExecutor dbosExecutor;
  private final SystemDatabase systemDatabase;
  private final Duration pollingInterval;
  private final AtomicReference<ScheduledExecutorService> execServiceRef = new AtomicReference<>();
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
        startAnnotatedSchedules();
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

  private void pollWorkflowSchedules() {
    try {
      pollWorkflowSchedulesImpl();
    } catch (Exception e) {
      // Catch all exceptions to prevent scheduleAtFixedRate from permanently suppressing future
      // poll invocations. A transient DB failure should not permanently disable the scheduler.
      logger.error("pollWorkflowSchedules failed", e);
    }
  }

  private void pollWorkflowSchedulesImpl() {
    // if execServiceRef is null, the scheduler service was shut down so don't poll schedules
    if (execServiceRef.get() == null) {
      return;
    }

    var schedules = dbosExecutor.listSchedules(null, null, null);

    // shut down any scheduled future that isn't in the list of current schedules
    var scheduleIds = schedules.stream().map(s -> s.id()).collect(Collectors.toSet());
    for (var key : workflowScheduleFutures.keySet()) {
      if (!scheduleIds.contains(key)) {
        cancelWorkflowSchedule(key);
      }
    }

    for (var schedule : schedules) {

      if (!schedule.isActive()) {
        if (workflowScheduleFutures.containsKey(schedule.id())) {
          cancelWorkflowSchedule(schedule.id());
        }
      } else {
        var optRegWf = dbosExecutor.getWorkflow(schedule.workflowName(), schedule.className());
        if (optRegWf.isEmpty()) {
          logger.error(
              "Workflow schedule {} has missing workflow function {}",
              schedule.scheduleName(),
              RegisteredWorkflow.fullyQualifiedName(schedule.className(), schedule.workflowName()));
          continue;
        }

        var regWorkflow = optRegWf.orElseThrow();
        if (!Arrays.equals(regWorkflow.workflowMethod().getParameterTypes(), EXPECTED_PARAMETERS)) {
          logger.error(
              "Workflow schedule {} workflow {} has invalid signature, signature must be (Instant, Object)",
              schedule.scheduleName(),
              regWorkflow.fullyQualifiedName());
          continue;
        }

        final String queueName =
            Objects.requireNonNullElse(schedule.queueName(), Constants.DBOS_INTERNAL_QUEUE);
        if (dbosExecutor.getQueue(queueName).isEmpty()) {
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
                  Objects.requireNonNullElse(schedule.cronTimezone(), ZoneId.systemDefault());
              final WorkflowSchedule wfSchedule = schedule;
              final ExecutionTime executionTime = ExecutionTime.forCron(cron);

              ZonedDateTime nextTime = ZonedDateTime.now(timeZone);

              public void schedule() {
                executionTime
                    .nextExecution(nextTime)
                    .ifPresent(
                        cronTime -> {
                          this.nextTime = cronTime.truncatedTo(ChronoUnit.SECONDS);
                          // prevFuture should be null or a scheduled task that already fired.
                          // but we still cancel it just to be sure
                          var prevFuture =
                              workflowScheduleFutures.put(
                                  wfSchedule.id(), scheduleTask(this.nextTime, this));
                          if (prevFuture != null) {
                            prevFuture.cancel(false);
                          }
                        });
              }

              @Override
              public void run() {
                // if execServiceRef is null, the scheduler service was shut down so don't start the
                // workflow or schedule the next execution
                if (execServiceRef.get() == null) {
                  return;
                }

                try {
                  var args = new Object[] {nextTime.toInstant(), wfSchedule.context()};
                  logger.debug(
                      "starting scheduled workflow {} at {}",
                      regWorkflow.fullyQualifiedName(),
                      nextTime);
                  var workflowId = "sched-%s-%s".formatted(wfSchedule.scheduleName(), nextTime);
                  var appVersion = dbosExecutor.getLatestApplicationVersion().versionName();
                  var options =
                      new StartWorkflowOptions(workflowId)
                          .withQueue(queueName)
                          .withAppVersion(appVersion);
                  dbosExecutor.startWorkflow(regWorkflow, args, options);
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
  }

  private void startAnnotatedSchedules() {
    logger.debug("startAnnotatedSchedules");

    for (var swf : getAnnotatedWorkflowSchedules()) {
      var task =
          new Runnable() {

            final ExecutionTime executionTime = ExecutionTime.forCron(swf.cron());
            final String workflowName = swf.workflow().fullyQualifiedName();

            ZonedDateTime nextTime = getLastTime(dbosExecutor, swf);

            public void schedule() {
              executionTime
                  .nextExecution(nextTime)
                  .ifPresent(
                      cronTime -> {
                        this.nextTime = cronTime.truncatedTo(ChronoUnit.SECONDS);
                        scheduleTask(this.nextTime, this);
                      });
            }

            @Override
            public void run() {
              // if execServiceRef is null, the scheduler service was shut down so don't start the
              // workflow or schedule the next execution
              if (execServiceRef.get() == null) {
                return;
              }

              var scheduledTime = nextTime;
              try {
                var args = new Object[] {scheduledTime.toInstant(), Instant.now()};
                logger.debug("starting scheduled workflow {} at {}", workflowName, args[1]);
                var workflowId = "sched-%s-%s".formatted(workflowName, scheduledTime);
                var appVersion = dbosExecutor.getLatestApplicationVersion().versionName();
                var options =
                    new StartWorkflowOptions(workflowId)
                        .withQueue(swf.queue())
                        .withAppVersion(appVersion);
                dbosExecutor.startWorkflow(swf.workflow(), args, options);
                nextTime = setLastTime(dbosExecutor, swf, scheduledTime);
              } catch (Exception e) {
                logger.error("Annotated scheduled task exception {}", workflowName, e);
              } finally {
                schedule();
              }
            }
          };
      task.schedule();
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

  private static ZonedDateTime getLastTime(
      DBOSExecutor dbosExecutor, AnnotatedScheduledWorkflow swf) {
    if (!swf.ignoreMissed()) {
      var state =
          dbosExecutor.getExternalState(
              "DBOS.SchedulerService", swf.workflow().fullyQualifiedName(), "lastTime");
      if (state.isPresent()) {
        return ZonedDateTime.parse(state.get().value());
      }
    }
    return ZonedDateTime.now();
  }

  private static ZonedDateTime setLastTime(
      DBOSExecutor dbosExecutor, AnnotatedScheduledWorkflow swf, ZonedDateTime lastTime) {
    if (swf.ignoreMissed()) {
      return ZonedDateTime.now();
    }

    var state =
        dbosExecutor.upsertExternalState(
            new ExternalState(
                "DBOS.SchedulerService",
                swf.workflow().fullyQualifiedName(),
                "lastTime",
                lastTime.toString(),
                null,
                BigInteger.valueOf(lastTime.toInstant().toEpochMilli())));

    return ZonedDateTime.parse(state.value()).plus(1, ChronoUnit.MILLIS);
  }

  private List<AnnotatedScheduledWorkflow> getAnnotatedWorkflowSchedules() {
    return dbosExecutor.getWorkflows().stream()
        .map(
            wf -> {
              var method = wf.workflowMethod();
              var schedTag = method.getAnnotation(Scheduled.class);
              if (schedTag == null) {
                return null;
              }

              if (!Arrays.equals(method.getParameterTypes(), ANNOTATED_EXPECTED_PARAMETERS)) {
                logger.error(
                    "Annotated workflow schedule {} has invalid signature, signature must be (Instant, Instant)",
                    wf.fullyQualifiedName());
                return null;
              }

              var queueName =
                  schedTag.queue().isEmpty() ? Constants.DBOS_INTERNAL_QUEUE : schedTag.queue();
              if (dbosExecutor.getQueue(queueName).isEmpty()) {
                logger.error(
                    "Annotated workflow schedule {} refers to undefined queue {}",
                    wf.fullyQualifiedName(),
                    queueName);
                return null;
              }

              try {
                var cron = CRON_PARSER.parse(schedTag.cron()).validate();
                return new AnnotatedScheduledWorkflow(wf, cron, queueName, schedTag.ignoreMissed());
              } catch (Exception e) {
                logger.error(
                    "Annotated workflow schedule {} has invalid cron expression {}",
                    wf.fullyQualifiedName(),
                    schedTag.cron(),
                    e);
                return null;
              }
            })
        .filter(Objects::nonNull)
        .toList();
  }
}
