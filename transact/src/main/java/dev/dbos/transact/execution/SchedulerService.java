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
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchedulerService implements AutoCloseable {

  // Tracks a running schedule: the definition fields are a snapshot used to detect definition
  // changes, the future is the currently scheduled task which is replaced on every firing.
  private record RunningSchedule(
      String workflowName,
      String className,
      String cron,
      Object context,
      ZoneId cronTimezone,
      String queueName,
      AtomicReference<ScheduledFuture<?>> future) {

    RunningSchedule(WorkflowSchedule schedule) {
      this(
          schedule.workflowName(),
          schedule.className(),
          schedule.cron(),
          schedule.context(),
          schedule.cronTimezone(),
          schedule.queueName(),
          new AtomicReference<>());
    }

    boolean matches(WorkflowSchedule schedule) {
      return Objects.equals(workflowName, schedule.workflowName())
          && Objects.equals(className, schedule.className())
          && Objects.equals(cron, schedule.cron())
          && Objects.equals(context, schedule.context())
          && Objects.equals(cronTimezone, schedule.cronTimezone())
          && Objects.equals(queueName, schedule.queueName());
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(SchedulerService.class);

  public static final CronParser CRON_PARSER =
      new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.SPRING53));
  private static final Class<?>[] EXPECTED_PARAMETERS =
      new Class<?>[] {Instant.class, Object.class};
  private static final Duration MAX_JITTER = Duration.ofSeconds(10);

  // During the first minute after startup, poll for schedules every second so schedules registered
  // around launch are picked up promptly rather than after a full polling interval.
  private static final Duration STARTUP_FAST_POLL_DURATION = Duration.ofSeconds(60);
  private static final Duration STARTUP_FAST_POLL_INTERVAL = Duration.ofSeconds(1);

  private final DBOSExecutor dbosExecutor;
  private final SystemDatabase systemDatabase;
  private final Duration pollingInterval;
  private final AtomicReference<ScheduledExecutorService> execServiceRef = new AtomicReference<>();
  private final AtomicBoolean paused = new AtomicBoolean(false);
  private final ConcurrentHashMap<String, RunningSchedule> runningSchedules =
      new ConcurrentHashMap<>();
  // Ensures the fast-path poll and the regular fixed-rate poll never run concurrently, which would
  // otherwise race when scheduling new workflow futures.
  private final ReentrantLock pollLock = new ReentrantLock();

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

        // Fast-path: poll more frequently for the first minute after startup. Only needed when the
        // configured polling interval is slower than the fast interval.
        long fastIntervalMs =
            Math.min(STARTUP_FAST_POLL_INTERVAL.toMillis(), pollingInterval.toMillis());
        if (fastIntervalMs < pollingInterval.toMillis()) {
          var fastFuture =
              scheduler.scheduleAtFixedRate(
                  this::pollWorkflowSchedules,
                  fastIntervalMs,
                  fastIntervalMs,
                  TimeUnit.MILLISECONDS);
          scheduler.schedule(
              () -> fastFuture.cancel(false),
              STARTUP_FAST_POLL_DURATION.toMillis(),
              TimeUnit.MILLISECONDS);
        }
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
    // Skip if another poll (fast-path or regular) is already running
    if (!pollLock.tryLock()) {
      return;
    }
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

      // shut down any running schedule that isn't in the list of current schedules
      var currentIds = schedules.stream().map(WorkflowSchedule::id).collect(Collectors.toSet());
      for (var key : runningSchedules.keySet()) {
        if (!currentIds.contains(key)) {
          cancelWorkflowSchedule(key);
        }
      }

      for (var schedule : schedules) {
        var running = runningSchedules.get(schedule.id());
        if (!schedule.isActive()) {
          // if the schedule is no longer active, cancel any running future we have for it
          if (running != null) {
            cancelWorkflowSchedule(schedule.id());
          }
        } else if (running != null) {
          // already running: applySchedules upserts in place, so a changed definition doesn't
          // change schedule.id(). Detect the change and restart with the new definition; no
          // backfill needed since the schedule was already running.
          if (!running.matches(schedule)) {
            cancelWorkflowSchedule(schedule.id());
            startWorkflowSchedule(schedule, false);
          }
        } else {
          // if schedule is active but we don't have a running future for it, schedule it now
          startWorkflowSchedule(schedule, true);
        }
      }
    } catch (Exception e) {
      // Catch all exceptions to prevent scheduleAtFixedRate from permanently suppressing future
      // poll invocations. A transient DB failure should not permanently disable the scheduler.
      logger.error("pollWorkflowSchedules failed", e);
    } finally {
      pollLock.unlock();
    }
  }

  // allowBackfill should be false when restarting an already-running schedule whose definition
  // changed; backfill only applies the first time a schedule starts firing.
  private void startWorkflowSchedule(WorkflowSchedule schedule, boolean allowBackfill) {
    var optRegWf =
        dbosExecutor.getRegisteredWorkflow(schedule.workflowName(), schedule.className(), "");
    if (optRegWf.isEmpty()) {
      logger.error(
          "Workflow schedule {} has missing workflow function {}",
          schedule.scheduleName(),
          RegisteredWorkflow.fullyQualifiedName(schedule.workflowName(), schedule.className()));
      return;
    }

    var regWorkflow = optRegWf.orElseThrow();
    if (!Arrays.equals(regWorkflow.workflowMethod().getParameterTypes(), EXPECTED_PARAMETERS)) {
      logger.error(
          "Workflow schedule {} workflow {} has invalid signature, signature must be (Instant, Object)",
          schedule.scheduleName(),
          regWorkflow.fullyQualifiedName());
      return;
    }

    final String queueName =
        Objects.requireNonNullElse(schedule.queueName(), Constants.DBOS_INTERNAL_QUEUE);
    if (dbosExecutor.findQueue(queueName).isEmpty()) {
      logger.error("Workflow schedule {} has invalid queue {}", schedule.scheduleName(), queueName);
      return;
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
      return;
    }

    if (allowBackfill
        && schedule.automaticBackfill()
        && schedule.lastFiredAt() != null
        && schedule.lastFiredAt().isBefore(Instant.now())) {
      dbosExecutor.backfillSchedule(schedule.scheduleName(), schedule.lastFiredAt(), Instant.now());
    }

    var running = new RunningSchedule(schedule);

    var task =
        new Runnable() {

          final ZoneId timeZone =
              Objects.requireNonNullElseGet(schedule.cronTimezone(), () -> ZoneId.systemDefault());
          final WorkflowSchedule wfSchedule = schedule;
          final ExecutionTime executionTime = ExecutionTime.forCron(cron);

          ZonedDateTime nextTime = ZonedDateTime.now(timeZone);

          // Returns true if a future was scheduled for the next execution, false if the cron
          // expression has no next execution (eg. a day-of-month/month combination that can
          // never occur, like April 31st).
          public boolean schedule() {
            return executionTime
                .nextExecution(nextTime)
                .map(
                    cronTime -> {
                      this.nextTime = cronTime.truncatedTo(ChronoUnit.SECONDS);
                      var prevFuture =
                          running.future().getAndSet(scheduleTask(this.nextTime, this));
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
                      return true;
                    })
                .orElse(false);
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
                  "sched-%s-%s".formatted(wfSchedule.scheduleName(), nextTime.toOffsetDateTime());
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

    if (task.schedule()) {
      runningSchedules.put(schedule.id(), running);
    } else {
      logger.error(
          "Workflow schedule {} cron expression {} has no upcoming execution; not starting",
          schedule.scheduleName(),
          schedule.cron());
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
    var running = runningSchedules.remove(scheduleId);
    if (running != null) {
      var future = running.future().get();
      if (future != null) {
        future.cancel(false);
      }
    }
  }
}
