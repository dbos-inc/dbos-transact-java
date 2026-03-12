package dev.dbos.transact.execution;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.database.ExternalState;
import dev.dbos.transact.workflow.Scheduled;

import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchedulerService implements DBOSLifecycleListener {

  record ScheduledWorkflow(
      RegisteredWorkflow workflow, Cron cron, String queue, boolean ignoreMissed) {}

  private static final Logger logger = LoggerFactory.getLogger(SchedulerService.class);
  private static final CronParser cronParser =
      new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.SPRING53));
  private static final Class<?>[] expectedParams = new Class<?>[] {Instant.class, Instant.class};

  private final String defaultSchedulerQueueName;
  private final AtomicReference<DBOS> dbosRef = new AtomicReference<>();
  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);

  public SchedulerService(String defaultSchedulerQueueName) {
    this.defaultSchedulerQueueName = Objects.requireNonNull(defaultSchedulerQueueName);
  }

  public static void validateScheduledWorkflow(RegisteredWorkflow workflow) {
    var method = workflow.workflowMethod();
    var skedTag = method.getAnnotation(Scheduled.class);
    if (skedTag != null) {
      var paramTypes = method.getParameterTypes();
      if (!Arrays.equals(paramTypes, expectedParams)) {
        throw new IllegalArgumentException(
            "Invalid signature for Scheduled workflow %s. Signature must be (Instant, Instant)"
                .formatted(workflow.fullyQualifiedName()));
      }

      cronParser.parse(skedTag.cron());
    }
  }

  @Override
  public void dbosLaunched(DBOS dbos) {
    DBOS prev = this.dbosRef.getAndUpdate(existing -> existing == null ? dbos : existing);
    if (prev == null) {
      startScheduledWorkflows(dbos);
    } else if (prev != dbos) {
      throw new IllegalStateException(
          "SchedulerService already initialized with a different DBOS instance");
    }
  }

  @Override
  public void dbosShutDown() {
    DBOS prev = this.dbosRef.getAndSet(null);
    if (prev != null) {
      List<Runnable> notRun = scheduler.shutdownNow();
      logger.debug("Shutting down scheduler service. Tasks not run {}", notRun.size());
    }
  }

  private static ZonedDateTime getLastTime(DBOS dbos, ScheduledWorkflow swf) {
    if (!swf.ignoreMissed()) {
      var state =
          dbos.getExternalState(
              "DBOS.SchedulerService", swf.workflow().fullyQualifiedName(), "lastTime");
      if (state.isPresent()) {
        return ZonedDateTime.parse(state.get().value());
      }
    }
    return ZonedDateTime.now(ZoneOffset.UTC).withNano(0);
  }

  private static ZonedDateTime setLastTime(
      DBOS dbos, ScheduledWorkflow swf, ZonedDateTime lastTime) {
    if (swf.ignoreMissed()) {
      return ZonedDateTime.now(ZoneOffset.UTC).withNano(0);
    }

    var state =
        dbos.upsertExternalState(
            new ExternalState(
                "DBOS.SchedulerService",
                swf.workflow().fullyQualifiedName(),
                "lastTime",
                lastTime.toString(),
                null,
                BigInteger.valueOf(lastTime.toInstant().toEpochMilli())));
    return ZonedDateTime.parse(state.value()).plus(1, ChronoUnit.MILLIS);
  }

  private static List<ScheduledWorkflow> getScheduledWorkflows(
      DBOS dbos, String defaultSchedulerQueueName) {
    var registeredWorkflows = dbos.getRegisteredWorkflows();
    var scheduledWorkflows = new ArrayList<ScheduledWorkflow>();
    for (var wf : registeredWorkflows) {
      var method = wf.workflowMethod();
      var skedTag = method.getAnnotation(Scheduled.class);
      if (skedTag == null) {
        continue;
      }

      var paramTypes = method.getParameterTypes();
      if (!Arrays.equals(paramTypes, expectedParams)) {
        logger.error(
            "Scheduled workflow {} has invalid signature, signature must be (Instant, Instant)",
            wf.fullyQualifiedName());
        continue;
      }

      // fields of Java annotations can't be null.
      // @Scheduled.queue defaults to empty string if not specified
      // using requireNonNullElse here for safety purposes
      var queueName = Objects.requireNonNullElse(skedTag.queue(), "");
      queueName = queueName.isEmpty() ? defaultSchedulerQueueName : queueName;
      var queue = dbos.getQueue(queueName);
      if (!queue.isPresent()) {
        logger.error(
            "Scheduled workflow {} refers to undefined queue {}",
            wf.fullyQualifiedName(),
            queueName);
        continue;
      }

      try {
        var cron = cronParser.parse(skedTag.cron());
        scheduledWorkflows.add(
            new ScheduledWorkflow(
                wf, Objects.requireNonNull(cron), queueName, skedTag.ignoreMissed()));
      } catch (IllegalArgumentException e) {
        logger.error(
            "Scheduled workflow {} has invalid cron expression {}",
            wf.fullyQualifiedName(),
            skedTag.cron());
      }
    }
    return scheduledWorkflows;
  }

  private void startScheduledWorkflows(DBOS dbos) {
    logger.debug("startScheduledWorkflows");

    var scheduledWorkflows = getScheduledWorkflows(dbos, defaultSchedulerQueueName);
    for (var _scheduledWorkflow : scheduledWorkflows) {
      var _nextTime = getLastTime(dbos, _scheduledWorkflow);
      var task =
          new Runnable() {
            final ScheduledWorkflow scheduledWorkflow = _scheduledWorkflow;
            final ExecutionTime executionTime = ExecutionTime.forCron(scheduledWorkflow.cron());
            final String workflowName = scheduledWorkflow.workflow().fullyQualifiedName();
            ZonedDateTime nextTime = _nextTime;

            public void schedule() {
              executionTime
                  .nextExecution(nextTime)
                  .ifPresent(
                      _nextTime -> {
                        this.nextTime = _nextTime;
                        long initialDelayMs =
                            Duration.between(ZonedDateTime.now(ZoneOffset.UTC), _nextTime)
                                .toMillis();
                        // ensure scheduler hasn't been shutdown before scheduling
                        if (dbosRef.get() != null) {
                          logger.debug("Scheduling {} @ {}", workflowName, _nextTime);
                          scheduler.schedule(
                              this, initialDelayMs < 0 ? 0 : initialDelayMs, TimeUnit.MILLISECONDS);
                        }
                      });
            }

            @Override
            public void run() {
              // if dbos is null, the scheduler service was shut down so don't start the workflow or
              // schedule the next execution
              var dbos = dbosRef.get();
              if (dbos == null) {
                return;
              }

              ZonedDateTime scheduledTime = nextTime;
              try {
                Object[] args = new Object[2];
                args[0] = scheduledTime.toInstant();
                args[1] = ZonedDateTime.now(ZoneOffset.UTC).toInstant();

                logger.debug("starting scheduled workflow {} at {}", workflowName, args[1]);

                String workflowId =
                    String.format("sched-%s-%s", workflowName, scheduledTime.toString());
                var options =
                    new StartWorkflowOptions(workflowId).withQueue(scheduledWorkflow.queue());
                dbos.startWorkflow(scheduledWorkflow.workflow(), args, options);
                nextTime = setLastTime(dbos, scheduledWorkflow, scheduledTime);
              } catch (Exception e) {
                logger.error("Scheduled task exception {}", workflowName, e);
              } finally {
                schedule();
              }
            }
          };

      task.schedule();
    }
  }
}
