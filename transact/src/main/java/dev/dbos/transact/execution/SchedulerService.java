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

  private static final Logger logger = LoggerFactory.getLogger(SchedulerService.class);
  private static final CronParser cronParser =
      new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.SPRING53));

  private final String schedulerQueueName;
  private final AtomicReference<ScheduledExecutorService> scheduler = new AtomicReference<>();

  public SchedulerService(String defSchedulerQueue) {
    this.schedulerQueueName = Objects.requireNonNull(defSchedulerQueue);
  }

  public static void validateScheduledWorkflow(RegisteredWorkflow workflow) {
    var method = workflow.workflowMethod();
    var skedTag = method.getAnnotation(Scheduled.class);
    if (skedTag == null) {
      return;
    }

    var expectedParams = new Class<?>[] {Instant.class, Instant.class};
    var paramTypes = method.getParameterTypes();
    if (!Arrays.equals(paramTypes, expectedParams)) {
      throw new IllegalArgumentException(
          "Invalid signature for Scheduled workflow %s. Signature must be (Instant, Instant)"
              .formatted(workflow.fullyQualifiedName()));
    }

    cronParser.parse(skedTag.cron());
  }

  public void dbosLaunched() {
    if (this.scheduler.get() == null) {
      var scheduler = Executors.newScheduledThreadPool(4);
      if (this.scheduler.compareAndSet(null, scheduler)) {
        startScheduledWorkflows();
      }
    }
  }

  public void dbosShutDown() {
    var scheduler = this.scheduler.getAndSet(null);
    if (scheduler != null) {
      List<Runnable> notRun = scheduler.shutdownNow();
      logger.debug("Shutting down scheduler service. Tasks not run {}", notRun.size());
    }
  }

  record ScheduledWorkflow(
      RegisteredWorkflow workflow, Cron cron, String queue, boolean ignoreMissed) {}

  private ZonedDateTime getLastTime(ScheduledWorkflow swf) {
    if (!swf.ignoreMissed()) {
      var state =
          DBOS.getExternalState(
              "DBOS.SchedulerService", swf.workflow().fullyQualifiedName(), "lastTime");
      if (state.isPresent()) {
        return ZonedDateTime.parse(state.get().value());
      }
    }
    return ZonedDateTime.now(ZoneOffset.UTC).withNano(0);
  }

  private ZonedDateTime setLastTime(ScheduledWorkflow swf, ZonedDateTime lastTime) {
    if (swf.ignoreMissed()) {
      return ZonedDateTime.now(ZoneOffset.UTC).withNano(0);
    }

    var state =
        DBOS.upsertExternalState(
            new ExternalState(
                "DBOS.SchedulerService",
                swf.workflow().fullyQualifiedName(),
                "lastTime",
                lastTime.toString(),
                null,
                BigInteger.valueOf(lastTime.toInstant().toEpochMilli())));
    return ZonedDateTime.parse(state.value()).plus(1, ChronoUnit.MILLIS);
  }

  private void startScheduledWorkflows() {
    logger.debug("startScheduledWorkflows");

    var expectedParams = new Class<?>[] {Instant.class, Instant.class};

    // collect all workflows that have an @Scheduled annotation
    List<ScheduledWorkflow> scheduledWorkflows = new ArrayList<>();
    for (var wf : DBOS.getRegisteredWorkflows()) {
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

      String queue =
          skedTag.queue() != null && !skedTag.queue().isEmpty()
              ? skedTag.queue()
              : this.schedulerQueueName;
      var q = DBOS.getQueue(queue);
      if (!q.isPresent()) {
        logger.error(
            "Scheduled workflow {} refers to undefined queue {}", wf.fullyQualifiedName(), queue);
        queue = this.schedulerQueueName;
      }

      try {
        var cron = cronParser.parse(skedTag.cron());
        scheduledWorkflows.add(
            new ScheduledWorkflow(wf, Objects.requireNonNull(cron), queue, skedTag.ignoreMissed()));
      } catch (IllegalArgumentException e) {
        logger.error(
            "Scheduled workflow {} has invalid cron expression {}",
            wf.fullyQualifiedName(),
            skedTag.cron());
      }
    }

    for (var _swf : scheduledWorkflows) {

      var task =
          new Runnable() {
            final ScheduledWorkflow swf = _swf;
            final ExecutionTime executionTime = ExecutionTime.forCron(swf.cron());
            final String workflowName = swf.workflow().fullyQualifiedName();

            ZonedDateTime nextTime = getLastTime(swf);

            public void schedule() {
              executionTime
                  .nextExecution(nextTime)
                  .ifPresent(
                      nextTime -> {
                        this.nextTime = nextTime;
                        long initialDelayMs =
                            Duration.between(ZonedDateTime.now(ZoneOffset.UTC), nextTime)
                                .toMillis();
                        // ensure scheduler hasn't been shutdown before scheduling
                        var localScheduler = scheduler.get();
                        if (localScheduler != null) {
                          logger.debug("Scheduling {} @ {}", workflowName, nextTime);

                          localScheduler.schedule(
                              this, initialDelayMs < 0 ? 0 : initialDelayMs, TimeUnit.MILLISECONDS);
                        }
                      });
            }

            @Override
            public void run() {
              // if scheduler service isn't running, the scheduler service was shut down so don't
              // start the workflow or schedule the next execution
              if (scheduler.get() == null) {
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
                var options = new StartWorkflowOptions(workflowId).withQueue(swf.queue());
                DBOS.startWorkflow(swf.workflow(), args, options);
                nextTime = setLastTime(swf, scheduledTime);
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
