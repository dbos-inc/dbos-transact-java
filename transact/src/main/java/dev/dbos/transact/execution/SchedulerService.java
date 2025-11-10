package dev.dbos.transact.execution;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.database.ExternalState;
import dev.dbos.transact.workflow.Scheduled;

import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchedulerService implements DBOSLifecycleListener {

  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);

  private static final Logger logger = LoggerFactory.getLogger(SchedulerService.class);
  private static final CronParser cronParser =
      new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.SPRING53));

  private volatile boolean stop = false;
  private final String schedulerQueueName;

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
    startScheduledWorkflows();
    stop = false;
  }

  public void dbosShutDown() {
    stop = true;
    List<Runnable> notRun = scheduler.shutdownNow();
    logger.debug("Shutting down scheduler service. Tasks not run {}", notRun.size());
  }

  record ScheduledWorkflow(
      RegisteredWorkflow workflow, Cron cron, String queue, boolean ignoreMissed) {}

  private ZonedDateTime getNextTime(ScheduledWorkflow swf) {
    ZonedDateTime now = null;
    if (swf.ignoreMissed()) {
      now = ZonedDateTime.now();
    } else {
      var extstate =
          DBOS.getExternalState(
              "DBOS.SchedulerService", swf.workflow().fullyQualifiedName(), "lastTime");
      if (extstate.isPresent()) {
        now = ZonedDateTime.parse(extstate.get().value());
      } else {
        now = ZonedDateTime.now(ZoneOffset.UTC);
      }
    }
    return now;
  }

  private ZonedDateTime setLastTime(ScheduledWorkflow swf, ZonedDateTime lastTime) {
    var nes =
        new ExternalState(
            "DBOS.SchedulerService",
            swf.workflow().fullyQualifiedName(),
            "lastTime",
            lastTime.toString(),
            null,
            BigInteger.valueOf(lastTime.toInstant().toEpochMilli()));
    var upstate = DBOS.upsertExternalState(nes);
    return ZonedDateTime.parse(upstate.value());
  }

  private void startScheduledWorkflows() {

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

    for (var swf : scheduledWorkflows) {
      ExecutionTime executionTime = ExecutionTime.forCron(swf.cron());

      var wf = swf.workflow();

      var task =
          new Runnable() {
            @Override
            public void run() {

              ZonedDateTime scheduledTime = getNextTime(swf);
              try {
                Object[] args = new Object[2];
                args[0] = scheduledTime.toInstant();
                args[1] = ZonedDateTime.now(ZoneOffset.UTC).toInstant();
                logger.debug("submitting to dbos Executor {}", wf.fullyQualifiedName());
                String workflowId =
                    String.format("sched-%s-%s", wf.fullyQualifiedName(), scheduledTime.toString());
                var options =
                    new ExecuteWorkflowOptions(workflowId, null, null, swf.queue(), null, null);
                DBOS.executeWorkflow(wf, args, options);

              } catch (Exception e) {
                logger.error("Scheduled task exception {}", wf.fullyQualifiedName(), e);
              }

              if (!stop) {
                ZonedDateTime now =
                    swf.ignoreMissed()
                        ? ZonedDateTime.now(ZoneOffset.UTC)
                        : setLastTime(swf, scheduledTime);
                logger.debug(
                    "Scheduling the next execution {} {}", wf.fullyQualifiedName(), now.toString());
                executionTime
                    .nextExecution(now)
                    .ifPresent(
                        nextTime -> {
                          logger.debug("Next execution time {}", nextTime);
                          long delayMs = Duration.between(now, nextTime).toMillis();
                          scheduler.schedule(
                              this, delayMs < 0 ? 0 : delayMs, TimeUnit.MILLISECONDS);
                        });
              }
            }
          };

      // Kick off the first run (but only scheduled at the next proper time)
      ZonedDateTime cnow = getNextTime(swf);
      executionTime
          .nextExecution(cnow)
          .ifPresent(
              nextTime -> {
                long initialDelayMs = Duration.between(cnow, nextTime).toMillis();
                scheduler.schedule(
                    task, initialDelayMs < 0 ? 0 : initialDelayMs, TimeUnit.MILLISECONDS);
              });
    }
  }
}
