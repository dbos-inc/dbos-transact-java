package dev.dbos.transact.execution;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.execution.DBOSExecutor.ExecuteWorkflowOptions;
import dev.dbos.transact.workflow.Queue;
import dev.dbos.transact.workflow.Scheduled;

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

public class SchedulerService {

  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);

  private static final Logger logger = LoggerFactory.getLogger(SchedulerService.class);
  private static final CronParser cronParser =
      new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.SPRING53));

  private final DBOSExecutor dbosExecutor;
  private volatile boolean stop = false;
  private final Queue schedulerQueue;
  private final String schedulerQueueName;

  public SchedulerService(DBOSExecutor dbosExecutor, Queue schedulerQueue) {
    this.dbosExecutor = Objects.requireNonNull(dbosExecutor);
    this.schedulerQueue = Objects.requireNonNull(schedulerQueue);
    this.schedulerQueueName = this.schedulerQueue.name();
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

  public void start() {
    startScheduledWorkflows();
    stop = false;
  }

  public void stop() {
    stop = true;
    List<Runnable> notRun = scheduler.shutdownNow();
    logger.debug("Shutting down scheduler service. Tasks not run {}", notRun.size());
  }

  private void startScheduledWorkflows() {

    var expectedParams = new Class<?>[] {Instant.class, Instant.class};

    // collect all workflows that have an @Scheduled annotation
    record ScheduledWorkflow(RegisteredWorkflow workflow, Cron cron) {}
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

      try {
        var cron = cronParser.parse(skedTag.cron());
        scheduledWorkflows.add(new ScheduledWorkflow(wf, Objects.requireNonNull(cron)));
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

              try {
                ZonedDateTime scheduledTime = ZonedDateTime.now(ZoneOffset.UTC);
                Object[] args = new Object[2];
                args[0] = scheduledTime.toInstant();
                args[1] = ZonedDateTime.now(ZoneOffset.UTC).toInstant();
                logger.debug("submitting to dbos Executor {}", wf.fullyQualifiedName());
                String workflowId =
                    String.format("sched-%s-%s", wf.fullyQualifiedName(), scheduledTime.toString());
                var options =
                    new ExecuteWorkflowOptions(
                        workflowId, null, null, schedulerQueueName, null, null);
                DBOS.executeWorkflow(wf, args, options);
              } catch (Exception e) {
                logger.error("Scheduled task exception {}", wf.fullyQualifiedName(), e);
              }

              if (!stop) {
                logger.debug("Scheduling the next execution {}", wf.fullyQualifiedName());
                ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
                executionTime
                    .nextExecution(now)
                    .ifPresent(
                        nextTime -> {
                          logger.debug("Next execution time {}", nextTime);
                          long delayMs = Duration.between(now, nextTime).toMillis();
                          scheduler.schedule(this, delayMs, TimeUnit.MILLISECONDS);
                        });
              }
            }
          };

      // Kick off the first run (but only scheduled at the next proper time)
      ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
      executionTime
          .nextExecution(now)
          .ifPresent(
              nextTime -> {
                long initialDelayMs = Duration.between(now, nextTime).toMillis();
                scheduler.schedule(task, initialDelayMs, TimeUnit.MILLISECONDS);
              });
    }
  }
}
