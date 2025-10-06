package dev.dbos.transact.scheduled;

import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.execution.DBOSExecutor.ExecuteWorkflowOptions;
import dev.dbos.transact.execution.RegisteredWorkflow;
import dev.dbos.transact.queue.Queue;

import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;
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

  public record ScheduledInstance(
      String className, String workflowName, Object instance, Cron cron) {}

  private static final Logger logger = LoggerFactory.getLogger(SchedulerService.class);
  private static final CronParser cronParser =
      new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ));

  public static ScheduledInstance makeScheduledInstance(
      String className, String workflowName, Object instance, String cronExpr) {
    logger.debug("Scheduling wf {}", workflowName);
    Cron cron = cronParser.parse(cronExpr);
    return new ScheduledInstance(className, workflowName, instance, cron);
  }

  private final DBOSExecutor dbosExecutor;
  private volatile boolean stop = false;
  private final Queue schedulerQueue;
  private final String schedulerQueueName;
  private final List<ScheduledInstance> scheduledWorkflows;

  public SchedulerService(
      DBOSExecutor dbosExecutor, Queue schedulerQueue, List<ScheduledInstance> scheduledWorkflows) {
    this.dbosExecutor = Objects.requireNonNull(dbosExecutor);
    this.schedulerQueue = Objects.requireNonNull(schedulerQueue);
    this.schedulerQueueName = this.schedulerQueue.name();
    this.scheduledWorkflows = Objects.requireNonNull(scheduledWorkflows);
  }

  private void startScheduledWorkflows() {

    for (var wf : this.scheduledWorkflows) {

      ExecutionTime executionTime = ExecutionTime.forCron(wf.cron);

      RegisteredWorkflow regWF = dbosExecutor.getWorkflow(wf.className, "", wf.workflowName);
      if (regWF == null) {
        throw new IllegalStateException(
            "Workflow not registered: %s/%s".formatted(wf.className, wf.workflowName));
      }

      Runnable scheduleTask =
          new Runnable() {
            @Override
            public void run() {
              try {
                ZonedDateTime scheduledTime = ZonedDateTime.now(ZoneOffset.UTC);
                Object[] args = new Object[2];
                args[0] = scheduledTime.toInstant();
                args[1] = ZonedDateTime.now(ZoneOffset.UTC).toInstant();
                logger.debug("submitting to dbos Executor {}", wf.workflowName);
                String workflowId =
                    String.format("sched-%s-%s", wf.workflowName, scheduledTime.toString());
                var options =
                    new ExecuteWorkflowOptions(
                        workflowId, null, null, schedulerQueueName, null, OptionalInt.empty());
                dbosExecutor.executeWorkflow(regWF, args, options, null, null);
              } catch (Exception e) {
                e.printStackTrace();
              }

              if (!stop) {
                logger.debug("Scheduling the next execution");
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
                scheduler.schedule(scheduleTask, initialDelayMs, TimeUnit.MILLISECONDS);
              });
    }
  }

  public void stop() {
    stop = true;
    List<Runnable> notRun = scheduler.shutdownNow();
    logger.debug("Shutting down scheduler service. Tasks not run {}", notRun.size());
  }

  public void start() {
    startScheduledWorkflows();
    stop = false;
  }
}
