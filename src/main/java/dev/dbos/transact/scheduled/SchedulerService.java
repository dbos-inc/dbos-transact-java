package dev.dbos.transact.scheduled;

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import dev.dbos.transact.context.SetWorkflowID;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.execution.WorkflowFunctionWrapper;
import dev.dbos.transact.workflow.Scheduled;
import dev.dbos.transact.workflow.Workflow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SchedulerService {

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
    private final DBOSExecutor dbosExecutor;
    private final CronParser cronParser;
    Logger logger = LoggerFactory.getLogger(SchedulerService.class);
    private volatile boolean stop = false ;

    public SchedulerService(DBOSExecutor dbosExecutor) {
        this.dbosExecutor = dbosExecutor;
        this.cronParser = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ));
    }

    public void scanAndSchedule(Object implementation) {
        for (Method method : implementation.getClass().getDeclaredMethods()) {
            if (method.isAnnotationPresent(Workflow.class) && method.isAnnotationPresent(Scheduled.class)) {

                if (!Arrays.equals(
                        method.getParameterTypes(),
                        new Class<?>[]{Instant.class, Instant.class})) {
                    throw new IllegalArgumentException("Scheduled workflow must have parameters (Instant scheduledTime, Instant actualTime)");
                }

                Workflow wfAnnotation = method.getAnnotation(Workflow.class);
                Scheduled scheduled = method.getAnnotation(Scheduled.class);
                String workflowName = wfAnnotation.name().isEmpty() ? method.getName() : wfAnnotation.name();
                // register with dbosExecutor for recovery
                dbosExecutor.registerWorkflow(workflowName, implementation, implementation.getClass().getName(), method);
                String cron = scheduled.cron();
                scheduleRecurringWorkflow(workflowName, implementation, method, cron);
            }
        }
    }

    private void scheduleRecurringWorkflow(String workflowName, Object instance, Method method, String cronExpr) {

        logger.info("Scheduling wf " + workflowName) ;
        Cron cron = cronParser.parse(cronExpr);
        ExecutionTime executionTime = ExecutionTime.forCron(cron);

        WorkflowFunctionWrapper wrapper = dbosExecutor.getWorkflow(workflowName);
        if (wrapper == null) {
            throw new IllegalStateException("Workflow not registered: " + workflowName);
        }

        Runnable scheduleTask = new Runnable() {
            @Override
            public void run() {
                try {
                    ZonedDateTime scheduledTime = ZonedDateTime.now(ZoneOffset.UTC);
                    Object[] args = new Object[2];
                    args[0] = scheduledTime.toInstant();
                    args[1] = ZonedDateTime.now().toInstant();
                    logger.info("submitting to dbos Executor " + workflowName);
                    String workflowId = String.format("sched-%s-%s",workflowName, scheduledTime.toString()) ;
                    try(SetWorkflowID id = new SetWorkflowID(workflowId)) {
                        dbosExecutor.submitWorkflow(workflowName, instance.getClass().getName(), wrapper.target, args, wrapper.function);
                    }
                } catch (Throwable e) {
                    e.printStackTrace();
                }

                if (!stop) {
                    logger.info("Scheduling the next execution") ;
                    ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
                    executionTime.nextExecution(now).ifPresent(nextTime -> {
                        logger.info("Next execution time " + nextTime.toString());
                        long delayMs = Duration.between(now, nextTime).toMillis();
                        scheduler.schedule(this, delayMs, TimeUnit.MILLISECONDS);
                    });
                }
            }
        };

        // Kick off the first run (but only scheduled at the next proper time)
        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
        executionTime.nextExecution(now).ifPresent(nextTime -> {
            long initialDelayMs = Duration.between(now, nextTime).toMillis();
            scheduler.schedule(scheduleTask, initialDelayMs, TimeUnit.MILLISECONDS);
        });
    }

    public void stop () {
        stop = true ;
    }

    public void start () {
        stop = false ;
    }

}
