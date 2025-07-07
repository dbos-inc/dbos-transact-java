package dev.dbos.transact.scheduled;

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.workflow.Scheduled;
import dev.dbos.transact.workflow.Workflow;

import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SchedulerService {

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
    private final DBOSExecutor dbosExecutor;
    private final CronParser cronParser;

    public SchedulerService(DBOSExecutor dbosExecutor) {
        this.dbosExecutor = dbosExecutor;
        this.cronParser = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX));
    }

    public void scanAndSchedule(Object bean) {
        for (Method method : bean.getClass().getDeclaredMethods()) {
            if (method.isAnnotationPresent(Workflow.class) && method.isAnnotationPresent(Scheduled.class)) {
                Scheduled scheduled = method.getAnnotation(Scheduled.class);
                //TODO : register with dbosExecutor for recovery
                String cron = scheduled.cron();
                scheduleRecurringWorkflow(bean, method, cron);
            }
        }
    }

    private void scheduleRecurringWorkflow(Object instance, Method method, String cronExpr) {
        Cron cron = cronParser.parse(cronExpr);
        ExecutionTime executionTime = ExecutionTime.forCron(cron);

        Runnable scheduleTask = new Runnable() {
            @Override
            public void run() {
                try {
                    ZonedDateTime scheduledTime = ZonedDateTime.now();
                    dbosExecutor.submitWorkflow(instance, method, scheduledTime, ZonedDateTime.now());
                } catch (Exception e) {
                    e.printStackTrace();
                }

                ZonedDateTime now = ZonedDateTime.now();
                executionTime.nextExecution(now).ifPresent(nextTime -> {
                    long delayMs = Duration.between(now, nextTime).toMillis();
                    scheduler.schedule(this, delayMs, TimeUnit.MILLISECONDS);
                });
            }
        };

        // Kick off the first run (but only scheduled at the next proper time)
        ZonedDateTime now = ZonedDateTime.now();
        executionTime.nextExecution(now).ifPresent(nextTime -> {
            long initialDelayMs = Duration.between(now, nextTime).toMillis();
            scheduler.schedule(scheduleTask, initialDelayMs, TimeUnit.MILLISECONDS);
        });
    }
}
