package dev.dbos.transact.scheduled;

import java.time.Instant;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.workflow.Scheduled;
import dev.dbos.transact.workflow.Workflow;

public class SkedServiceImpl implements SkedService {

    private static final Logger logger = LoggerFactory.getLogger(SkedServiceImpl.class);

    public volatile int everySecondCounter = 0;
    public volatile int everyThirdCounter = 0;
    public volatile Instant scheduled;
    public volatile Instant actual;

    @Override
    @Workflow
    @Scheduled(cron = "0 * * * * ?")
    public void everyMinute(Instant scheduled, Instant actual) {
        logger.info("Executing everyMinute {} {}", scheduled, actual);
    }

    @Override
    @Workflow
    @Scheduled(cron = "0/1 * * * * ?")
    public void everySecond(Instant scheduled, Instant actual) {
        logger.info("Executing everySecond {} {} {}", everySecondCounter, scheduled, actual);
        ++everySecondCounter;
    }

    @Override
    @Workflow
    @Scheduled(cron = "0/3 * * * * ?")
    public void everyThird(Instant scheduled, Instant actual) {
        logger.info("Executing everyThird {} {} {}", everyThirdCounter, scheduled, actual);
        ++everyThirdCounter;
    }

    @Override
    @Workflow
    @Scheduled(cron = "0/4 * * * * ?")
    public void timed(Instant scheduled, Instant actual) {
        logger.info("Executing timed {} {}", scheduled, actual);
        this.scheduled = Objects.requireNonNull(scheduled);
        this.actual = Objects.requireNonNull(actual);
    }

    @Override
    @Workflow
    @Scheduled(cron = "0/4 * * * * ?")
    public void withSteps(Instant scheduled, Instant actual) {
        logger.info("Executing withSteps {} {}", scheduled, actual);
        DBOS.runStep(() -> {}, "stepOne");
        DBOS.runStep(() -> {}, "stepTwo");
    }
}
