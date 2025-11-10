package dev.dbos.transact.scheduled;

import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.workflow.Scheduled;
import dev.dbos.transact.workflow.Workflow;

import java.time.Instant;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

interface SkedService {
  void everySecond(Instant schedule, Instant actual);

  void everyThird(Instant schedule, Instant actual);

  void timed(Instant schedule, Instant actual);

  void withSteps(Instant schedule, Instant actual);

  void everySecondIgnoreMissed(Instant schedule, Instant actual);

  void everySecondDontIgnoreMissed(Instant schedule, Instant actual);
}

class SkedServiceImpl implements SkedService {

  private static final Logger logger = LoggerFactory.getLogger(SkedServiceImpl.class);

  public volatile int everySecondCounter = 0;
  public volatile int everyThirdCounter = 0;
  public volatile Instant scheduled;
  public volatile Instant actual;

  public volatile int everySecondCounterIgnoreMissed = 0;
  public volatile int everySecondCounterDontIgnoreMissed = 0;

  @Override
  @Workflow
  @Scheduled(cron = "0/1 * * * * *")
  public void everySecond(Instant scheduled, Instant actual) {
    assertTrue(DBOS.inWorkflow());
    logger.info("Executing everySecond {} {} {}", everySecondCounter, scheduled, actual);
    ++everySecondCounter;
  }

  @Override
  @Workflow
  @Scheduled(cron = "0/3 * * * * *", queue = "q2")
  public void everyThird(Instant scheduled, Instant actual) {
    logger.info("Executing everyThird {} {} {}", everyThirdCounter, scheduled, actual);
    ++everyThirdCounter;
  }

  @Override
  @Workflow
  @Scheduled(cron = "0/4 * * * * *")
  public void timed(Instant scheduled, Instant actual) {
    logger.info("Executing timed {} {}", scheduled, actual);
    this.scheduled = Objects.requireNonNull(scheduled);
    this.actual = Objects.requireNonNull(actual);
  }

  @Override
  @Workflow
  @Scheduled(cron = "0/4 * * * * *")
  public void withSteps(Instant scheduled, Instant actual) {
    logger.info("Executing withSteps {} {}", scheduled, actual);
    DBOS.runStep(() -> {}, "stepOne");
    DBOS.runStep(() -> {}, "stepTwo");
  }

  @Override
  @Workflow
  @Scheduled(cron = "0/1 * * * * *", ignoreMissed = true)
  public void everySecondIgnoreMissed(Instant scheduled, Instant actual) {
    if (everySecondCounterIgnoreMissed++ == 0) {
      try {
        Thread.sleep(3000);
      } catch (Exception e) {
      }
    }
    logger.info(
        "Executing everySecond ignore missed {} {} {}", everySecondCounter, scheduled, actual);
  }

  @Override
  @Workflow
  @Scheduled(cron = "0/1 * * * * *", ignoreMissed = false)
  public void everySecondDontIgnoreMissed(Instant scheduled, Instant actual) {
    if (everySecondCounterDontIgnoreMissed++ == 0) {
      try {
        Thread.sleep(3000);
      } catch (Exception e) {
      }
    }
    logger.info(
        "Executing everySecond do not ignore missed {} {} {}",
        everySecondCounter,
        scheduled,
        actual);
  }
}
