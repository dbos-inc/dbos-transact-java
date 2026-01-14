package dev.dbos.transact.queue;

import dev.dbos.transact.workflow.Workflow;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConcurrencyTestServiceImpl implements ConcurrencyTestService {

  private static final Logger logger = LoggerFactory.getLogger(ConcurrencyTestServiceImpl.class);

  public CountDownLatch latch = new CountDownLatch(1);
  public Semaphore wfSemaphore = new Semaphore(0);
  public AtomicInteger counter = new AtomicInteger(0);

  @Workflow(name = "noopWorkflow")
  public int noopWorkflow(int i) {
    return i;
  }

  @Workflow(name = "blockedWorkflow")
  public int blockedWorkflow(int i) throws InterruptedException {
    counter.incrementAndGet();
    logger.info("release {} semaphore", i);
    wfSemaphore.release();
    latch.await();
    return i;
  }
}
