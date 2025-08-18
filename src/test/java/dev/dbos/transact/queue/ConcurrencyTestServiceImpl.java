package dev.dbos.transact.queue;

import dev.dbos.transact.workflow.Workflow;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConcurrencyTestServiceImpl implements ConcurrencyTestService {

    private static final Logger logger = LoggerFactory.getLogger(ConcurrencyTestServiceImpl.class);

    public CountDownLatch latch = new CountDownLatch(1);
    public List<Semaphore> wfSemaphores = List.of(new Semaphore(0), new Semaphore(0));
    public int counter = 0;

    @Workflow(name = "noopWorkflow")
    public int noopWorkflow(int i) {
        return i;
    }

    @Workflow(name = "blockedWorkflow")
    public int blockedWorkflow(int i) throws InterruptedException {
        logger.info("release {} semaphore", i);
        wfSemaphores.get(i).release();
        counter++;
        latch.await();
        return i;
    }
}
