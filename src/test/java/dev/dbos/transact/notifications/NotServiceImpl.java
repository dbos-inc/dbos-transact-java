package dev.dbos.transact.notifications;

import dev.dbos.transact.context.DBOSContext;
import dev.dbos.transact.workflow.Workflow;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class NotServiceImpl {

  private final ReentrantLock lock = new ReentrantLock();
  private final Condition condition = lock.newCondition();
  private final AtomicInteger counter = new AtomicInteger(0);
  private final CountDownLatch recvReadyLatch = new CountDownLatch(1);

  @Workflow(name = "sendWorkflow")
  public void sendWorkflow(String target, String topic, String msg) {
    try {
      // Wait for recv to signal that it's ready
      recvReadyLatch.await();
      // Now proceed with sending
      DBOSContext.dbosInstance().send(target, msg, topic);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while waiting for recv signal", e);
    }
    // dbos.send(target, msg, topic);
  }

  @Workflow(name = "recvWorkflow")
  public String recvWorkflow(String topic, Duration timeout) {
    recvReadyLatch.countDown();
    String msg = (String) DBOSContext.dbosInstance().recv(topic, timeout);
    return msg;
  }

  @Workflow(name = "recvMultiple")
  public String recvMultiple(String topic) {
    recvReadyLatch.countDown();
    var dbos = DBOSContext.dbosInstance();
    String msg1 = (String) dbos.recv(topic, Duration.ofSeconds(5));
    String msg2 = (String) dbos.recv(topic, Duration.ofSeconds(5));
    String msg3 = (String) dbos.recv(topic, Duration.ofSeconds(5));
    return msg1 + msg2 + msg3;
  }

  @Workflow(name = "concWorkflow")
  public String concWorkflow(String topic) {
    recvReadyLatch.countDown();
    lock.lock();
    try {
      int currentCount = counter.incrementAndGet();
      if (currentCount % 2 == 1) {
        // Wait for the other one to notify
        try {
          condition.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException("Interrupted while waiting", e);
        }
      } else {
        // Notify the other one
        String message = (String) DBOSContext.dbosInstance().recv(topic, Duration.ofSeconds(5));
        condition.signalAll();
        return message;
      }
    } finally {
      lock.unlock();
    }

    String message = (String) DBOSContext.dbosInstance().recv(topic, Duration.ofSeconds(5));
    return message;
  }
}
