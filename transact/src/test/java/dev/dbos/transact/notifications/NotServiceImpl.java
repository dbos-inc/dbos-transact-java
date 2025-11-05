package dev.dbos.transact.notifications;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.workflow.Workflow;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class NotServiceImpl implements NotService {

  private final ReentrantLock lock = new ReentrantLock();
  private final Condition condition = lock.newCondition();
  private final AtomicInteger counter = new AtomicInteger(0);
  final CountDownLatch recvReadyLatch = new CountDownLatch(1);

  @Workflow(name = "sendWorkflow")
  public void sendWorkflow(String target, String topic, String msg) {
    try {
      // Wait for recv to signal that it's ready
      recvReadyLatch.await();
      // Now proceed with sending
      DBOS.send(target, msg, topic);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while waiting for recv signal", e);
    }
    // DBOS.send(target, msg, topic);
  }

  @Workflow(name = "recvWorkflow")
  public String recvWorkflow(String topic, Duration timeout) {
    recvReadyLatch.countDown();
    String msg = (String) DBOS.recv(topic, timeout);
    return msg;
  }

  @Workflow(name = "recvMultiple")
  public String recvMultiple(String topic) {
    recvReadyLatch.countDown();
    String msg1 = (String) DBOS.recv(topic, Duration.ofSeconds(5));
    String msg2 = (String) DBOS.recv(topic, Duration.ofSeconds(5));
    String msg3 = (String) DBOS.recv(topic, Duration.ofSeconds(5));
    return msg1 + msg2 + msg3;
  }

  @Workflow(name = "recvCount")
  public int recvCount(String topic) {
    try {
      recvReadyLatch.await();
    } catch (InterruptedException e) {
    }
    String msg1 = (String) DBOS.recv(topic, Duration.ofSeconds(0));
    String msg2 = (String) DBOS.recv(topic, Duration.ofSeconds(0));
    String msg3 = (String) DBOS.recv(topic, Duration.ofSeconds(0));
    int rc = 0;
    if (msg1 != null) ++rc;
    if (msg2 != null) ++rc;
    if (msg3 != null) ++rc;
    return rc;
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
        String message = (String) DBOS.recv(topic, Duration.ofSeconds(5));
        condition.signalAll();
        return message;
      }
    } finally {
      lock.unlock();
    }

    String message = (String) DBOS.recv(topic, Duration.ofSeconds(5));
    return message;
  }

  @Workflow(name = "disallowedSend")
  public String disallowedSendInStep() {
    DBOS.runStep(() -> DBOS.send("a", "b", "c"), "send");
    return "Done";
  }

  @Workflow(name = "disallowedRecv")
  public String disallowedRecvInStep() {
    DBOS.runStep(() -> DBOS.recv("a", Duration.ofSeconds(0)), "recv");
    return "Done";
  }
}
