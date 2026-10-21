package dev.dbos.transact.notifications;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSClient;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.WorkflowState;
import dev.dbos.transact.workflow.WorkflowStatus;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

interface InOrderReceiver {
  void receive(String topic, int count);
}

class InOrderReceiverImpl implements InOrderReceiver {
  private final DBOS dbos;

  InOrderReceiverImpl(DBOS dbos) {
    this.dbos = dbos;
  }

  /** Receives 0..count-1 on the topic, in order, and fails on anything else. */
  @Override
  @Workflow
  public void receive(String topic, int count) {
    for (int i = 0; i < count; i++) {
      var value = dbos.<String>recv(topic, Duration.ofSeconds(60)).orElse(null);
      if (!String.valueOf(i).equals(value)) {
        throw new IllegalStateException("Unexpected message %s, expected %d".formatted(value, i));
      }
    }
  }
}

/**
 * A receiver that is recovered onto a new executor again and again, while messages keep arriving,
 * gets every message exactly once and in order. On DBOS Cloud, Conductor recovers a dead executor's
 * workflows through recoverPendingWorkflows, as this test calls it.
 */
public class RecvAcrossRecoveryTest {
  private static final String TOPIC = "in-order";
  private static final int MESSAGES = 500;

  @AutoClose final PgContainer pgContainer = new PgContainer();
  @AutoClose DBOSClient client;
  private final List<DBOS> executors = new ArrayList<>();

  @AfterEach
  void closeExecutors() {
    for (var dbos : executors) {
      dbos.close();
    }
  }

  private record Launched(DBOS dbos, InOrderReceiver proxy, String executorId) {}

  private void close(Launched launched) {
    launched.dbos().close();
    executors.remove(launched.dbos());
  }

  private Launched launch(String executorId) {
    // Every executor reports the same application version, so each can recover the others.
    var dbos =
        new DBOS(
            pgContainer.dbosConfig().withExecutorId(executorId).withAppVersion("recv-recovery"));
    var proxy = dbos.registerProxy(InOrderReceiver.class, new InOrderReceiverImpl(dbos));
    dbos.launch();
    executors.add(dbos);
    return new Launched(dbos, proxy, executorId);
  }

  /**
   * Waits for a new execution to claim the receiver after a hand-off (its start time changes) and
   * receive at least one more message, while the sender is still sending. A recovered workflow
   * waits for a queue poll, about a second, before anything runs it again.
   */
  private boolean resumedMidStream(
      String workflowId, Instant startedBefore, int stepsBefore, CompletableFuture<Void> sender)
      throws InterruptedException {
    long giveUp = System.nanoTime() + Duration.ofSeconds(15).toNanos();
    while (!sender.isDone()) {
      var status = client.getWorkflowStatus(workflowId).orElseThrow();
      if (status.status() == WorkflowState.PENDING
          && !Objects.equals(status.startedAt(), startedBefore)
          && client.listWorkflowSteps(workflowId).size() > stepsBefore) {
        return true;
      }
      assertTrue(System.nanoTime() < giveUp, "the receiver was not resumed after a hand-off");
      Thread.sleep(50);
    }
    return false;
  }

  private WorkflowStatus awaitFinished(String workflowId) throws InterruptedException {
    long giveUp = System.nanoTime() + Duration.ofSeconds(30).toNanos();
    while (true) {
      var status = client.getWorkflowStatus(workflowId).orElseThrow();
      if (status.status() != WorkflowState.PENDING && status.status() != WorkflowState.ENQUEUED) {
        return status;
      }
      assertTrue(System.nanoTime() < giveUp, "the receiver did not finish");
      Thread.sleep(50);
    }
  }

  /**
   * A send retried after its first attempt went through, as a caller does when the response is
   * lost, delivers the message twice unless both attempts carry the same idempotency key.
   */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void aRetriedSendIsDeliveredTwiceWithoutAnIdempotencyKey(boolean withKey) throws Exception {
    var launched = launch("executor-0");
    client = pgContainer.dbosClient();
    var workflowId = UUID.randomUUID().toString();
    launched
        .dbos()
        .startWorkflow(
            () -> launched.proxy().receive(TOPIC, 2), new StartWorkflowOptions(workflowId));

    var key = withKey ? workflowId + "-0" : null;
    client.send(workflowId, "0", TOPIC, key);
    client.send(workflowId, "0", TOPIC, key);
    client.send(workflowId, "1", TOPIC, withKey ? workflowId + "-1" : null);

    var status = awaitFinished(workflowId);
    if (withKey) {
      assertEquals(WorkflowState.SUCCESS, status.status());
    } else {
      assertEquals(WorkflowState.ERROR, status.status());
      assertEquals("Unexpected message 0, expected 1", status.error().message());
    }
  }

  /**
   * {@code close}: each executor shuts down before the next recovers its workflows, as a redeploy
   * does, so some messages arrive while nothing runs the receiver. {@code abandon}: each executor
   * is left running when the next recovers its workflows, so its execution of the receiver goes on
   * until it is refused, as on a host that was presumed dead.
   */
  @ParameterizedTest
  @ValueSource(strings = {"close", "abandon"})
  void aReceiverRecoveredRepeatedlyGetsEveryMessageInOrder(String handOff) throws Exception {
    var current = launch("executor-0");
    client = pgContainer.dbosClient();
    var workflowId = UUID.randomUUID().toString();
    current
        .dbos()
        .startWorkflow(
            () -> current.proxy().receive(TOPIC, MESSAGES), new StartWorkflowOptions(workflowId));

    // Sent without idempotency keys, from outside any executor, about one every 20 ms.
    var sender =
        CompletableFuture.runAsync(
            () -> {
              for (int i = 0; i < MESSAGES; i++) {
                client.send(workflowId, String.valueOf(i), TOPIC, null);
                try {
                  Thread.sleep(20);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  return;
                }
              }
            });

    int handOffs = 0;
    int resumptions = 0;
    var previous = current;
    Launched abandoned = null;
    var replaced = new ArrayList<String>();
    while (!sender.isDone()) {
      var before = client.getWorkflowStatus(workflowId).orElseThrow();
      int stepsBefore = client.listWorkflowSteps(workflowId).size();
      if (handOff.equals("close")) {
        close(previous);
      }
      var next = launch("executor-" + (++handOffs));
      // Every earlier executor counts as dead, as Cloud's sweep of unknown executors treats them:
      // an abandoned one may have claimed the workflow from the queue before it was closed.
      replaced.add(previous.executorId());
      DBOSTestAccess.getDbosExecutor(next.dbos()).recoverPendingWorkflows(List.copyOf(replaced));
      if (handOff.equals("abandon")) {
        // Each abandoned executor runs on through the next hand-off; closing it after that keeps
        // the database's connections in bounds.
        if (abandoned != null) {
          close(abandoned);
        }
        abandoned = previous;
      }
      previous = next;
      if (resumedMidStream(workflowId, before.startedAt(), stepsBefore, sender)) {
        resumptions++;
      }
    }
    sender.get();
    assertTrue(
        resumptions >= 5,
        "only %d of %d hand-offs resumed the receiver while messages were arriving"
            .formatted(resumptions, handOffs));

    // The receiver may still be working through a backlog when the sending ends, slowly on
    // CockroachDB, so wait as long as it keeps receiving, and give up once it stalls.
    WorkflowStatus status = null;
    int steps = -1;
    long giveUp = 0;
    var last = DBOSTestAccess.getDbosExecutor(previous.dbos());
    while (true) {
      int now = client.listWorkflowSteps(workflowId).size();
      if (now != steps) {
        steps = now;
        giveUp = System.nanoTime() + Duration.ofSeconds(30).toNanos();
      } else if (System.nanoTime() > giveUp) {
        break;
      }
      // An abandoned executor closed after the last hand-off may have held the workflow.
      last.recoverPendingWorkflows(List.copyOf(replaced));
      status = client.getWorkflowStatus(workflowId).orElseThrow();
      // ENQUEUED right after a recovery, PENDING while an executor runs it.
      if (status.status() != WorkflowState.PENDING && status.status() != WorkflowState.ENQUEUED) {
        break;
      }
      Thread.sleep(100);
    }
    var finalStatus = status;
    var finalResumptions = resumptions;
    var finalSteps = steps;
    assertEquals(
        WorkflowState.SUCCESS,
        finalStatus.status(),
        () ->
            "after %d resumptions and %d steps: %s"
                .formatted(
                    finalResumptions,
                    finalSteps,
                    finalStatus.error() == null ? "no error" : finalStatus.error().message()));
  }
}
