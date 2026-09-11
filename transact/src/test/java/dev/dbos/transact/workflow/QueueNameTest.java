package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;

import org.junit.jupiter.api.Test;

/**
 * {@link QueueName} and the overloads that take one.
 *
 * <p>Container-free. Every overload here delegates positionally to a {@code String} counterpart,
 * where a wrong argument would compile cleanly and quietly bind the name to the wrong field — most
 * sharply on {@link StartWorkflowOptions}, whose {@code String} constructor means a workflow id. So
 * each is compared against the counterpart it delegates to rather than spot-checked.
 */
class QueueNameTest {

  @Test
  void carriesTheNameItWasGiven() {
    var name = QueueName.of("orders");
    assertEquals("orders", name.value());
    assertEquals(QueueName.of("orders"), name);
  }

  @Test
  void printsTheBareName() {
    // Goes into log lines and exception messages, where the record's generated form reads worse.
    assertEquals("orders", QueueName.of("orders").toString());
  }

  @Test
  void refusesAbsentNames() {
    assertThrows(NullPointerException.class, () -> QueueName.of(null));
    assertThrows(IllegalArgumentException.class, () -> QueueName.of(""));
    assertThrows(IllegalArgumentException.class, () -> QueueName.of("   "));
  }

  @Test
  void imposesNoCharacterSetRule() {
    // Queue names are not application names. The only Transact constraint is the reserved
    // _dbos_internal_queue, enforced at registration. A stricter rule here would refuse to address
    // queues a peer SDK created on a shared system database.
    for (var accepted :
        new String[] {
          "UPPER", "with.dots", "with spaces", "ünïcode", "a", "_dbos_internal_queue"
        }) {
      assertEquals(accepted, QueueName.of(accepted).value());
    }
  }

  @Test
  void startWorkflowOptionsConstructorSetsTheQueueNotTheId() {
    // The whole point of the type: the String constructor means a workflow id.
    assertEquals(
        new StartWorkflowOptions().withQueue("orders"),
        new StartWorkflowOptions(QueueName.of("orders")));
  }

  @Test
  void startWorkflowOptionsWithQueueMatchesTheStringForm() {
    assertEquals(
        new StartWorkflowOptions("wf-1").withQueue("orders"),
        new StartWorkflowOptions("wf-1").withQueue(QueueName.of("orders")));
  }

  @Test
  void forkOptionsWithQueueMatchesTheStringForm() {
    assertEquals(
        new ForkOptions().withQueue("orders"), new ForkOptions().withQueue(QueueName.of("orders")));
  }

  @Test
  void dbosConfigWithListenQueueMatchesTheStringForm() {
    var base = DBOSConfig.defaults("queue-name-test");
    assertEquals(base.withListenQueue("orders"), base.withListenQueue(QueueName.of("orders")));
  }

  @Test
  void dbosConfigWithListenQueuesMatchesTheStringForm() {
    var base = DBOSConfig.defaults("queue-name-test");
    assertEquals(
        base.withListenQueues("orders", "shipping"),
        base.withListenQueues(QueueName.of("orders"), QueueName.of("shipping")));
  }

  @Test
  void dbosConfigWithListenQueuesAcceptsNoneAndNull() {
    var base = DBOSConfig.defaults("queue-name-test");
    assertEquals(base.withListenQueues(new String[] {}), base.withListenQueues(new QueueName[] {}));
    assertEquals(base.withListenQueues((String[]) null), base.withListenQueues((QueueName[]) null));
  }
}
