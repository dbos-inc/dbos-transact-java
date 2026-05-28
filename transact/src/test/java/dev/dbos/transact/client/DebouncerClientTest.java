package dev.dbos.transact.client;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSClient;
import dev.dbos.transact.DebouncerClient;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.Queue;
import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.WorkflowState;

import java.time.Duration;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

interface ClientTargetService {
  String process(String input);
}

class ClientTargetServiceImpl implements ClientTargetService {
  final AtomicInteger callCount = new AtomicInteger();
  final ConcurrentLinkedQueue<String> callArgs = new ConcurrentLinkedQueue<>();

  @Override
  @Workflow
  public String process(String input) {
    callCount.incrementAndGet();
    callArgs.add(input);
    return "result:" + input;
  }
}

public class DebouncerClientTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();

  DBOSConfig dbosConfig;
  @AutoClose DBOS dbos;
  @AutoClose HikariDataSource dataSource;
  @AutoClose DBOSClient dbosClient;

  static final Queue USER_QUEUE = new Queue("client-user-queue");

  ClientTargetServiceImpl serviceImpl;

  @BeforeEach
  void beforeEach() {
    dbosConfig = pgContainer.dbosConfig();
    dbos = new DBOS(dbosConfig);
    dataSource = pgContainer.dataSource();

    serviceImpl = new ClientTargetServiceImpl();
    dbos.registerProxy(ClientTargetService.class, serviceImpl);
    dbos.registerQueue(USER_QUEUE);
    dbos.launch();

    dbosClient =
        new DBOSClient(pgContainer.jdbcUrl(), pgContainer.username(), pgContainer.password());
  }

  private DebouncerClient<String> debouncer() {
    return dbosClient
        .<String>debouncer("process")
        .withClassName(ClientTargetServiceImpl.class.getName());
  }

  @Test
  void singleCallFiresOnce() throws Exception {
    var handle = debouncer().debounce("key-1", Duration.ofMillis(500), "hello");
    assertEquals("result:hello", handle.getResult());
    assertEquals(1, serviceImpl.callCount.get());
  }

  @Test
  void multipleCallsCoalesceToLatestArgs() throws Exception {
    var d = debouncer();
    // Use a long period (3s) so the window cannot close between the three calls even on slow CI.
    var h1 = d.debounce("key-2", Duration.ofSeconds(3), "v1");
    Thread.sleep(100);
    var h2 = d.debounce("key-2", Duration.ofSeconds(3), "v2");
    Thread.sleep(100);
    var h3 = d.debounce("key-2", Duration.ofSeconds(3), "v3");

    String result = h3.getResult();
    assertEquals("result:v3", result);
    assertEquals(h1.workflowId(), h2.workflowId());
    assertEquals(h2.workflowId(), h3.workflowId());
    assertEquals(1, serviceImpl.callCount.get());
  }

  @Test
  void differentKeysFireIndependently() throws Exception {
    var d = debouncer();
    var hA = d.debounce("key-A", Duration.ofMillis(400), "A");
    var hB = d.debounce("key-B", Duration.ofMillis(400), "B");

    assertNotEquals(hA.workflowId(), hB.workflowId());
    assertEquals("result:A", hA.getResult());
    assertEquals("result:B", hB.getResult());
    assertEquals(2, serviceImpl.callCount.get());
  }

  @Test
  void reDebounceAfterWindowCloses() throws Exception {
    var d = debouncer();

    var h1 = d.debounce("key-r", Duration.ofMillis(300), "first");
    assertEquals("result:first", h1.getResult());
    assertEquals(1, serviceImpl.callCount.get());

    Thread.sleep(200);

    var h2 = d.debounce("key-r", Duration.ofMillis(300), "second");
    assertEquals("result:second", h2.getResult());
    assertEquals(2, serviceImpl.callCount.get());

    assertNotEquals(h1.workflowId(), h2.workflowId());
  }

  @Test
  void debouncerClientWithQueue() throws Exception {
    var handle =
        debouncer().withQueue(USER_QUEUE).debounce("key-q", Duration.ofMillis(400), "queued");

    assertEquals("result:queued", handle.getResult());

    var status = dbosClient.getWorkflowStatus(handle.workflowId()).orElseThrow();
    assertEquals(WorkflowState.SUCCESS, status.status());
    assertEquals(USER_QUEUE.name(), status.queueName());
    assertEquals(1, serviceImpl.callCount.get());
  }
}
