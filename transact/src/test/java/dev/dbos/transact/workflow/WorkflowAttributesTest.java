package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSClient;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.utils.PgContainer;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for custom workflow attributes (see py-transact PR #720). */
public class WorkflowAttributesTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();

  DBOSConfig dbosConfig;
  @AutoClose DBOS dbos;
  @AutoClose HikariDataSource dataSource;

  static final Queue ATTR_QUEUE = new Queue("attr-queue");

  private AttributesService proxy;

  @BeforeEach
  void beforeEach() {
    dbosConfig = pgContainer.dbosConfig().withAppVersion("v1.0.0");
    dbos = new DBOS(dbosConfig);
    dataSource = pgContainer.dataSource();

    var impl = new AttributesServiceImpl();
    proxy = dbos.registerProxy(AttributesService.class, impl);
    impl.setProxy(proxy);
    dbos.registerQueue(ATTR_QUEUE);

    dbos.launch();
  }

  @Test
  public void directInvocation() {
    var wfid = UUID.randomUUID().toString();
    Map<String, Object> attributes = Map.of("customer", "acme", "tier", 3);

    String childId;
    try (var a = new WorkflowOptions().withAttributes(attributes).setContext()) {
      try (var w = new WorkflowOptions(wfid).setContext()) {
        childId = proxy.parentWorkflow();
      }
    }

    assertEquals(attributes, dbos.listWorkflows(new ListWorkflowsInput(wfid)).get(0).attributes());

    // Child workflows do not inherit their parent's attributes
    assertNull(dbos.listWorkflows(new ListWorkflowsInput(childId)).get(0).attributes());

    // Workflows started outside the block have no attributes
    var wfidNoAttrs = UUID.randomUUID().toString();
    try (var w = new WorkflowOptions(wfidNoAttrs).setContext()) {
      proxy.parentWorkflow();
    }
    assertNull(dbos.listWorkflows(new ListWorkflowsInput(wfidNoAttrs)).get(0).attributes());
  }

  @Test
  public void startWorkflowNestedBlocks() throws Exception {
    // Nested blocks override and restore attributes
    WorkflowHandle<Void, RuntimeException> innerHandle;
    WorkflowHandle<Void, RuntimeException> outerHandle;

    try (var outer =
        new WorkflowOptions().withAttributes(Map.of("region", "us-east-1")).setContext()) {
      try (var inner =
          new WorkflowOptions().withAttributes(Map.of("region", "eu-west-1")).setContext()) {
        innerHandle = dbos.startWorkflow(() -> proxy.noopWorkflow());
      }
      outerHandle = dbos.startWorkflow(() -> proxy.noopWorkflow());
    }

    innerHandle.getResult();
    outerHandle.getResult();
    assertEquals(Map.of("region", "eu-west-1"), innerHandle.getStatus().attributes());
    assertEquals(Map.of("region", "us-east-1"), outerHandle.getStatus().attributes());
  }

  @Test
  public void enqueue() throws Exception {
    Map<String, Object> attributes = Map.of("source", "queue");
    WorkflowHandle<Integer, RuntimeException> handle;
    try (var a = new WorkflowOptions().withAttributes(attributes).setContext()) {
      handle =
          dbos.startWorkflow(() -> proxy.queuedWorkflow(5), new StartWorkflowOptions(ATTR_QUEUE));
    }
    assertEquals(5, handle.getResult());
    assertEquals(attributes, handle.getStatus().attributes());
  }

  @Test
  public void startWorkflowOptions() throws Exception {
    // Attributes set on StartWorkflowOptions are recorded.
    var handle =
        dbos.startWorkflow(
            () -> proxy.noopWorkflow(),
            new StartWorkflowOptions().withAttributes(Map.of("source", "options")));
    handle.getResult();
    assertEquals(Map.of("source", "options"), handle.getStatus().attributes());
  }

  @Test
  public void startWorkflowOptionsOverridesContext() throws Exception {
    // Attributes on StartWorkflowOptions take precedence over the context attributes.
    WorkflowHandle<Void, RuntimeException> handle;
    try (var a = new WorkflowOptions().withAttributes(Map.of("source", "context")).setContext()) {
      handle =
          dbos.startWorkflow(
              () -> proxy.noopWorkflow(),
              new StartWorkflowOptions().withAttributes(Map.of("source", "options")));
    }
    handle.getResult();
    assertEquals(Map.of("source", "options"), handle.getStatus().attributes());
  }

  @Test
  public void fork() throws Exception {
    var wfid = UUID.randomUUID().toString();
    Map<String, Object> attributes = Map.of("customer", "acme");
    try (var a = new WorkflowOptions().withAttributes(attributes).setContext()) {
      try (var w = new WorkflowOptions(wfid).setContext()) {
        proxy.noopWorkflow();
      }
    }

    var forkedHandle = dbos.forkWorkflow(wfid, 1);
    forkedHandle.getResult();
    assertEquals(attributes, forkedHandle.getStatus().attributes());
  }

  @Test
  public void client() throws Exception {
    // Enqueue to a queue nothing consumes; the workflow stays ENQUEUED, which is enough to
    // check the attributes recorded at creation.
    var qs = DBOSTestAccess.getQueueService(dbos);
    qs.pause();

    try (DBOSClient cl = pgContainer.dbosClient()) {
      var options =
          new DBOSClient.EnqueueOptions("client_workflow", ATTR_QUEUE.name())
              .withAttributes(Map.of("source", "client"));
      var handle = cl.enqueueWorkflow(options, new Object[] {1});
      assertEquals(
          Map.of("source", "client"),
          cl.getWorkflowStatus(handle.workflowId()).orElseThrow().attributes());
    } finally {
      qs.unpause();
    }
  }

  @Test
  public void listFilter() throws Exception {
    Map<String, Object> a1 = new LinkedHashMap<>();
    a1.put("customer", "acme");
    a1.put("tier", 1);
    a1.put("beta", true);
    a1.put("note", null);

    Map<String, Object> a2 = new LinkedHashMap<>();
    a2.put("customer", "bigco");
    a2.put("tier", 2);
    a2.put("meta", Map.of("region", "us-east-1"));

    String id1;
    String id2;
    try (var a = new WorkflowOptions().withAttributes(a1).setContext()) {
      var h1 = dbos.startWorkflow(() -> proxy.attrWorkflow());
      id1 = h1.workflowId();
      h1.getResult();
    }
    try (var a = new WorkflowOptions().withAttributes(a2).setContext()) {
      var h2 = dbos.startWorkflow(() -> proxy.attrWorkflow());
      id2 = h2.workflowId();
      h2.getResult();
    }

    // Single key
    assertEquals(Set.of(id1), matchedIds(Map.of("customer", "acme")));
    // Multiple keys AND together
    assertEquals(Set.of(id2), matchedIds(Map.of("customer", "bigco", "tier", 2)));
    // Value mismatch on one key matches nothing
    assertEquals(Set.of(), matchedIds(Map.of("customer", "acme", "tier", 2)));
    // Non-string value types
    assertEquals(Set.of(id1), matchedIds(Map.of("tier", 1)));
    assertEquals(Set.of(id1), matchedIds(Map.of("beta", true)));
    assertEquals(Set.of(id1), matchedIds(singletonNullValue("note")));
    assertEquals(Set.of(id2), matchedIds(Map.of("meta", Map.of("region", "us-east-1"))));
    // Workflows without attributes never match
    assertEquals(Set.of(), matchedIds(Map.of("missing", "key")));
  }

  @Test
  public void invalidAttributesRejected() {
    // A non-JSON-serializable value fails fast when building the options.
    assertThrows(
        IllegalArgumentException.class,
        () -> new WorkflowOptions().withAttributes(Map.of("bad", new Unserializable())));
  }

  /** A value whose serialization fails: Jackson invokes the getter, which throws. */
  static class Unserializable {
    public String getValue() {
      throw new IllegalStateException("not serializable");
    }
  }

  private Set<String> matchedIds(Map<String, Object> attributes) {
    return dbos.listWorkflows(new ListWorkflowsInput().withAttributes(attributes)).stream()
        .map(WorkflowStatus::workflowId)
        .collect(Collectors.toSet());
  }

  private static Map<String, Object> singletonNullValue(String key) {
    var m = new HashMap<String, Object>();
    m.put(key, null);
    return m;
  }
}
