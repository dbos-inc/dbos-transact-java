package dev.dbos.transact.json;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSClient;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.workflow.Queue;
import dev.dbos.transact.workflow.SerializationStrategy;
import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.WorkflowClassName;
import dev.dbos.transact.workflow.WorkflowHandle;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Cross-language interoperability tests for portable workflow serialization.
 *
 * <p>These tests prove that portable-serialized DB records are identical across Java, Python, and
 * TypeScript by:
 *
 * <ol>
 *   <li>Running a canonical workflow and verifying DB records match golden JSON strings
 *   <li>Replaying from raw SQL inserts (proving cross-language compatibility)
 * </ol>
 *
 * <p>Corresponding tests in other languages:
 *
 * <ul>
 *   <li>Python: dbos-transact-py/tests/test_interop.py
 *   <li>TypeScript: dbos-transact-ts/tests/interop.test.ts
 * </ul>
 */
@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
public class InteropTest {

  private static final ObjectMapper mapper = new ObjectMapper();

  private static DBOSConfig dbosConfig;
  private HikariDataSource dataSource;

  // ============================================================================
  // Golden canonical values (identical across Java, Python, TypeScript tests)
  // ============================================================================

  static final String CANONICAL_TEXT = "hello-interop";
  static final int CANONICAL_NUM = 42;
  static final String CANONICAL_DT = "2025-06-15T10:30:00.000Z";
  static final ArrayList<String> CANONICAL_ITEMS =
      new ArrayList<>(Arrays.asList("alpha", "beta", "gamma"));
  static final Map<String, Object> CANONICAL_META;

  static {
    // Preserve insertion order with LinkedHashMap for consistent JSON output
    CANONICAL_META = new LinkedHashMap<>();
    CANONICAL_META.put("key1", "value1");
    CANONICAL_META.put("key2", 99);
    Map<String, Object> nested = new LinkedHashMap<>();
    nested.put("deep", true);
    CANONICAL_META.put("nested", nested);
  }

  static final boolean CANONICAL_FLAG = true;
  static final Object CANONICAL_EMPTY = null;

  static final Map<String, Object> CANONICAL_MESSAGE;

  static {
    CANONICAL_MESSAGE = new LinkedHashMap<>();
    CANONICAL_MESSAGE.put("sender", "test");
    CANONICAL_MESSAGE.put("payload", Arrays.asList(1, 2, 3));
  }

  // Golden inputs JSON (used for direct-insert tests in ALL languages)
  static final String GOLDEN_INPUTS_JSON =
      "{\"positionalArgs\":[\"hello-interop\",42,\"2025-06-15T10:30:00.000Z\","
          + "[\"alpha\",\"beta\",\"gamma\"],"
          + "{\"key1\":\"value1\",\"key2\":99,\"nested\":{\"deep\":true}},"
          + "true,null]}";

  static final String GOLDEN_MESSAGE_JSON = "{\"sender\":\"test\",\"payload\":[1,2,3]}";

  // Golden output JSON — the exact string each language's portable serializer must produce.
  static final String GOLDEN_OUTPUT_JSON =
      "{\"echo_text\":\"hello-interop\",\"meta_keys\":[\"key1\",\"key2\",\"nested\"],\"flag\":true,\"items_count\":3,\"echo_dt\":\"2025-06-15T10:30:00.000Z\",\"received\":{\"sender\":\"test\",\"payload\":[1,2,3]},\"echo_num\":42,\"empty\":null}";

  // Golden event value JSON
  static final String GOLDEN_EVENT_JSON = "{\"flag\":true,\"num\":42,\"text\":\"hello-interop\"}";

  @BeforeAll
  static void onetimeSetup() throws Exception {
    InteropTest.dbosConfig =
        DBOSConfig.defaultsFromEnv("interoptest")
            .withDatabaseUrl("jdbc:postgresql://localhost:5432/dbos_java_sys");
  }

  @BeforeEach
  void beforeEachTest() throws Exception {
    DBUtils.recreateDB(dbosConfig);
    dataSource = SystemDatabase.createDataSource(dbosConfig);
    DBOS.reinitialize(dbosConfig);
  }

  @AfterEach
  void afterEachTest() throws Exception {
    dataSource.close();
    DBOS.shutdown();
  }

  // ============================================================================
  // Canonical workflow definition
  // ============================================================================

  /** Workflow interface for the canonical interop workflow. */
  public interface InteropService {
    Map<String, Object> canonicalWorkflow(
        String text,
        int num,
        String dt,
        ArrayList<String> items,
        Map<String, Object> meta,
        boolean flag,
        Object empty);
  }

  /** Implementation of the canonical interop workflow. */
  @WorkflowClassName("interop")
  public static class InteropServiceImpl implements InteropService {
    @Workflow(name = "canonicalWorkflow", serializationStrategy = SerializationStrategy.PORTABLE)
    @Override
    public Map<String, Object> canonicalWorkflow(
        String text,
        int num,
        String dt,
        ArrayList<String> items,
        Map<String, Object> meta,
        boolean flag,
        Object empty) {

      // Set event with portable serialization
      Map<String, Object> eventValue = new LinkedHashMap<>();
      eventValue.put("text", text);
      eventValue.put("num", num);
      eventValue.put("flag", flag);
      DBOS.setEvent("interop_status", eventValue);

      // No writeStream in Java (streams not supported)

      // Receive message
      Object msg = DBOS.recv("interop_topic", Duration.ofSeconds(30));

      // Build deterministic result
      Map<String, Object> result = new LinkedHashMap<>();
      result.put("echo_text", text);
      result.put("echo_num", num);
      result.put("echo_dt", dt);
      result.put("items_count", items.size());
      result.put("meta_keys", new ArrayList<>(new TreeSet<>(meta.keySet())));
      result.put("flag", flag);
      result.put("empty", empty);
      result.put("received", msg);
      return result;
    }
  }

  // ============================================================================
  // Helper to verify result structurally
  // ============================================================================

  @SuppressWarnings("unchecked")
  private void assertResultMatchesExpected(Object result) {
    assertNotNull(result);
    assertTrue(result instanceof Map, "Result should be a Map, got: " + result.getClass());
    Map<String, Object> resultMap = (Map<String, Object>) result;

    assertEquals(CANONICAL_TEXT, resultMap.get("echo_text"));
    assertEquals(CANONICAL_NUM, ((Number) resultMap.get("echo_num")).intValue());
    assertEquals(CANONICAL_DT, resultMap.get("echo_dt"));
    assertEquals(3, ((Number) resultMap.get("items_count")).intValue());
    assertEquals(Arrays.asList("key1", "key2", "nested"), resultMap.get("meta_keys"));
    assertEquals(CANONICAL_FLAG, resultMap.get("flag"));
    assertNull(resultMap.get("empty"));

    // Verify received message
    Object received = resultMap.get("received");
    assertNotNull(received);
    assertTrue(received instanceof Map, "received should be a Map");
    Map<String, Object> receivedMap = (Map<String, Object>) received;
    assertEquals("test", receivedMap.get("sender"));
    assertEquals(Arrays.asList(1, 2, 3), receivedMap.get("payload"));
  }

  // ============================================================================
  // Helper to insert workflow_status row
  // ============================================================================

  private void insertPortableWorkflowRow(
      String workflowId, String className, String workflowName, String queueName, String inputsJson)
      throws Exception {
    try (Connection conn = dataSource.getConnection()) {
      String sql =
          """
          INSERT INTO dbos.workflow_status(
            workflow_uuid, name, class_name, config_name,
            queue_name, status, inputs, created_at, serialization
          )
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
          """;
      try (PreparedStatement stmt = conn.prepareStatement(sql)) {
        stmt.setString(1, workflowId);
        stmt.setString(2, workflowName);
        stmt.setString(3, className);
        stmt.setString(4, null);
        stmt.setString(5, queueName);
        stmt.setString(6, "ENQUEUED");
        stmt.setString(7, inputsJson);
        stmt.setLong(8, System.currentTimeMillis());
        stmt.setString(9, "portable_json");
        stmt.executeUpdate();
      }
    }
  }

  private void insertPortableNotification(String destinationUuid, String topic, String messageJson)
      throws Exception {
    try (Connection conn = dataSource.getConnection()) {
      String sql =
          """
          INSERT INTO dbos.notifications(
            destination_uuid, topic, message, serialization
          )
          VALUES (?, ?, ?, ?)
          """;
      try (PreparedStatement stmt = conn.prepareStatement(sql)) {
        stmt.setString(1, destinationUuid);
        stmt.setString(2, topic);
        stmt.setString(3, messageJson);
        stmt.setString(4, "portable_json");
        stmt.executeUpdate();
      }
    }
  }

  // ============================================================================
  // Test: Canonical workflow execution and DB record verification
  // ============================================================================

  @Test
  public void testInteropCanonical() throws Exception {
    Queue testQueue = new Queue("interopq");
    DBOS.registerQueue(testQueue);
    DBOS.registerWorkflows(InteropService.class, new InteropServiceImpl());
    DBOS.launch();

    try (DBOSClient client = new DBOSClient(dataSource)) {
      String workflowId = UUID.randomUUID().toString();

      // Enqueue the canonical workflow with portable serialization
      var options =
          new DBOSClient.EnqueueOptions("interop", "canonicalWorkflow", "interopq")
              .withWorkflowId(workflowId)
              .withSerialization(SerializationStrategy.PORTABLE);

      WorkflowHandle<String, ?> handle =
          client.enqueuePortableWorkflow(
              options,
              new Object[] {
                CANONICAL_TEXT,
                CANONICAL_NUM,
                CANONICAL_DT,
                CANONICAL_ITEMS,
                CANONICAL_META,
                CANONICAL_FLAG,
                CANONICAL_EMPTY,
              },
              null);

      // Send the canonical message with portable serialization
      client.send(
          workflowId, CANONICAL_MESSAGE, "interop_topic", null, DBOSClient.SendOptions.portable());

      // Wait for result and verify
      Object result = handle.getResult();
      assertResultMatchesExpected(result);

      // ---- Verify DB records ----

      // workflow_status
      var wsRow = DBUtils.getWorkflowRow(dataSource, workflowId);
      assertNotNull(wsRow);
      assertEquals("portable_json", wsRow.serialization());
      assertEquals("SUCCESS", wsRow.status());
      assertEquals("canonicalWorkflow", wsRow.name());
      assertEquals("interop", wsRow.className());

      // Parse and verify inputs
      assertEquals(GOLDEN_INPUTS_JSON, wsRow.inputs().replace(",\"namedArgs\":null", ""));

      // Output: exact string comparison
      assertEquals(GOLDEN_OUTPUT_JSON, wsRow.output());

      // workflow_events: exact string comparison
      var events = DBUtils.getWorkflowEvents(dataSource, workflowId);
      var interopEvent = events.stream().filter(e -> e.key().equals("interop_status")).findFirst();
      assertTrue(interopEvent.isPresent());
      assertEquals("portable_json", interopEvent.get().serialization());
      assertEquals(GOLDEN_EVENT_JSON, interopEvent.get().value());

      // workflow_events_history: exact string comparison
      var eventHistory = DBUtils.getWorkflowEventHistory(dataSource, workflowId);
      var interopEventHist =
          eventHistory.stream().filter(e -> e.key().equals("interop_status")).findFirst();
      assertTrue(interopEventHist.isPresent());
      assertEquals("portable_json", interopEventHist.get().serialization());
      assertEquals(GOLDEN_EVENT_JSON, interopEventHist.get().value());

      // No stream assertions — Java doesn't have streams
    }
  }

  // ============================================================================
  // Test: Direct-insert replay (proves cross-language compatibility)
  // ============================================================================

  @Test
  public void testInteropDirectInsert() throws Exception {
    Queue testQueue = new Queue("interopq");
    DBOS.registerQueue(testQueue);
    DBOS.registerWorkflows(InteropService.class, new InteropServiceImpl());
    DBOS.launch();

    String workflowId = UUID.randomUUID().toString();

    // Insert golden workflow_status
    insertPortableWorkflowRow(
        workflowId, "interop", "canonicalWorkflow", "interopq", GOLDEN_INPUTS_JSON);

    // Insert golden notification
    insertPortableNotification(workflowId, "interop_topic", GOLDEN_MESSAGE_JSON);

    // Retrieve and verify the workflow executes correctly
    WorkflowHandle<String, ?> handle = DBOS.retrieveWorkflow(workflowId);
    Object result = handle.getResult();
    assertResultMatchesExpected(result);

    // Verify it completed with portable_json serialization
    var wsRow = DBUtils.getWorkflowRow(dataSource, workflowId);
    assertNotNull(wsRow);
    assertEquals("portable_json", wsRow.serialization());
    assertEquals("SUCCESS", wsRow.status());
  }

  // ============================================================================
  // Test: Client enqueue with namedArgs (for kwargs interop with Python)
  // ============================================================================

  /** Workflow interface for named-args testing. */
  public interface NamedArgsService {
    String namedArgsWorkflow(String name, int count, ArrayList<String> tags);
  }

  @WorkflowClassName("interop")
  public static class NamedArgsServiceImpl implements NamedArgsService {
    @Workflow(name = "namedArgsWorkflow", serializationStrategy = SerializationStrategy.PORTABLE)
    @Override
    public String namedArgsWorkflow(String name, int count, ArrayList<String> tags) {
      return name + "-" + count + "-" + String.join(",", tags);
    }
  }

  @Test
  public void testInteropNamedArgs() throws Exception {
    Queue testQueue = new Queue("interopq");
    DBOS.registerQueue(testQueue);
    DBOS.registerWorkflows(NamedArgsService.class, new NamedArgsServiceImpl());
    DBOS.launch();

    try (DBOSClient client = new DBOSClient(dataSource)) {
      String workflowId = UUID.randomUUID().toString();

      // Enqueue with namedArgs (this is what Python kwargs produce)
      Map<String, Object> namedArgs = new LinkedHashMap<>();
      namedArgs.put("name", "test");
      namedArgs.put("count", 42);
      namedArgs.put("tags", Arrays.asList("a", "b"));

      var options =
          new DBOSClient.EnqueueOptions("interop", "namedArgsWorkflow", "interopq")
              .withWorkflowId(workflowId);

      client.<String>enqueuePortableWorkflow(options, new Object[] {}, namedArgs);

      // Java doesn't use named args but can pass them to Python.
      // For this test, let's verify the stored format matches what Python produces.

      // Verify the stored inputs JSON
      var wsRow = DBUtils.getWorkflowRow(dataSource, workflowId);
      assertNotNull(wsRow);
      assertEquals("portable_json", wsRow.serialization());

      @SuppressWarnings("unchecked")
      Map<String, Object> storedInputs = mapper.readValue(wsRow.inputs(), Map.class);
      assertNotNull(storedInputs.get("positionalArgs"));
      assertNotNull(storedInputs.get("namedArgs"));

      @SuppressWarnings("unchecked")
      List<Object> positionalArgs = (List<Object>) storedInputs.get("positionalArgs");
      assertEquals(0, positionalArgs.size());

      @SuppressWarnings("unchecked")
      Map<String, Object> storedNamedArgs = (Map<String, Object>) storedInputs.get("namedArgs");
      assertEquals("test", storedNamedArgs.get("name"));
      assertEquals(42, ((Number) storedNamedArgs.get("count")).intValue());
      assertEquals(Arrays.asList("a", "b"), storedNamedArgs.get("tags"));
    }
  }
}
