package dev.dbos.transact.json;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSClient;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.ExportedWorkflow;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.Queue;
import dev.dbos.transact.workflow.SerializationStrategy;
import dev.dbos.transact.workflow.StepInfo;
import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.WorkflowClassName;
import dev.dbos.transact.workflow.WorkflowHandle;
import dev.dbos.transact.workflow.WorkflowState;
import dev.dbos.transact.workflow.internal.StepResult;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.*;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

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
public class InteropTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();

  private static final JsonMapper mapper = new JsonMapper();

  private DBOSConfig dbosConfig;
  @AutoClose private DBOS dbos;
  @AutoClose private HikariDataSource dataSource;

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

  @BeforeEach
  void setup() {
    this.dbosConfig = pgContainer.dbosConfig();
    this.dbos = new DBOS(dbosConfig);
    this.dataSource = pgContainer.dataSource();
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
    private final DBOS dbos;

    public InteropServiceImpl(DBOS dbos) {
      this.dbos = dbos;
    }

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
      dbos.setEvent("interop_status", eventValue);

      // No writeStream in Java (streams not supported)

      // Receive message
      var msg =
          dbos.<Map<String, Object>>recv("interop_topic", Duration.ofSeconds(30)).orElseThrow();

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

  /**
   * Insert a completed workflow row as another SDK would leave it.
   *
   * <p>{@code serialization} and {@code error} are what vary between them: Go stores the error as a
   * non-nullable string and so writes "" for a workflow that succeeded, and every SDK writes its
   * own native format unless the workflow was declared portable.
   */
  private void insertPeerWorkflowRow(
      String workflowId, String serialization, String inputs, String output, String error)
      throws Exception {
    try (Connection conn = dataSource.getConnection()) {
      String sql =
          """
          INSERT INTO dbos.workflow_status(
            workflow_uuid, name, class_name, config_name,
            status, inputs, output, error, created_at, serialization, application_name
          )
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          """;
      try (PreparedStatement stmt = conn.prepareStatement(sql)) {
        stmt.setString(1, workflowId);
        stmt.setString(2, "echoWorkflow");
        stmt.setString(3, "interop");
        stmt.setString(4, null);
        stmt.setString(5, "SUCCESS");
        stmt.setString(6, inputs);
        stmt.setString(7, output);
        stmt.setString(8, error);
        stmt.setLong(9, System.currentTimeMillis());
        stmt.setString(10, serialization);
        stmt.setString(11, "interop-peer");
        stmt.executeUpdate();
      }
    }
  }

  /** Insert a completed step row as another SDK would leave it. */
  private void insertPeerStepRow(
      String workflowId, String serialization, String output, String error) throws Exception {
    try (Connection conn = dataSource.getConnection()) {
      String sql =
          """
          INSERT INTO dbos.operation_outputs(
            workflow_uuid, function_id, function_name, output, error, serialization, application_name
          )
          VALUES (?, ?, ?, ?, ?, ?, ?)
          """;
      try (PreparedStatement stmt = conn.prepareStatement(sql)) {
        stmt.setString(1, workflowId);
        stmt.setInt(2, 0);
        stmt.setString(3, "echoStep");
        stmt.setString(4, output);
        stmt.setString(5, error);
        stmt.setString(6, serialization);
        stmt.setString(7, "interop-peer");
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
    dbos.registerQueue(testQueue);
    dbos.registerProxy(InteropService.class, new InteropServiceImpl(dbos));
    dbos.launch();

    try (DBOSClient client = new DBOSClient(dataSource)) {
      String workflowId = UUID.randomUUID().toString();

      // Enqueue the canonical workflow with portable serialization
      var options =
          new DBOSClient.EnqueueOptions("canonicalWorkflow", "interop", "interopq")
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
      assertEquals(WorkflowState.SUCCESS.name(), wsRow.status());
      assertEquals("canonicalWorkflow", wsRow.workflowName());
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
    dbos.registerQueue(testQueue);
    dbos.registerProxy(InteropService.class, new InteropServiceImpl(dbos));
    dbos.launch();

    String workflowId = UUID.randomUUID().toString();

    // Insert golden workflow_status
    insertPortableWorkflowRow(
        workflowId, "interop", "canonicalWorkflow", "interopq", GOLDEN_INPUTS_JSON);

    // Insert golden notification
    insertPortableNotification(workflowId, "interop_topic", GOLDEN_MESSAGE_JSON);

    // Retrieve and verify the workflow executes correctly
    WorkflowHandle<Map<String, Object>, ?> handle = dbos.retrieveWorkflow(workflowId);
    Object result = handle.getResult();
    assertResultMatchesExpected(result);

    // Verify it completed with portable_json serialization
    var wsRow = DBUtils.getWorkflowRow(dataSource, workflowId);
    assertNotNull(wsRow);
    assertEquals("portable_json", wsRow.serialization());
    assertEquals(WorkflowState.SUCCESS.name(), wsRow.status());
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
    dbos.registerQueue(testQueue);
    dbos.registerProxy(NamedArgsService.class, new NamedArgsServiceImpl());
    dbos.launch();

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

  // ============================================================================
  // Test: reading a workflow another application owns
  // ============================================================================

  /**
   * A workflow that succeeded, whose error column holds "" rather than NULL.
   *
   * <p>That is what the Go SDK leaves behind — it stores the error as a non-nullable string — and
   * on a shared system database this runtime is routinely asked about such a row. An empty column
   * has to mean the same thing as an absent one; parsing it as JSON does not.
   */
  @Test
  public void testStatusReadTreatsAnEmptyErrorColumnAsNoError() throws Exception {
    String workflowId = "peer-empty-error";
    insertPeerWorkflowRow(
        workflowId, "portable_json", "{\"positionalArgs\":[]}", "{\"ok\":true}", "");

    dbos.launch();
    var status = dbos.getWorkflowStatus(workflowId).orElseThrow();

    assertEquals(WorkflowState.SUCCESS, status.status());
    assertNull(status.error(), "an empty error column is not an error");
  }

  /**
   * A workflow another application ran, in a serialization format this runtime cannot read.
   *
   * <p>Workflow IDs address the whole system database, so a status read reaches a peer's rows on
   * purpose — and what is wanted there is the metadata: who owns it, what it is, whether it
   * finished. A payload in the peer's own format must not take that away, so the fields that cannot
   * be deserialized come back null and the read succeeds. Python behaves the same way, in
   * safe_deserialize.
   */
  @Test
  public void testStatusReadOfAPeerRowInAnUnreadableFormat() throws Exception {
    String workflowId = "peer-foreign-format";
    insertPeerWorkflowRow(workflowId, "py_pickle", "gASVCgAAAA==", "gASVBAAAAA==", null);

    dbos.launch();
    var status = dbos.getWorkflowStatus(workflowId).orElseThrow();

    // The metadata is the point, and it is all there.
    assertEquals(workflowId, status.workflowId());
    assertEquals("echoWorkflow", status.workflowName());
    assertEquals(WorkflowState.SUCCESS, status.status());
    assertEquals("interop-peer", status.applicationName());
    assertEquals("py_pickle", status.serialization());

    // The payloads are not, and saying so beats failing the whole read.
    assertNull(status.input());
    assertNull(status.output());
    assertNull(status.error());
  }

  /** The same row, through the listing rather than by ID. */
  @Test
  public void testListingAPeerRowInAnUnreadableFormat() throws Exception {
    String workflowId = "peer-foreign-format-listed";
    insertPeerWorkflowRow(workflowId, "py_pickle", "gASVCgAAAA==", "gASVBAAAAA==", "");

    dbos.launch();
    var listed = dbos.listWorkflows(new ListWorkflowsInput().withWorkflowIds(workflowId)).get(0);

    assertEquals(workflowId, listed.workflowId());
    assertEquals("interop-peer", listed.applicationName());
    assertNull(listed.input());
    assertNull(listed.output());
    assertNull(listed.error());
  }

  /**
   * A step of a peer's workflow, recorded with an empty error column.
   *
   * <p>Steps carry the same column with the same quirk, so they get the same reading.
   */
  @Test
  public void testStepReadTreatsAnEmptyErrorColumnAsNoError() throws Exception {
    String workflowId = "peer-step-empty-error";
    insertPeerWorkflowRow(workflowId, "portable_json", "{\"positionalArgs\":[]}", "{}", "");
    insertPeerStepRow(workflowId, "portable_json", "{\"ok\":true}", "");

    dbos.launch();
    var steps = dbos.listWorkflowSteps(workflowId);

    assertEquals(1, steps.size());
    assertNull(steps.get(0).error(), "an empty error column is not an error");
  }

  /**
   * The same gap without another language in it: two Java applications on one system database, one
   * configured with a custom serializer and one not.
   *
   * <p>`custom_base64` is a format this runtime has no deserializer for, exactly as `py_pickle` is,
   * and canDeserialize says so without having to try and fail.
   */
  @Test
  public void testStatusReadOfARowWrittenByACustomSerializerWeLack() throws Exception {
    String workflowId = "peer-custom-serializer";
    insertPeerWorkflowRow(workflowId, "custom_base64", "cG9zaXRpb25hbA==", "b3V0cHV0", null);

    dbos.launch();
    var status = dbos.getWorkflowStatus(workflowId).orElseThrow();

    assertEquals(WorkflowState.SUCCESS, status.status());
    assertEquals("custom_base64", status.serialization());
    assertNull(status.input());
    assertNull(status.output());
  }

  /**
   * Exporting a workflow this runtime cannot read is refused rather than silently emptied.
   *
   * <p>A status read reports an unreadable payload as null, which is right when the metadata is
   * what was asked for. An export is imported back, so the same null would restore a workflow that
   * never had an input.
   */
  @Test
  public void testExportRefusesAWorkflowItCannotRead() throws Exception {
    String workflowId = "peer-export";
    insertPeerWorkflowRow(workflowId, "py_pickle", "gASVCgAAAA==", "gASVBAAAAA==", null);

    dbos.launch();
    var systemDatabase = DBOSTestAccess.getSystemDatabase(dbos);

    var thrown =
        assertThrows(
            IllegalStateException.class, () -> systemDatabase.exportWorkflow(workflowId, false));
    assertTrue(
        thrown.getMessage().contains("py_pickle"),
        "The refusal should name the format it could not read, got: " + thrown.getMessage());
  }

  /**
   * Importing one is refused too, before anything is written.
   *
   * <p>Export and import are a single-SDK affair, but not a single-configuration one: the source
   * application may have had a serializer this one does not. Import re-serializes the payloads, so
   * it needs that serializer as much as export did — and a payload the export already carried as
   * null would otherwise be written as NULL and committed.
   */
  @Test
  public void testImportRefusesAWorkflowItCannotRead() throws Exception {
    String workflowId = "peer-import";
    insertPeerWorkflowRow(workflowId, "portable_json", "{\"positionalArgs\":[]}", "{}", null);

    dbos.launch();
    var systemDatabase = DBOSTestAccess.getSystemDatabase(dbos);

    // Exported readably, then given a step in a format this application has no serializer for —
    // which is what a batch arriving from an application configured differently looks like.
    var exported = systemDatabase.exportWorkflow(workflowId, false).get(0);
    var foreignStep =
        new StepInfo(
            0, "echoStep", "cG9zaXRpb25hbA==", null, null, null, null, "custom_base64", null);
    var batch =
        List.of(
            new ExportedWorkflow(
                exported.status(),
                List.of(foreignStep),
                exported.events(),
                exported.eventHistory(),
                exported.streams()));

    var thrown =
        assertThrows(IllegalStateException.class, () -> systemDatabase.importWorkflow(batch));
    assertTrue(
        thrown.getMessage().contains("custom_base64"),
        "The refusal should name the format, got: " + thrown.getMessage());
  }

  /**
   * Replaying a step whose result this application cannot read fails; it does not replay as null.
   *
   * <p>The tolerant reads are the ones that assemble a record. A recorded step result is consumed
   * to continue a workflow — a step that "returned null" because its output could not be read would
   * corrupt the run it is replaying, silently and durably.
   */
  @Test
  public void testStepResultRefusesToReplayWhatItCannotRead() {
    var recorded =
        new StepResult("wf", 0, "echoStep", "cG9zaXRpb25hbA==", null, null, "custom_base64");

    assertThrows(IllegalArgumentException.class, () -> recorded.toResult(null));
  }
}
