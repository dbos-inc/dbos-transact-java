package dev.dbos.transact.json;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSClient;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.workflow.Queue;
import dev.dbos.transact.workflow.SerializationStrategy;
import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.WorkflowClassName;
import dev.dbos.transact.workflow.WorkflowHandle;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for portable serialization format. These tests verify that workflows can be triggered via
 * direct database inserts using the portable JSON format, simulating cross-language workflow
 * initiation.
 */
@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
public class PortableSerializationTest {

  private static DBOSConfig dbosConfig;
  private HikariDataSource dataSource;

  @BeforeAll
  static void onetimeSetup() throws Exception {
    PortableSerializationTest.dbosConfig =
        DBOSConfig.defaultsFromEnv("portablesertest")
            .withDatabaseUrl("jdbc:postgresql://localhost:5432/dbos_java_sys");
  }

  @BeforeEach
  void beforeEachTest() throws SQLException {
    DBUtils.recreateDB(dbosConfig);
    dataSource = SystemDatabase.createDataSource(dbosConfig);
    DBOS.reinitialize(dbosConfig);
  }

  @AfterEach
  void afterEachTest() throws Exception {
    dataSource.close();
    DBOS.shutdown();
  }

  /** Workflow interface for portable serialization tests. */
  public interface PortableTestService {
    String recvWorkflow(String topic, long timeoutMs);
  }

  /** Implementation of the portable test workflow. */
  @WorkflowClassName("PortableTestService")
  public static class PortableTestServiceImpl implements PortableTestService {
    @Workflow(name = "recvWorkflow")
    @Override
    public String recvWorkflow(String topic, long timeoutMs) {
      Object received = DBOS.recv(topic, Duration.ofMillis(timeoutMs));
      return "received:" + received;
    }
  }

  /**
   * Tests that a workflow can be triggered via direct database insert using portable JSON format.
   * This simulates the scenario where a workflow is initiated by another language.
   */
  @Test
  public void testDirectInsertPortable() throws Exception {
    // Register queue and workflow
    Queue testQueue = new Queue("testq");
    DBOS.registerQueue(testQueue);

    PortableTestService service =
        DBOS.registerWorkflows(PortableTestService.class, new PortableTestServiceImpl());

    DBOS.launch();

    String workflowId = UUID.randomUUID().toString();

    // Insert workflow_status directly with portable_json format
    // The inputs are in portable format: { "positionalArgs": ["incoming", 30000] }
    // where "incoming" is the topic and 30000 is the timeout in ms
    try (Connection conn = dataSource.getConnection()) {
      String insertWorkflowSql =
          """
          INSERT INTO dbos.workflow_status(
            workflow_uuid,
            name,
            class_name,
            config_name,
            queue_name,
            status,
            inputs,
            created_at,
            serialization
          )
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
          """;

      try (PreparedStatement stmt = conn.prepareStatement(insertWorkflowSql)) {
        stmt.setString(1, workflowId);
        stmt.setString(2, "recvWorkflow"); // workflow name (from @Workflow annotation)
        stmt.setString(3, "PortableTestService"); // class name alias (from @WorkflowClassName)
        stmt.setString(4, null);
        stmt.setString(5, "testq"); // queue name
        stmt.setString(6, "ENQUEUED"); // status
        // Portable JSON format for inputs: positionalArgs array with topic and timeout
        stmt.setString(7, "{\"positionalArgs\":[\"incoming\",30000]}"); // inputs in portable format
        stmt.setLong(8, System.currentTimeMillis()); // created_at
        stmt.setString(9, "portable_json"); // serialization format
        stmt.executeUpdate();
      }

      // Insert notification directly with portable_json format
      String insertNotificationSql =
          """
          INSERT INTO dbos.notifications(
            destination_uuid,
            topic,
            message,
            serialization
          )
          VALUES (?, ?, ?, ?)
          """;

      try (PreparedStatement stmt = conn.prepareStatement(insertNotificationSql)) {
        stmt.setString(1, workflowId);
        stmt.setString(2, "incoming"); // topic
        stmt.setString(
            3, "\"HelloFromPortable\""); // message in portable JSON format (quoted string)
        stmt.setString(4, "portable_json"); // serialization format
        stmt.executeUpdate();
      }
    }

    // Retrieve the workflow handle and await the result
    WorkflowHandle<String, ?> handle = DBOS.retrieveWorkflow(workflowId);
    String result = handle.getResult();

    // Verify the result
    assertEquals("received:HelloFromPortable", result);

    // Verify the workflow completed successfully
    var status = handle.getStatus();
    assertEquals("SUCCESS", status.status());

    // Verify the output was written in portable format
    var row = DBUtils.getWorkflowRow(dataSource, workflowId);
    assertNotNull(row);
    assertEquals("portable_json", row.serialization());
    // The output should be in portable format (simple quoted string)
    assertEquals("\"received:HelloFromPortable\"", row.output());
  }

  /**
   * Tests that a workflow can be enqueued using DBOSClient.enqueueWorkflow with portable
   * serialization type option.
   */
  @Test
  public void testClientEnqueueWithPortableSerialization() throws Exception {
    // Register queue and workflow
    Queue testQueue = new Queue("testq");
    DBOS.registerQueue(testQueue);

    PortableTestService service =
        DBOS.registerWorkflows(PortableTestService.class, new PortableTestServiceImpl());

    DBOS.launch();

    // Create a DBOSClient
    try (DBOSClient client = new DBOSClient(dataSource)) {
      String workflowId = UUID.randomUUID().toString();

      // Enqueue workflow using client with portable serialization
      var options =
          new DBOSClient.EnqueueOptions("PortableTestService", "recvWorkflow", "testq")
              .withWorkflowId(workflowId)
              .withSerialization(SerializationStrategy.PORTABLE);

      WorkflowHandle<String, ?> handle =
          client.enqueueWorkflow(options, new Object[] {"incoming", 30000L});

      // Send a message using portable serialization
      client.send(
          workflowId, "HelloFromClient", "incoming", null, DBOSClient.SendOptions.portable());

      // Await the result
      String result = handle.getResult();

      // Verify the result
      assertEquals("received:HelloFromClient", result);

      // Verify the workflow completed successfully
      var status = handle.getStatus();
      assertEquals("SUCCESS", status.status());

      // Verify the workflow was stored with portable serialization
      var row = DBUtils.getWorkflowRow(dataSource, workflowId);
      assertNotNull(row);
      assertEquals("portable_json", row.serialization());
    }
  }

  /**
   * Tests that a workflow can be enqueued using DBOSClient.enqueuePortableWorkflow which uses
   * portable JSON serialization by default without validation.
   */
  @Test
  public void testClientEnqueuePortableWorkflow() throws Exception {
    // Register queue and workflow
    Queue testQueue = new Queue("testq");
    DBOS.registerQueue(testQueue);

    PortableTestService service =
        DBOS.registerWorkflows(PortableTestService.class, new PortableTestServiceImpl());

    DBOS.launch();

    // Create a DBOSClient
    try (DBOSClient client = new DBOSClient(dataSource)) {
      String workflowId = UUID.randomUUID().toString();

      // Enqueue workflow using enqueuePortableWorkflow
      var options =
          new DBOSClient.EnqueueOptions("PortableTestService", "recvWorkflow", "testq")
              .withWorkflowId(workflowId);

      // Use enqueuePortableWorkflow which defaults to portable serialization
      var handle =
          client.<String>enqueuePortableWorkflow(options, new Object[] {"incoming", 30000L}, null);

      // Send a message using portable serialization
      client.send(
          workflowId,
          "HelloFromPortableClient",
          "incoming",
          null,
          DBOSClient.SendOptions.portable());

      // Await the result
      String result = handle.getResult();

      // Verify the result
      assertEquals("received:HelloFromPortableClient", result);

      // Verify the workflow completed successfully
      var status = handle.getStatus();
      assertEquals("SUCCESS", status.status());

      // Verify the workflow was stored with portable serialization
      var row = DBUtils.getWorkflowRow(dataSource, workflowId);
      assertNotNull(row);
      assertEquals("portable_json", row.serialization());
      // The output should be in portable format
      assertEquals("\"received:HelloFromPortableClient\"", row.output());
    }
  }

  /** Workflow interface for testing setEvent and send with explicit serialization. */
  public interface ExplicitSerService {
    String eventWorkflow();

    void senderWorkflow(String targetId);
  }

  /** Implementation that sets events with different serialization types. */
  @WorkflowClassName("ExplicitSerService")
  public static class ExplicitSerServiceImpl implements ExplicitSerService {
    @Workflow(name = "eventWorkflow")
    @Override
    public String eventWorkflow() {
      // Set events with different serialization types
      DBOS.setEvent("defaultEvent", "defaultValue");
      DBOS.setEvent("nativeEvent", "nativeValue", SerializationStrategy.NATIVE);
      DBOS.setEvent("portableEvent", "portableValue", SerializationStrategy.PORTABLE);
      return "done";
    }

    @Workflow(name = "senderWorkflow")
    @Override
    public void senderWorkflow(String targetId) {
      // Send messages with different serialization types
      DBOS.send(targetId, "defaultMsg", "defaultTopic");
      DBOS.send(targetId, "nativeMsg", "nativeTopic", null, SerializationStrategy.NATIVE);
      DBOS.send(targetId, "portableMsg", "portableTopic", null, SerializationStrategy.PORTABLE);
    }
  }

  /** Implementation that sets events with different serialization types. */
  @WorkflowClassName("ExplicitSerServicePortable")
  public static class ExplicitSerServicePortableImpl implements ExplicitSerService {
    @Workflow(name = "eventWorkflow", serializationStrategy = SerializationStrategy.PORTABLE)
    @Override
    public String eventWorkflow() {
      // Set events with different serialization types
      DBOS.setEvent("defaultEvent", "defaultValue");
      DBOS.setEvent("nativeEvent", "nativeValue", SerializationStrategy.NATIVE);
      DBOS.setEvent("portableEvent", "portableValue", SerializationStrategy.PORTABLE);
      return "done";
    }

    @Workflow(name = "senderWorkflow")
    @Override
    public void senderWorkflow(String targetId) {
      // Send messages with different serialization types
      DBOS.send(targetId, "defaultMsg", "defaultTopic");
      DBOS.send(targetId, "nativeMsg", "nativeTopic", null, SerializationStrategy.NATIVE);
      DBOS.send(targetId, "portableMsg", "portableTopic", null, SerializationStrategy.PORTABLE);
    }
  }

  /** Workflow that throws an error for testing portable error serialization. */
  public interface ErrorService {
    void errorWorkflow();
  }

  @WorkflowClassName("ErrorService")
  public static class ErrorServiceImpl implements ErrorService {
    @Workflow(name = "errorWorkflow")
    @Override
    public void errorWorkflow() {
      throw new RuntimeException("Workflow failed!");
    }
  }

  /**
   * Tests that DBOS.setEvent() with explicit SerializationStrategy correctly stores the
   * serialization format in the database.
   */
  @Test
  public void testSetEventWithVaryingSerialization() throws Exception {
    Queue testQueue = new Queue("testq");
    DBOS.registerQueue(testQueue);
    var defsvc = DBOS.registerWorkflows(ExplicitSerService.class, new ExplicitSerServiceImpl());
    var portsvc =
        DBOS.registerWorkflows(ExplicitSerService.class, new ExplicitSerServicePortableImpl());

    DBOS.launch();

    for (String sertype : new String[] {"defq", "portq", "defstart", "portstart"}) {
      // Use DBOSClient to enqueue and run the workflow
      try (DBOSClient client = new DBOSClient(dataSource)) {
        String workflowId = UUID.randomUUID().toString();
        WorkflowHandle<String, ?> handle = null;
        boolean isPortable = sertype.startsWith("port");

        if (sertype.equals("defq")) {
          var options =
              new DBOSClient.EnqueueOptions("ExplicitSerService", "eventWorkflow", "testq")
                  .withWorkflowId(workflowId);

          handle = client.enqueueWorkflow(options, new Object[] {});
        }
        if (sertype.equals("portq")) {
          var options =
              new DBOSClient.EnqueueOptions("ExplicitSerService", "eventWorkflow", "testq")
                  .withWorkflowId(workflowId)
                  .withSerialization(SerializationStrategy.PORTABLE);

          handle = client.enqueueWorkflow(options, new Object[] {});
        }
        if (sertype.equals("defstart")) {
          handle =
              DBOS.startWorkflow(
                  () -> {
                    return defsvc.eventWorkflow();
                  },
                  new StartWorkflowOptions(workflowId));
        }
        if (sertype.equals("portstart")) {
          handle =
              DBOS.startWorkflow(
                  () -> {
                    return portsvc.eventWorkflow();
                  },
                  new StartWorkflowOptions(workflowId));
        }

        String result = handle.getResult();
        assertEquals("done", result);

        // Check workflow's serialization
        var wfRow = DBUtils.getWorkflowRow(dataSource, workflowId);
        assertNotNull(wfRow);
        var expectedSer = isPortable ? "portable_json" : "java_jackson";
        if (!expectedSer.equals(wfRow.serialization())) {
          System.err.println("Expected serialization does not match in: " + sertype);
        }
        assertEquals(expectedSer, wfRow.serialization());

        // Verify the events in the database have correct serialization
        var events = DBUtils.getWorkflowEvents(dataSource, workflowId);
        assertEquals(3, events.size());

        // Find each event and verify serialization
        var defaultEvent = events.stream().filter(e -> e.key().equals("defaultEvent")).findFirst();
        var nativeEvent = events.stream().filter(e -> e.key().equals("nativeEvent")).findFirst();
        var portableEvent =
            events.stream().filter(e -> e.key().equals("portableEvent")).findFirst();

        assertTrue(defaultEvent.isPresent());
        assertTrue(nativeEvent.isPresent());
        assertTrue(portableEvent.isPresent());

        // Default setEvent inherits workflow's serialization
        assertEquals(expectedSer, defaultEvent.get().serialization());
        // Native should have java_jackson (explicitly set)
        assertEquals("java_jackson", nativeEvent.get().serialization());
        // Portable should have portable_json (explicitly set)
        assertEquals("portable_json", portableEvent.get().serialization());

        // Also verify the event history
        var eventHistory = DBUtils.getWorkflowEventHistory(dataSource, workflowId);
        assertEquals(3, eventHistory.size());

        var defaultHist =
            eventHistory.stream().filter(e -> e.key().equals("defaultEvent")).findFirst();
        var nativeHist =
            eventHistory.stream().filter(e -> e.key().equals("nativeEvent")).findFirst();
        var portableHist =
            eventHistory.stream().filter(e -> e.key().equals("portableEvent")).findFirst();

        assertTrue(defaultHist.isPresent());
        assertTrue(nativeHist.isPresent());
        assertTrue(portableHist.isPresent());

        assertEquals(expectedSer, defaultHist.get().serialization());
        assertEquals("java_jackson", nativeHist.get().serialization());
        assertEquals("portable_json", portableHist.get().serialization());
      }
    }
  }

  /**
   * Tests that DBOS.send() with explicit SerializationStrategy correctly stores the serialization
   * format in the notifications table.
   */
  @Test
  public void testSendWithExplicitSerialization() throws Exception {
    Queue testQueue = new Queue("testq");
    DBOS.registerQueue(testQueue);
    DBOS.registerWorkflows(ExplicitSerService.class, new ExplicitSerServiceImpl());

    DBOS.launch();

    // Create a target workflow to receive messages
    String targetId = UUID.randomUUID().toString();

    // Insert a dummy workflow to be the target (so FK constraint is satisfied)
    try (Connection conn = dataSource.getConnection()) {
      String insertSql =
          """
          INSERT INTO dbos.workflow_status(workflow_uuid, name, class_name, config_name, status, created_at)
          VALUES (?, 'dummy', 'Dummy', '', 'PENDING', ?)
          """;
      try (PreparedStatement stmt = conn.prepareStatement(insertSql)) {
        stmt.setString(1, targetId);
        stmt.setLong(2, System.currentTimeMillis());
        stmt.executeUpdate();
      }
    }

    // Use DBOSClient to enqueue and run the sender workflow
    try (DBOSClient client = new DBOSClient(dataSource)) {
      String workflowId = UUID.randomUUID().toString();

      var options =
          new DBOSClient.EnqueueOptions("ExplicitSerService", "senderWorkflow", "testq")
              .withWorkflowId(workflowId);

      WorkflowHandle<Void, ?> handle = client.enqueueWorkflow(options, new Object[] {targetId});
      handle.getResult();

      // Verify the notifications in the database have correct serialization
      var notifications = DBUtils.getNotifications(dataSource, targetId);
      assertEquals(3, notifications.size());

      var defaultNotif =
          notifications.stream().filter(n -> n.topic().equals("defaultTopic")).findFirst();
      var nativeNotif =
          notifications.stream().filter(n -> n.topic().equals("nativeTopic")).findFirst();
      var portableNotif =
          notifications.stream().filter(n -> n.topic().equals("portableTopic")).findFirst();

      assertTrue(defaultNotif.isPresent());
      assertTrue(nativeNotif.isPresent());
      assertTrue(portableNotif.isPresent());

      // Default should have native serialization (backward compatible)
      assertEquals("java_jackson", defaultNotif.get().serialization());
      // Native should have java_jackson
      assertEquals("java_jackson", nativeNotif.get().serialization());
      // Portable should have portable_json
      assertEquals("portable_json", portableNotif.get().serialization());

      // Also verify the message format
      // Portable format wraps strings in quotes
      assertEquals("\"portableMsg\"", portableNotif.get().message());
    }
  }

  /** Simple workflow interface for event setting tests. */
  public interface EventSetterService {
    String setEventWorkflow();
  }

  @WorkflowClassName("EventSetterService")
  public static class EventSetterServiceImpl implements EventSetterService {
    @Workflow(name = "setEventWorkflow")
    @Override
    public String setEventWorkflow() {
      // Set event without explicit serialization - should inherit from workflow
      // context
      DBOS.setEvent("myKey", "myValue");
      return "eventSet";
    }
  }

  /**
   * Tests that a portable workflow (started via portable enqueue) uses portable serialization by
   * default for setEvent when no explicit serialization is specified.
   */
  @Test
  public void testPortableWorkflowDefaultSerialization() throws Exception {
    // Workflow that sets an event without explicit serialization
    Queue testQueue = new Queue("testq");
    DBOS.registerQueue(testQueue);

    // Register a simple workflow that sets an event
    DBOS.registerWorkflows(EventSetterService.class, new EventSetterServiceImpl());

    DBOS.launch();

    try (DBOSClient client = new DBOSClient(dataSource)) {
      String workflowId = UUID.randomUUID().toString();

      // Enqueue with portable serialization
      var options =
          new DBOSClient.EnqueueOptions("EventSetterService", "setEventWorkflow", "testq")
              .withWorkflowId(workflowId)
              .withSerialization(SerializationStrategy.PORTABLE);

      WorkflowHandle<String, ?> handle = client.enqueueWorkflow(options, new Object[] {});

      // Wait for completion
      String result = handle.getResult();
      assertEquals("eventSet", result);

      // Verify the workflow used portable serialization
      var row = DBUtils.getWorkflowRow(dataSource, workflowId);
      assertNotNull(row);
      assertEquals("portable_json", row.serialization());

      // Verify the event inherited portable serialization (since it was set without
      // explicit type)
      var events = DBUtils.getWorkflowEvents(dataSource, workflowId);
      assertEquals(1, events.size());
      assertEquals("myKey", events.get(0).key());
      // Event should inherit workflow's portable serialization
      assertEquals("portable_json", events.get(0).serialization());
    }
  }

  /** Tests that errors thrown from portable workflows are stored in portable JSON format. */
  @Test
  public void testPortableWorkflowErrorSerialization() throws Exception {
    Queue testQueue = new Queue("testq");
    DBOS.registerQueue(testQueue);

    DBOS.registerWorkflows(ErrorService.class, new ErrorServiceImpl());

    DBOS.launch();

    try (DBOSClient client = new DBOSClient(dataSource)) {
      String workflowId = UUID.randomUUID().toString();

      // Enqueue with portable serialization
      var options =
          new DBOSClient.EnqueueOptions("ErrorService", "errorWorkflow", "testq")
              .withWorkflowId(workflowId)
              .withSerialization(SerializationStrategy.PORTABLE);

      WorkflowHandle<Void, ?> handle = client.enqueueWorkflow(options, new Object[] {});

      // Wait for completion - should throw
      try {
        handle.getResult();
        fail("Expected exception to be thrown");
      } catch (Exception e) {
        // Expected
        assertTrue(e.getMessage().contains("Workflow failed!"));
      }

      // Verify the workflow stored error in portable format
      var row = DBUtils.getWorkflowRow(dataSource, workflowId);
      assertNotNull(row);
      assertEquals("portable_json", row.serialization());
      assertEquals("ERROR", row.status());

      // Verify error is in portable JSON format
      assertNotNull(row.error());
      // Portable error format: {"name":"...", "message":"..."}
      assertTrue(row.error().contains("\"name\""));
      assertTrue(row.error().contains("\"message\""));
      assertTrue(row.error().contains("Workflow failed!"));
    }
  }

  /**
   * Tests that DBOSClient.getEvent can retrieve events set with different serialization formats.
   */
  @Test
  public void testClientGetEvent() throws Exception {
    Queue testQueue = new Queue("testq");
    DBOS.registerQueue(testQueue);
    DBOS.registerWorkflows(ExplicitSerService.class, new ExplicitSerServiceImpl());

    DBOS.launch();

    // Use DBOSClient to enqueue and run the workflow that sets events
    try (DBOSClient client = new DBOSClient(dataSource)) {
      String workflowId = UUID.randomUUID().toString();

      var options =
          new DBOSClient.EnqueueOptions("ExplicitSerService", "eventWorkflow", "testq")
              .withWorkflowId(workflowId);

      WorkflowHandle<String, ?> handle = client.enqueueWorkflow(options, new Object[] {});
      String result = handle.getResult();
      assertEquals("done", result);

      // Get events with different serializations
      Object defaultVal = client.getEvent(workflowId, "defaultEvent", Duration.ofSeconds(5));
      Object nativeVal = client.getEvent(workflowId, "nativeEvent", Duration.ofSeconds(5));
      Object portableVal = client.getEvent(workflowId, "portableEvent", Duration.ofSeconds(5));

      // All should be retrievable regardless of serialization format
      assertEquals("defaultValue", defaultVal);
      assertEquals("nativeValue", nativeVal);
      assertEquals("portableValue", portableVal);
    }
  }

  /** Simple workflow interface for more JSON expressiveness. */
  public interface SerializedTypesService {
    String checkWorkflow(
        String sv, boolean bv, double dv, ArrayList<String> sva, Map<String, Object> mapv);
  }

  @WorkflowClassName("SerializedTypesService")
  public static class SerializedTypesServiceImpl implements SerializedTypesService {
    @Workflow(name = "checkWorkflow")
    @Override
    public String checkWorkflow(
        String sv, boolean bv, double dv, ArrayList<String> sva, Map<String, Object> mapv) {
      return sv + bv + dv + sva + mapv;
    }
  }

  /** Tests that errors thrown from portable workflows are stored in portable JSON format. */
  @Test
  public void testArgSerialization() throws Exception {
    Queue testQueue = new Queue("testq");
    DBOS.registerQueue(testQueue);

    DBOS.registerWorkflows(SerializedTypesService.class, new SerializedTypesServiceImpl());

    DBOS.launch();

    try (DBOSClient client = new DBOSClient(dataSource)) {
      String workflowId = UUID.randomUUID().toString();

      // Enqueue with portable serialization
      var options =
          new DBOSClient.EnqueueOptions("SerializedTypesService", "checkWorkflow", "testq")
              .withWorkflowId(workflowId)
              .withSerialization(SerializationStrategy.PORTABLE);

      WorkflowHandle<String, ?> handle =
          client.enqueueWorkflow(
              options,
              new Object[] {
                "Apex", true, 1.01, new String[] {"hello", "world"}, Map.of("K3Y", "VALU3")
              });

      var rv = handle.getResult();

      // Verify the workflow stored error in portable format
      var row = DBUtils.getWorkflowRow(dataSource, workflowId);
      assertNotNull(row);
      assertEquals("portable_json", row.serialization());
      assertEquals("SUCCESS", row.status());
      assertEquals("Apextrue1.01[hello, world]{K3Y=VALU3}", rv);
    }
  }

  // ============ Custom Serializer Tests ============

  /** A test custom serializer that base64-encodes JSON. */
  static class TestBase64Serializer implements DBOSSerializer {
    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public String name() {
      return "custom_base64";
    }

    @Override
    public String stringify(Object value, boolean noHistoricalWrapper) {
      try {
        String json = mapper.writeValueAsString(value);
        return Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8));
      } catch (Exception e) {
        throw new RuntimeException("Failed to serialize", e);
      }
    }

    @Override
    public Object parse(String text, boolean noHistoricalWrapper) {
      if (text == null) return null;
      try {
        String json = new String(Base64.getDecoder().decode(text), StandardCharsets.UTF_8);
        return mapper.readValue(json, Object.class);
      } catch (Exception e) {
        throw new RuntimeException("Failed to deserialize", e);
      }
    }

    @Override
    public String stringifyThrowable(Throwable throwable) {
      try {
        var errorMap =
            Map.of(
                "class",
                throwable.getClass().getName(),
                "message",
                throwable.getMessage() != null ? throwable.getMessage() : "");
        String json = mapper.writeValueAsString(errorMap);
        return Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8));
      } catch (Exception e) {
        throw new RuntimeException("Failed to serialize throwable", e);
      }
    }

    @Override
    public Throwable parseThrowable(String text) {
      if (text == null) return null;
      try {
        String json = new String(Base64.getDecoder().decode(text), StandardCharsets.UTF_8);
        @SuppressWarnings("unchecked")
        var map = (Map<String, String>) mapper.readValue(json, Map.class);
        return new RuntimeException(map.get("message"));
      } catch (Exception e) {
        throw new RuntimeException("Failed to deserialize throwable", e);
      }
    }
  }

  /** Workflow interface for custom serializer tests. */
  public interface CustomSerService {
    String customSerWorkflow(String input);
  }

  @WorkflowClassName("CustomSerService")
  public static class CustomSerServiceImpl implements CustomSerService {
    @Workflow(name = "customSerWorkflow")
    @Override
    public String customSerWorkflow(String input) {
      DBOS.setEvent("testKey", "eventValue-" + input);
      Object msg = DBOS.recv("testTopic", Duration.ofSeconds(30));
      return "result:" + input + ":" + msg;
    }
  }

  /** Tests that a custom serializer configured via DBOSConfig is used for all serialization. */
  @Test
  public void testCustomSerializer() throws Exception {
    // Shutdown existing DBOS from @BeforeEach
    DBOS.shutdown();

    // Reinitialize with custom serializer
    var customConfig = dbosConfig.withSerializer(new TestBase64Serializer());
    DBOS.reinitialize(customConfig);

    Queue testQueue = new Queue("testq");
    DBOS.registerQueue(testQueue);
    CustomSerService svc =
        DBOS.registerWorkflows(CustomSerService.class, new CustomSerServiceImpl());

    DBOS.launch();

    String workflowId = UUID.randomUUID().toString();

    // Start the workflow via queue
    try (DBOSClient client = new DBOSClient(dataSource, null, new TestBase64Serializer())) {
      var options =
          new DBOSClient.EnqueueOptions("CustomSerService", "customSerWorkflow", "testq")
              .withWorkflowId(workflowId);

      WorkflowHandle<String, ?> handle = client.enqueueWorkflow(options, new Object[] {"hello"});

      // Send a message
      client.send(workflowId, "worldMsg", "testTopic", null);

      // Wait for result
      String result = handle.getResult();
      assertEquals("result:hello:worldMsg", result);

      // Verify DB rows have custom serialization format
      var row = DBUtils.getWorkflowRow(dataSource, workflowId);
      assertNotNull(row);
      assertEquals("custom_base64", row.serialization());

      // Verify the event was stored with custom serialization
      var events = DBUtils.getWorkflowEvents(dataSource, workflowId);
      var testEvent = events.stream().filter(e -> e.key().equals("testKey")).findFirst();
      assertTrue(testEvent.isPresent());
      assertEquals("custom_base64", testEvent.get().serialization());

      // Verify getEvent works through custom serializer
      Object eventVal = client.getEvent(workflowId, "testKey", Duration.ofSeconds(5));
      assertEquals("eventValue-hello", eventVal);
    }
  }

  /**
   * Tests that data written with the default serializer is readable after switching to a custom
   * serializer, and vice versa (SerializationUtil dispatches based on stored format name).
   */
  @Test
  public void testCustomSerializerInterop() throws Exception {
    // Phase 1: Launch with default serializer, run a workflow
    Queue testQueue = new Queue("testq");
    DBOS.registerQueue(testQueue);
    DBOS.registerWorkflows(EventSetterService.class, new EventSetterServiceImpl());
    DBOS.launch();

    String wfId1 = UUID.randomUUID().toString();
    try (DBOSClient client = new DBOSClient(dataSource)) {
      var options =
          new DBOSClient.EnqueueOptions("EventSetterService", "setEventWorkflow", "testq")
              .withWorkflowId(wfId1);
      var handle = client.enqueueWorkflow(options, new Object[] {});
      assertEquals("eventSet", handle.getResult());
    }

    // Verify Phase 1 data is java_jackson
    var row1 = DBUtils.getWorkflowRow(dataSource, wfId1);
    assertNotNull(row1);
    assertEquals("java_jackson", row1.serialization());

    DBOS.shutdown();

    // Phase 2: Relaunch with custom serializer
    var customConfig = dbosConfig.withSerializer(new TestBase64Serializer());
    DBOS.reinitialize(customConfig);
    DBOS.registerQueue(testQueue);
    DBOS.registerWorkflows(EventSetterService.class, new EventSetterServiceImpl());
    DBOS.launch();

    String wfId2 = UUID.randomUUID().toString();
    try (DBOSClient client = new DBOSClient(dataSource, null, new TestBase64Serializer())) {
      var options =
          new DBOSClient.EnqueueOptions("EventSetterService", "setEventWorkflow", "testq")
              .withWorkflowId(wfId2);
      var handle = client.enqueueWorkflow(options, new Object[] {});
      assertEquals("eventSet", handle.getResult());

      // Read event from Phase 1 (java_jackson) - should still be readable
      Object val1 = client.getEvent(wfId1, "myKey", Duration.ofSeconds(5));
      assertEquals("myValue", val1);

      // Read event from Phase 2 (custom_base64) - should be readable
      Object val2 = client.getEvent(wfId2, "myKey", Duration.ofSeconds(5));
      assertEquals("myValue", val2);
    }

    // Verify Phase 2 data uses custom serialization
    var row2 = DBUtils.getWorkflowRow(dataSource, wfId2);
    assertNotNull(row2);
    assertEquals("custom_base64", row2.serialization());

    DBOS.shutdown();

    // Phase 3: Relaunch with custom serializer again, verify Phase 2 data still readable
    DBOS.reinitialize(customConfig);
    DBOS.registerQueue(testQueue);
    DBOS.registerWorkflows(EventSetterService.class, new EventSetterServiceImpl());
    DBOS.launch();

    try (DBOSClient client = new DBOSClient(dataSource, null, new TestBase64Serializer())) {
      Object val2 = client.getEvent(wfId2, "myKey", Duration.ofSeconds(5));
      assertEquals("myValue", val2);
    }
  }

  /**
   * Tests that data written with a custom serializer becomes unreadable if that serializer is
   * removed.
   */
  @Test
  public void testCustomSerializerRemoved() throws Exception {
    // Shutdown existing DBOS from @BeforeEach
    DBOS.shutdown();

    // Launch with custom serializer
    var customConfig = dbosConfig.withSerializer(new TestBase64Serializer());
    DBOS.reinitialize(customConfig);

    Queue testQueue = new Queue("testq");
    DBOS.registerQueue(testQueue);
    DBOS.registerWorkflows(EventSetterService.class, new EventSetterServiceImpl());
    DBOS.launch();

    String wfId = UUID.randomUUID().toString();
    try (DBOSClient client = new DBOSClient(dataSource, null, new TestBase64Serializer())) {
      var options =
          new DBOSClient.EnqueueOptions("EventSetterService", "setEventWorkflow", "testq")
              .withWorkflowId(wfId);
      var handle = client.enqueueWorkflow(options, new Object[] {});
      assertEquals("eventSet", handle.getResult());
    }

    // Verify it's stored with custom_base64
    var row = DBUtils.getWorkflowRow(dataSource, wfId);
    assertNotNull(row);
    assertEquals("custom_base64", row.serialization());

    DBOS.shutdown();

    // Relaunch WITHOUT custom serializer
    DBOS.reinitialize(dbosConfig);
    DBOS.registerQueue(testQueue);
    DBOS.registerWorkflows(EventSetterService.class, new EventSetterServiceImpl());
    DBOS.launch();

    // Attempt to getEvent on the custom-serialized workflow - should fail
    try (DBOSClient client = new DBOSClient(dataSource)) {
      assertThrows(
          IllegalArgumentException.class,
          () -> client.getEvent(wfId, "myKey", Duration.ofSeconds(2)),
          "Serialization is not available");
    }
  }
}
