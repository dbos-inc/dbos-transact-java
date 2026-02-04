package dev.dbos.transact.json;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSClient;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.workflow.Queue;
import dev.dbos.transact.workflow.SerializationStrategy;
import dev.dbos.transact.workflow.WorkflowHandle;

import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.WorkflowClassName;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.UUID;

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
    // Use long for timeout because portable JSON deserializes numbers as Long/Integer,
    // not as Duration objects
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
   * This simulates the scenario where a workflow is initiated by another language (e.g.,
   * TypeScript) using the portable serialization format.
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
        stmt.setString(4, ""); // config_name (instance name) - must be empty string, not null
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
      client.send(workflowId, "HelloFromClient", "incoming", null, DBOSClient.SendOptions.portable());

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
      WorkflowHandle<String, ?> handle =
          client.enqueuePortableWorkflow(options, new Object[] {"incoming", 30000L}, null);

      // Send a message using portable serialization
      client.send(workflowId, "HelloFromPortableClient", "incoming", null, DBOSClient.SendOptions.portable());

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
}
