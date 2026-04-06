package dev.dbos.transact.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.Queue;
import dev.dbos.transact.workflow.StepInfo;

import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.*;

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
public class StreamTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();

  DBOSConfig dbosConfig;
  @AutoClose DBOS dbos;
  @AutoClose HikariDataSource dataSource;
  StreamTestService proxy;

  @BeforeEach
  void beforeEach() {
    dbosConfig = pgContainer.dbosConfig();
    dbos = new DBOS(dbosConfig);
    dataSource = pgContainer.dataSource();

    dbos.registerQueue(new Queue("testQueue"));
    proxy = dbos.registerProxy(StreamTestService.class, new StreamTestServiceImpl(dbos));

    dbos.launch();
  }

  @Test
  public void writeStreamBasicTest() throws Exception {
    var wfid = UUID.randomUUID().toString();
    try (var ctx = new WorkflowOptions(wfid).setContext()) {
      proxy.writeStreamBasic("mykey", "myvalue");
    }

    Iterator<Object> iter = dbos.readStream(wfid, "mykey");
    assertTrue(iter.hasNext());
    assertEquals("myvalue", iter.next());
    assertFalse(iter.hasNext());

    // writeStream from workflow records one step
    List<StepInfo> steps = dbos.listWorkflowSteps(wfid);
    assertEquals(1, steps.size());
    assertEquals("DBOS.writeStream", steps.get(0).functionName());
  }

  @Test
  public void writeStreamMultipleTest() throws Exception {
    var wfid = UUID.randomUUID().toString();
    try (var ctx = new WorkflowOptions(wfid).setContext()) {
      proxy.writeStreamMultiple("mykey");
    }

    Iterator<Object> iter = dbos.readStream(wfid, "mykey");
    assertEquals("value1", iter.next());
    assertEquals("value2", iter.next());
    assertEquals("value3", iter.next());
    assertFalse(iter.hasNext());

    // each writeStream from workflow records one step
    List<StepInfo> steps = dbos.listWorkflowSteps(wfid);
    assertEquals(3, steps.size());
    assertTrue(steps.stream().allMatch(s -> s.functionName().equals("DBOS.writeStream")));
  }

  @Test
  public void writeStreamCloseTest() throws Exception {
    var wfid = UUID.randomUUID().toString();
    try (var ctx = new WorkflowOptions(wfid).setContext()) {
      proxy.writeAndCloseStream("mykey");
    }

    Iterator<Object> iter = dbos.readStream(wfid, "mykey");
    assertEquals("value1", iter.next());
    assertEquals("value2", iter.next());
    assertFalse(iter.hasNext());

    // two writeStream + one closeStream each record a step
    List<StepInfo> steps = dbos.listWorkflowSteps(wfid);
    assertEquals(3, steps.size());
    assertEquals(
        2, steps.stream().filter(s -> s.functionName().equals("DBOS.writeStream")).count());
    assertEquals(
        1, steps.stream().filter(s -> s.functionName().equals("DBOS.closeStream")).count());
  }

  @Test
  public void writeStreamInStepTest() throws Exception {
    var wfid = UUID.randomUUID().toString();
    try (var ctx = new WorkflowOptions(wfid).setContext()) {
      proxy.writeStreamInStep("mykey", "stepvalue");
    }

    Iterator<Object> iter = dbos.readStream(wfid, "mykey");
    assertTrue(iter.hasNext());
    assertEquals("stepvalue", iter.next());
    assertFalse(iter.hasNext());

    // writeStream from inside a step does not add its own step record
    List<StepInfo> steps = dbos.listWorkflowSteps(wfid);
    assertEquals(1, steps.size());
    assertEquals("streamStep", steps.get(0).functionName());
  }

  @Test
  public void readStreamFromDBOSTest() throws Exception {
    var wfid = UUID.randomUUID().toString();
    try (var ctx = new WorkflowOptions(wfid).setContext()) {
      proxy.writeStreamMultiple("mykey");
    }

    Iterator<Object> iter = dbos.readStream(wfid, "mykey");
    assertTrue(iter.hasNext());
    assertEquals("value1", iter.next());
    assertTrue(iter.hasNext());
    assertEquals("value2", iter.next());
    assertTrue(iter.hasNext());
    assertEquals("value3", iter.next());
    assertFalse(iter.hasNext());
  }

  @Test
  public void readStreamFromClientTest() throws Exception {
    var wfid = UUID.randomUUID().toString();
    try (var ctx = new WorkflowOptions(wfid).setContext()) {
      proxy.writeStreamMultiple("mykey");
    }

    try (var client = pgContainer.dbosClient()) {
      Iterator<Object> iter = client.readStream(wfid, "mykey");
      assertTrue(iter.hasNext());
      assertEquals("value1", iter.next());
      assertTrue(iter.hasNext());
      assertEquals("value2", iter.next());
      assertTrue(iter.hasNext());
      assertEquals("value3", iter.next());
      assertFalse(iter.hasNext());
    }
  }

  @Test
  public void readStreamClosedTest() throws Exception {
    var wfid = UUID.randomUUID().toString();
    try (var ctx = new WorkflowOptions(wfid).setContext()) {
      proxy.writeAndCloseStream("mykey");
    }

    Iterator<Object> iter = dbos.readStream(wfid, "mykey");
    assertTrue(iter.hasNext());
    assertEquals("value1", iter.next());
    assertTrue(iter.hasNext());
    assertEquals("value2", iter.next());
    assertFalse(iter.hasNext());
  }

  @Test
  public void readStreamNonExistentTest() {
    Iterator<Object> iter = dbos.readStream("nonexistent", "mykey");
    assertFalse(iter.hasNext());
  }

  @Test
  public void writeStreamOutsideWorkflowTest() {
    assertThrows(IllegalStateException.class, () -> dbos.writeStream("key", "value"));
  }

  @Test
  public void closeStreamOutsideWorkflowTest() {
    assertThrows(IllegalStateException.class, () -> dbos.closeStream("key"));
  }

  @Test
  public void closeStreamInStepTest() {
    var wfid = UUID.randomUUID().toString();
    assertThrows(
        IllegalStateException.class,
        () -> {
          try (var ctx = new WorkflowOptions(wfid).setContext()) {
            dbos.runStep(
                () -> {
                  dbos.closeStream("mykey");
                },
                "closeStep");
          }
        });
  }

  @Test
  public void readStreamNonExistentFromClientTest() {
    try (var client = pgContainer.dbosClient()) {
      Iterator<Object> iter = client.readStream("nonexistent", "mykey");
      assertFalse(iter.hasNext());
    }
  }
}
