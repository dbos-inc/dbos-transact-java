package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.utils.PgContainer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.*;

/**
 * Tests for {@link DBOS#listWorkflows}. Rather than executing real workflows, this test inserts
 * rows directly into {@code "dbos".workflow_status} so each filter option in {@link
 * ListWorkflowsInput} can be exercised deterministically and without the overhead of actually
 * running workflows.
 *
 * <p>Test data (10 rows, created_at = baseTime + offset ms):
 *
 * <pre>
 *  UUID          | status    | name    | class  | config | queue | exec   | ver  | user   | parent     | forkedFrom | +ms
 *  wf-alpha-1    | SUCCESS   | alpha   | ClassA | instA  | -     | exec-1 | v1.0 | user-a | -          | -          | +100
 *  wf-child-1    | SUCCESS   | child   | ClassA | instA  | -     | exec-1 | v1.0 | user-a | wf-alpha-1 | -          | +150
 *  wf-alpha-2    | SUCCESS   | alpha   | ClassA | instA  | -     | exec-1 | v1.0 | user-a | -          | -          | +200
 *  wf-alpha-3    | ERROR     | alpha   | ClassA | instB  | -     | exec-2 | v1.0 | user-b | -          | -          | +300
 *  wf-beta-1     | SUCCESS   | beta    | ClassB | instB  | q1    | exec-2 | v1.0 | user-a | -          | -          | +400
 *  wf-queue-3    | SUCCESS   | queueWf | ClassD | instC  | q3    | exec-2 | v1.0 | user-b | -          | -          | +450
 *  wf-beta-2     | CANCELLED | beta    | ClassB | instB  | -     | exec-1 | v2.0 | user-b | -          | -          | +500
 *  wf-gamma-1    | SUCCESS   | gamma   | ClassC | instA  | -     | exec-1 | v2.0 | user-a | -          | -          | +600
 *  wf-forked-1   | SUCCESS   | gamma   | ClassC | instA  | -     | exec-1 | v2.0 | user-a | -          | wf-alpha-1 | +650
 *  wf-gamma-2    | ERROR     | gamma   | ClassC | instA  | q2    | exec-2 | v2.0 | user-b | -          | -          | +700
 * </pre>
 *
 * Status totals: SUCCESS=7, ERROR=2, CANCELLED=1
 */
@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
public class ListWorkflowsTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();
  @AutoClose HikariDataSource dataSource;
  DBOSConfig dbosConfig;
  @AutoClose DBOS dbos;
  final long baseTime = System.currentTimeMillis();

  /**
   * Fixed base epoch-ms. All {@code created_at} values are {@code baseTime + offset}, guaranteeing
   * stable ascending order regardless of when the test runs.
   */
  @BeforeEach
  void beforeEach() throws Exception {
    dbosConfig = pgContainer.dbosConfig();
    dbos = new DBOS(dbosConfig);
    dbos.launch();
    dataSource = pgContainer.dataSource();
    populateWorkflows(dataSource, baseTime);
  }

  /** Inserts the 10 standard test rows described in the class-level javadoc. */
  private static void populateWorkflows(DataSource dataSource, long baseTime) throws SQLException {
    final String sql =
        """
            INSERT INTO "dbos".workflow_status
                (workflow_uuid, status, name, class_name, config_name,
                 queue_name, executor_id, application_version, authenticated_user,
                 parent_workflow_id, forked_from, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      // @formatter:off
      Object[][] workflows = {
        {
          "wf-alpha-1",
          "SUCCESS",
          "alpha",
          "ClassA",
          "instA",
          null,
          "exec-1",
          "v1.0",
          "user-a",
          null,
          null,
          baseTime + 100
        },
        {
          "wf-child-1",
          "SUCCESS",
          "child",
          "ClassA",
          "instA",
          null,
          "exec-1",
          "v1.0",
          "user-a",
          "wf-alpha-1",
          null,
          baseTime + 150
        },
        {
          "wf-alpha-2",
          "SUCCESS",
          "alpha",
          "ClassA",
          "instA",
          null,
          "exec-1",
          "v1.0",
          "user-a",
          null,
          null,
          baseTime + 200
        },
        {
          "wf-alpha-3",
          "ERROR",
          "alpha",
          "ClassA",
          "instB",
          null,
          "exec-2",
          "v1.0",
          "user-b",
          null,
          null,
          baseTime + 300
        },
        {
          "wf-beta-1",
          "SUCCESS",
          "beta",
          "ClassB",
          "instB",
          "q1",
          "exec-2",
          "v1.0",
          "user-a",
          null,
          null,
          baseTime + 400
        },
        {
          "wf-queue-3",
          "SUCCESS",
          "queueWf",
          "ClassD",
          "instC",
          "q3",
          "exec-2",
          "v1.0",
          "user-b",
          null,
          null,
          baseTime + 450
        },
        {
          "wf-beta-2",
          "CANCELLED",
          "beta",
          "ClassB",
          "instB",
          null,
          "exec-1",
          "v2.0",
          "user-b",
          null,
          null,
          baseTime + 500
        },
        {
          "wf-gamma-1",
          "SUCCESS",
          "gamma",
          "ClassC",
          "instA",
          null,
          "exec-1",
          "v2.0",
          "user-a",
          null,
          null,
          baseTime + 600
        },
        {
          "wf-forked-1",
          "SUCCESS",
          "gamma",
          "ClassC",
          "instA",
          null,
          "exec-1",
          "v2.0",
          "user-a",
          null,
          "wf-alpha-1",
          baseTime + 650
        },
        {
          "wf-gamma-2",
          "ERROR",
          "gamma",
          "ClassC",
          "instA",
          "q2",
          "exec-2",
          "v2.0",
          "user-b",
          null,
          null,
          baseTime + 700
        }
      };
      // @formatter:on
      for (Object[] wf : workflows) {
        for (int i = 0; i < wf.length; i++) {
          if (wf[i] == null) {
            ps.setObject(i + 1, null);
          } else if (wf[i] instanceof Long) {
            ps.setLong(i + 1, (Long) wf[i]);
          } else {
            ps.setString(i + 1, wf[i].toString());
          }
        }
        // updated_at = created_at
        ps.setLong(13, (Long) wf[11]);
        ps.addBatch();
      }
      ps.executeBatch();
    }
  }

  // ---------------------------------------------------------------------------
  // Tests
  // ---------------------------------------------------------------------------

  @Test
  public void testListAll() throws Exception {

    List<WorkflowStatus> all = dbos.listWorkflows(new ListWorkflowsInput());
    assertEquals(10, all.size());
  }

  @Test
  public void testFilterByWorkflowName() throws Exception {

    // alpha: wf-alpha-1, wf-alpha-2, wf-alpha-3 = 3
    List<WorkflowStatus> alpha =
        dbos.listWorkflows(new ListWorkflowsInput().withWorkflowName("alpha"));
    assertEquals(3, alpha.size());
    alpha.forEach(wf -> assertEquals("alpha", wf.name()));

    // beta: wf-beta-1, wf-beta-2 = 2
    List<WorkflowStatus> beta =
        dbos.listWorkflows(new ListWorkflowsInput().withWorkflowName("beta"));
    assertEquals(2, beta.size());

    // gamma: wf-gamma-1, wf-forked-1, wf-gamma-2 = 3
    List<WorkflowStatus> gamma =
        dbos.listWorkflows(new ListWorkflowsInput().withWorkflowName("gamma"));
    assertEquals(3, gamma.size());

    List<WorkflowStatus> none =
        dbos.listWorkflows(new ListWorkflowsInput().withWorkflowName("nonexistent"));
    assertEquals(0, none.size());
  }

  @Test
  public void testFilterByStatus() throws Exception {

    // SUCCESS: wf-alpha-1, wf-child-1, wf-alpha-2, wf-beta-1, wf-queue-3, wf-gamma-1, wf-forked-1 =
    // 7
    List<WorkflowStatus> success =
        dbos.listWorkflows(new ListWorkflowsInput().withStatus(WorkflowState.SUCCESS));
    assertEquals(7, success.size());
    success.forEach(wf -> assertEquals("SUCCESS", wf.status()));

    // ERROR: wf-alpha-3, wf-gamma-2 = 2
    List<WorkflowStatus> error =
        dbos.listWorkflows(new ListWorkflowsInput().withStatus(WorkflowState.ERROR));
    assertEquals(2, error.size());
    error.forEach(wf -> assertEquals("ERROR", wf.status()));

    // CANCELLED: wf-beta-2 = 1
    List<WorkflowStatus> cancelled =
        dbos.listWorkflows(new ListWorkflowsInput().withStatus(WorkflowState.CANCELLED));
    assertEquals(1, cancelled.size());
    assertEquals("wf-beta-2", cancelled.get(0).workflowId());

    // Multiple statuses in one filter
    List<WorkflowStatus> errorOrCancelled =
        dbos.listWorkflows(
            new ListWorkflowsInput()
                .withAddedStatus(WorkflowState.ERROR)
                .withAddedStatus(WorkflowState.CANCELLED));
    assertEquals(3, errorOrCancelled.size());
  }

  @Test
  public void testFilterByWorkflowId() throws Exception {

    // Single ID via withWorkflowId
    List<WorkflowStatus> single =
        dbos.listWorkflows(new ListWorkflowsInput().withWorkflowId("wf-alpha-1"));
    assertEquals(1, single.size());
    assertEquals("wf-alpha-1", single.get(0).workflowId());

    // Multiple IDs via withWorkflowIds
    List<WorkflowStatus> multi =
        dbos.listWorkflows(
            new ListWorkflowsInput()
                .withWorkflowIds(new String[] {"wf-alpha-1", "wf-beta-1", "wf-gamma-2"}));
    assertEquals(3, multi.size());

    // Empty list → no results
    List<WorkflowStatus> empty = dbos.listWorkflows(new ListWorkflowsInput());
    assertEquals(0, empty.size());

    // Incremental withAddedWorkflowId
    List<WorkflowStatus> added =
        dbos.listWorkflows(
            new ListWorkflowsInput()
                .withAddedWorkflowId("wf-alpha-1")
                .withAddedWorkflowId("wf-alpha-2"));
    assertEquals(2, added.size());
  }

  @Test
  public void testFilterByWorkflowIdPrefix() throws Exception {

    // wf-alpha- prefix: wf-alpha-1, wf-alpha-2, wf-alpha-3 = 3
    List<WorkflowStatus> alpha =
        dbos.listWorkflows(new ListWorkflowsInput().withWorkflowIdPrefix("wf-alpha-"));
    assertEquals(3, alpha.size());
    alpha.forEach(wf -> assertTrue(wf.workflowId().startsWith("wf-alpha-")));

    // wf- prefix: all 10
    List<WorkflowStatus> all =
        dbos.listWorkflows(new ListWorkflowsInput().withWorkflowIdPrefix("wf-"));
    assertEquals(10, all.size());

    List<WorkflowStatus> noMatch =
        dbos.listWorkflows(new ListWorkflowsInput().withWorkflowIdPrefix("no-match-"));
    assertEquals(0, noMatch.size());
  }

  @Test
  public void testFilterByClassName() throws Exception {

    // ClassA: wf-alpha-1, wf-child-1, wf-alpha-2, wf-alpha-3 = 4
    List<WorkflowStatus> classA =
        dbos.listWorkflows(new ListWorkflowsInput().withClassName("ClassA"));
    assertEquals(4, classA.size());
    classA.forEach(wf -> assertEquals("ClassA", wf.className()));

    // ClassB: wf-beta-1, wf-beta-2 = 2
    List<WorkflowStatus> classB =
        dbos.listWorkflows(new ListWorkflowsInput().withClassName("ClassB"));
    assertEquals(2, classB.size());

    // ClassC: wf-gamma-1, wf-forked-1, wf-gamma-2 = 3
    List<WorkflowStatus> classC =
        dbos.listWorkflows(new ListWorkflowsInput().withClassName("ClassC"));
    assertEquals(3, classC.size());
  }

  @Test
  public void testFilterByInstanceName() throws Exception {

    // instA: wf-alpha-1, wf-child-1, wf-alpha-2, wf-gamma-1, wf-forked-1, wf-gamma-2 = 6
    List<WorkflowStatus> instA =
        dbos.listWorkflows(new ListWorkflowsInput().withInstanceName("instA"));
    assertEquals(6, instA.size());
    instA.forEach(wf -> assertEquals("instA", wf.instanceName()));

    // instB: wf-alpha-3, wf-beta-1, wf-beta-2 = 3
    List<WorkflowStatus> instB =
        dbos.listWorkflows(new ListWorkflowsInput().withInstanceName("instB"));
    assertEquals(3, instB.size());
  }

  @Test
  public void testFilterByAuthenticatedUser() throws Exception {

    // user-a: wf-alpha-1, wf-child-1, wf-alpha-2, wf-beta-1, wf-gamma-1, wf-forked-1 = 6
    List<WorkflowStatus> userA =
        dbos.listWorkflows(new ListWorkflowsInput().withAuthenticatedUser("user-a"));
    assertEquals(6, userA.size());
    userA.forEach(wf -> assertEquals("user-a", wf.authenticatedUser()));

    // user-b: wf-alpha-3, wf-queue-3, wf-beta-2, wf-gamma-2 = 4
    List<WorkflowStatus> userB =
        dbos.listWorkflows(new ListWorkflowsInput().withAuthenticatedUser("user-b"));
    assertEquals(4, userB.size());
  }

  @Test
  public void testFilterByApplicationVersion() throws Exception {

    // v1.0: wf-alpha-1, wf-child-1, wf-alpha-2, wf-alpha-3, wf-beta-1, wf-queue-3 = 6
    List<WorkflowStatus> v1 =
        dbos.listWorkflows(new ListWorkflowsInput().withApplicationVersion("v1.0"));
    assertEquals(6, v1.size());
    v1.forEach(wf -> assertEquals("v1.0", wf.appVersion()));

    // v2.0: wf-beta-2, wf-gamma-1, wf-forked-1, wf-gamma-2 = 4
    List<WorkflowStatus> v2 =
        dbos.listWorkflows(new ListWorkflowsInput().withApplicationVersion("v2.0"));
    assertEquals(4, v2.size());
  }

  @Test
  public void testFilterByExecutorIds() throws Exception {

    // exec-1: wf-alpha-1, wf-child-1, wf-alpha-2, wf-beta-2, wf-gamma-1, wf-forked-1 = 6
    List<WorkflowStatus> exec1 =
        dbos.listWorkflows(new ListWorkflowsInput().withExecutorId("exec-1"));
    assertEquals(6, exec1.size());
    exec1.forEach(wf -> assertEquals("exec-1", wf.executorId()));

    // exec-2: wf-alpha-3, wf-beta-1, wf-queue-3, wf-gamma-2 = 4
    List<WorkflowStatus> exec2 =
        dbos.listWorkflows(new ListWorkflowsInput().withExecutorId("exec-2"));
    assertEquals(4, exec2.size());

    // Both executor IDs = all 10
    List<WorkflowStatus> both =
        dbos.listWorkflows(
            new ListWorkflowsInput().withAddedExecutorId("exec-1").withAddedExecutorId("exec-2"));
    assertEquals(10, both.size());
  }

  @Test
  public void testFilterByQueueName() throws Exception {

    // q1: wf-beta-1 = 1
    List<WorkflowStatus> q1 = dbos.listWorkflows(new ListWorkflowsInput().withQueueName("q1"));
    assertEquals(1, q1.size());
    assertEquals("wf-beta-1", q1.get(0).workflowId());
    assertEquals("q1", q1.get(0).queueName());

    // q2: wf-gamma-2 = 1
    List<WorkflowStatus> q2 = dbos.listWorkflows(new ListWorkflowsInput().withQueueName("q2"));
    assertEquals(1, q2.size());
    assertEquals("wf-gamma-2", q2.get(0).workflowId());

    // queuesOnly=true: wf-beta-1, wf-queue-3, wf-gamma-2 = 3
    List<WorkflowStatus> queuesOnly = dbos.listWorkflows(new ListWorkflowsInput().withQueuesOnly());
    assertEquals(3, queuesOnly.size());
    queuesOnly.forEach(wf -> assertNotNull(wf.queueName()));

    // Non-queued workflows have null queueName
    List<WorkflowStatus> noQueue =
        dbos.listWorkflows(new ListWorkflowsInput().withWorkflowIdPrefix("wf-alpha-"));
    assertEquals(3, noQueue.size());
    noQueue.forEach(wf -> assertNull(wf.queueName()));
  }

  @Test
  public void testFilterByParentWorkflowId() throws Exception {

    // wf-child-1 has parentWorkflowId = wf-alpha-1
    List<WorkflowStatus> children =
        dbos.listWorkflows(new ListWorkflowsInput().withParentWorkflowId("wf-alpha-1"));
    assertEquals(1, children.size());
    assertEquals("wf-child-1", children.get(0).workflowId());
    assertEquals("wf-alpha-1", children.get(0).parentWorkflowId());

    // No children for wf-gamma-1
    List<WorkflowStatus> noChildren =
        dbos.listWorkflows(new ListWorkflowsInput().withParentWorkflowId("wf-gamma-1"));
    assertEquals(0, noChildren.size());
  }

  @Test
  public void testFilterByForkedFrom() throws Exception {

    // wf-forked-1 was forked from wf-alpha-1
    List<WorkflowStatus> forked =
        dbos.listWorkflows(new ListWorkflowsInput().withForkedFrom("wf-alpha-1"));
    assertEquals(1, forked.size());
    assertEquals("wf-forked-1", forked.get(0).workflowId());
    assertEquals("wf-alpha-1", forked.get(0).forkedFrom());

    List<WorkflowStatus> notForked =
        dbos.listWorkflows(new ListWorkflowsInput().withForkedFrom("wf-beta-1"));
    assertEquals(0, notForked.size());
  }

  @Test
  public void testLimitAndOffset() throws Exception {

    // Default sort is ASC by created_at; wf-alpha-1 (+100 ms) is first
    List<WorkflowStatus> firstThree = dbos.listWorkflows(new ListWorkflowsInput().withLimit(3));
    assertEquals(3, firstThree.size());
    assertEquals("wf-alpha-1", firstThree.get(0).workflowId());

    List<WorkflowStatus> nextThree =
        dbos.listWorkflows(new ListWorkflowsInput().withLimit(3).withOffset(3));
    assertEquals(3, nextThree.size());

    // Pages must not overlap
    var firstIds = firstThree.stream().map(WorkflowStatus::workflowId).toList();
    nextThree.forEach(wf -> assertFalse(firstIds.contains(wf.workflowId())));

    // Offset past end of results returns empty
    List<WorkflowStatus> pastEnd =
        dbos.listWorkflows(new ListWorkflowsInput().withLimit(10).withOffset(100));
    assertEquals(0, pastEnd.size());
  }

  @Test
  public void testSortOrder() throws Exception {

    // Ascending: wf-alpha-1 (b+100) first, wf-gamma-2 (b+700) last
    List<WorkflowStatus> asc = dbos.listWorkflows(new ListWorkflowsInput().withSortDesc(false));
    assertEquals(10, asc.size());
    assertEquals("wf-alpha-1", asc.get(0).workflowId());
    assertEquals("wf-gamma-2", asc.get(9).workflowId());

    // Descending: wf-gamma-2 first, wf-alpha-1 last
    List<WorkflowStatus> desc = dbos.listWorkflows(new ListWorkflowsInput().withSortDesc(true));
    assertEquals(10, desc.size());
    assertEquals("wf-gamma-2", desc.get(0).workflowId());
    assertEquals("wf-alpha-1", desc.get(9).workflowId());

    // The two lists must be exact reverses of each other (all timestamps are distinct)
    for (int i = 0; i < 10; i++) {
      assertEquals(asc.get(i).workflowId(), desc.get(9 - i).workflowId());
    }
  }

  @Test
  public void testTimeRange() throws Exception {

    // created_at values: b+100, b+150, b+200, b+300, b+400, b+450, b+500, b+600, b+650, b+700
    long midMs = baseTime + 500;
    OffsetDateTime mid = OffsetDateTime.ofInstant(Instant.ofEpochMilli(midMs), ZoneOffset.UTC);

    // Rows with created_at <= mid (+500): +100,+150,+200,+300,+400,+450,+500 = 7
    List<WorkflowStatus> before = dbos.listWorkflows(new ListWorkflowsInput().withEndTime(mid));
    assertEquals(7, before.size());

    // Rows with created_at >= mid+1 (+501): +600,+650,+700 = 3
    OffsetDateTime afterMid =
        OffsetDateTime.ofInstant(Instant.ofEpochMilli(midMs + 1), ZoneOffset.UTC);
    List<WorkflowStatus> after =
        dbos.listWorkflows(new ListWorkflowsInput().withStartTime(afterMid));
    assertEquals(3, after.size());

    // All 10 within [baseTime, baseTime+800]
    OffsetDateTime start = OffsetDateTime.ofInstant(Instant.ofEpochMilli(baseTime), ZoneOffset.UTC);
    OffsetDateTime end =
        OffsetDateTime.ofInstant(Instant.ofEpochMilli(baseTime + 800), ZoneOffset.UTC);
    List<WorkflowStatus> all =
        dbos.listWorkflows(new ListWorkflowsInput().withStartTime(start).withEndTime(end));
    assertEquals(10, all.size());

    // Nothing before baseTime
    OffsetDateTime beforeBase =
        OffsetDateTime.ofInstant(Instant.ofEpochMilli(baseTime - 1), ZoneOffset.UTC);
    List<WorkflowStatus> none =
        dbos.listWorkflows(new ListWorkflowsInput().withEndTime(beforeBase));
    assertEquals(0, none.size());
  }

  @Test
  public void testLoadInputFalse() throws Exception {

    List<WorkflowStatus> wfs = dbos.listWorkflows(new ListWorkflowsInput().withLoadInput(false));
    assertEquals(10, wfs.size());

    // inputs column is not fetched; input is null
    wfs.forEach(
        wf -> {
          assertNull(wf.input());
        });

    // Core metadata fields must still be populated
    wfs.forEach(
        wf -> {
          assertNotNull(wf.workflowId());
          assertNotNull(wf.status());
          assertNotNull(wf.name());
          assertNotNull(wf.createdAt());
        });
  }

  @Test
  public void testLoadOutputFalse() throws Exception {

    List<WorkflowStatus> wfs = dbos.listWorkflows(new ListWorkflowsInput().withLoadOutput(false));
    assertEquals(10, wfs.size());

    // output and error columns are not fetched
    wfs.forEach(wf -> assertNull(wf.output()));
    wfs.forEach(wf -> assertNull(wf.error()));

    // Core metadata fields must still be populated
    wfs.forEach(
        wf -> {
          assertNotNull(wf.workflowId());
          assertNotNull(wf.status());
          assertNotNull(wf.name());
        });
  }

  /**
   * Regression test for a bug in {@code WorkflowDAO.resultsToWorkflowStatus}: when both {@code
   * loadInput} and {@code loadOutput} are explicitly {@code false}, the {@code serialization}
   * column is omitted from the SELECT clause, but the {@code WorkflowStatus} constructor call still
   * reads {@code rs.getString("serialization")} unconditionally — causing a {@code SQLException}
   * because the column is absent from the {@code ResultSet}.
   *
   * <p>The fix is to replace that final {@code rs.getString("serialization")} argument with the
   * local {@code serialization} variable that was already correctly computed to {@code null} on the
   * preceding line.
   */
  @Test
  public void testLoadInputAndOutputBothFalse() throws Exception {

    List<WorkflowStatus> wfs =
        dbos.listWorkflows(new ListWorkflowsInput().withLoadInput(false).withLoadOutput(false));

    assertEquals(10, wfs.size());

    // Payload columns are not fetched; input/output/error are null
    wfs.forEach(
        wf -> {
          assertNull(wf.input());
          assertNull(wf.output());
          assertNull(wf.error());
        });

    // Core metadata must still be correct
    wfs.forEach(
        wf -> {
          assertNotNull(wf.workflowId());
          assertNotNull(wf.status());
          assertNotNull(wf.name());
          assertNotNull(wf.className());
          assertNotNull(wf.createdAt());
        });

    // Status counts must be correct even though output/error were not loaded
    long successCount = wfs.stream().filter(wf -> "SUCCESS".equals(wf.status())).count();
    long errorCount = wfs.stream().filter(wf -> "ERROR".equals(wf.status())).count();
    long cancelledCount = wfs.stream().filter(wf -> "CANCELLED".equals(wf.status())).count();
    assertEquals(7, successCount);
    assertEquals(2, errorCount);
    assertEquals(1, cancelledCount);

    // A specific row should be findable with correct metadata
    WorkflowStatus alpha1 =
        wfs.stream()
            .filter(wf -> "wf-alpha-1".equals(wf.workflowId()))
            .findFirst()
            .orElseThrow(() -> new AssertionError("wf-alpha-1 not found"));
    assertEquals("alpha", alpha1.name());
    assertEquals("ClassA", alpha1.className());
    assertEquals("SUCCESS", alpha1.status());
    assertTrue(alpha1.input() == null || alpha1.input().length == 0);
    assertNull(alpha1.output());
    assertNull(alpha1.error());
  }

  @Test
  public void testDefaultLoadBehavior() throws Exception {
    // A null loadInput/loadOutput (the default) behaves like true for both.
    // Since we inserted NULL for inputs/output/error, deserialized values will
    // be null — but the call must not throw and must return all 10 rows.

    List<WorkflowStatus> wfs = dbos.listWorkflows(new ListWorkflowsInput());
    assertEquals(10, wfs.size());
    wfs.forEach(wf -> assertNotNull(wf.workflowId()));
  }

  /**
   * Tests that all filters accepting {@code String[]} correctly match any row whose field value is
   * in the provided array (i.e. SQL {@code = ANY(?)}).
   */
  @Test
  public void testMultiValueArrayFilters() throws Exception {

    // --- workflowName ---
    // alpha=3, beta=2 → 5
    List<WorkflowStatus> alphaOrBeta =
        dbos.listWorkflows(
            new ListWorkflowsInput().withWorkflowNames(new String[] {"alpha", "beta"}));
    assertEquals(5, alphaOrBeta.size());
    alphaOrBeta.forEach(wf -> assertTrue("alpha".equals(wf.name()) || "beta".equals(wf.name())));

    // alpha=3, beta=2, gamma=3 → 8
    List<WorkflowStatus> threenames =
        dbos.listWorkflows(
            new ListWorkflowsInput().withWorkflowNames(new String[] {"alpha", "beta", "gamma"}));
    assertEquals(8, threenames.size());

    // --- authenticatedUser ---
    // user-a=6, user-b=4 → all 10
    List<WorkflowStatus> bothUsers =
        dbos.listWorkflows(
            new ListWorkflowsInput().withAuthenticatedUsers(new String[] {"user-a", "user-b"}));
    assertEquals(10, bothUsers.size());

    // --- applicationVersion ---
    // v1.0=6, v2.0=4 → all 10
    List<WorkflowStatus> bothVersions =
        dbos.listWorkflows(
            new ListWorkflowsInput().withApplicationVersions(new String[] {"v1.0", "v2.0"}));
    assertEquals(10, bothVersions.size());

    // v1.0 only → 6
    List<WorkflowStatus> v1only =
        dbos.listWorkflows(new ListWorkflowsInput().withApplicationVersions(new String[] {"v1.0"}));
    assertEquals(6, v1only.size());
    v1only.forEach(wf -> assertEquals("v1.0", wf.appVersion()));

    // --- queueName ---
    // q1=1 (wf-beta-1), q2=1 (wf-gamma-2) → 2
    List<WorkflowStatus> q1orq2 =
        dbos.listWorkflows(new ListWorkflowsInput().withQueueNames(new String[] {"q1", "q2"}));
    assertEquals(2, q1orq2.size());
    q1orq2.forEach(wf -> assertTrue("q1".equals(wf.queueName()) || "q2".equals(wf.queueName())));

    // q1 + q2 + q3 → 3 (all queued workflows)
    List<WorkflowStatus> allQueues =
        dbos.listWorkflows(
            new ListWorkflowsInput().withQueueNames(new String[] {"q1", "q2", "q3"}));
    assertEquals(3, allQueues.size());

    // --- status ---
    // SUCCESS=7, CANCELLED=1 → 8
    List<WorkflowStatus> successOrCancelled =
        dbos.listWorkflows(
            new ListWorkflowsInput().withStatuses(new String[] {"SUCCESS", "CANCELLED"}));
    assertEquals(8, successOrCancelled.size());
    successOrCancelled.forEach(
        wf -> assertTrue("SUCCESS".equals(wf.status()) || "CANCELLED".equals(wf.status())));

    // --- forkedFrom ---
    // wf-forked-1 forked from wf-alpha-1; no workflow forked from wf-beta-1
    // Passing both still returns only wf-forked-1
    List<WorkflowStatus> forkedFromMulti =
        dbos.listWorkflows(
            new ListWorkflowsInput().withForkedFrom(new String[] {"wf-alpha-1", "wf-beta-1"}));
    assertEquals(1, forkedFromMulti.size());
    assertEquals("wf-forked-1", forkedFromMulti.get(0).workflowId());

    // --- parentWorkflowId ---
    // wf-child-1 has parent=wf-alpha-1; no workflow has parent=wf-gamma-1
    List<WorkflowStatus> parentMulti =
        dbos.listWorkflows(
            new ListWorkflowsInput()
                .withParentWorkflowIds(new String[] {"wf-alpha-1", "wf-gamma-1"}));
    assertEquals(1, parentMulti.size());
    assertEquals("wf-child-1", parentMulti.get(0).workflowId());
  }

  @Test
  public void testCombinedFilters() throws Exception {

    // name=alpha + status=SUCCESS → wf-alpha-1, wf-alpha-2 = 2
    List<WorkflowStatus> alphaSuccess =
        dbos.listWorkflows(
            new ListWorkflowsInput().withWorkflowName("alpha").withStatus(WorkflowState.SUCCESS));
    assertEquals(2, alphaSuccess.size());
    alphaSuccess.forEach(
        wf -> {
          assertEquals("alpha", wf.name());
          assertEquals("SUCCESS", wf.status());
        });

    // className=ClassC + appVersion=v2.0 → wf-gamma-1, wf-forked-1, wf-gamma-2 = 3
    List<WorkflowStatus> classCv2 =
        dbos.listWorkflows(
            new ListWorkflowsInput().withClassName("ClassC").withApplicationVersion("v2.0"));
    assertEquals(3, classCv2.size());

    // prefix=wf-alpha- + loadInput=false + loadOutput=false exercises the bug path with a filter
    List<WorkflowStatus> alphaNoPayload =
        dbos.listWorkflows(
            new ListWorkflowsInput()
                .withWorkflowIdPrefix("wf-alpha-")
                .withLoadInput(false)
                .withLoadOutput(false));
    assertEquals(3, alphaNoPayload.size());
    alphaNoPayload.forEach(
        wf -> {
          assertTrue(wf.workflowId().startsWith("wf-alpha-"));
          assertTrue(wf.input() == null || wf.input().length == 0);
          assertNull(wf.output());
          assertNull(wf.error());
        });

    // queuesOnly + status=SUCCESS → wf-beta-1, wf-queue-3 = 2
    List<WorkflowStatus> queuedSuccess =
        dbos.listWorkflows(
            new ListWorkflowsInput().withQueuesOnly().withStatus(WorkflowState.SUCCESS));
    assertEquals(2, queuedSuccess.size());
    queuedSuccess.forEach(
        wf -> {
          assertNotNull(wf.queueName());
          assertEquals("SUCCESS", wf.status());
        });

    // limit=2 on a sorted result
    List<WorkflowStatus> limited =
        dbos.listWorkflows(new ListWorkflowsInput().withWorkflowIdPrefix("wf-").withLimit(2));
    assertEquals(2, limited.size());
  }
}
