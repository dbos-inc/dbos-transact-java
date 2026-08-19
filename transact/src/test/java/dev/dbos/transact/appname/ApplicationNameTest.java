package dev.dbos.transact.appname;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.Constants;
import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSClient;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.exceptions.DBOSApplicationNameConflictException;
import dev.dbos.transact.exceptions.DBOSQueueDuplicatedException;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.Queue;
import dev.dbos.transact.workflow.QueueOptions;
import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.WorkflowSchedule;
import dev.dbos.transact.workflow.WorkflowState;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

interface AppNameService {
  String greet(String name);
}

class AppNameServiceImpl implements AppNameService {
  private final DBOS dbos;

  AppNameServiceImpl(DBOS dbos) {
    this.dbos = dbos;
  }

  @Workflow
  public String greet(String name) {
    return dbos.runStep(() -> "hello " + name, "greetStep");
  }
}

/**
 * Two applications sharing one system database. Rows are isolated by the application that owns
 * them, unclaimed rows -- written before this feature, or by an SDK that does not know the column
 * -- belong to everyone, and the names applications register objects under are shared address
 * space, so a collision raises.
 */
public class ApplicationNameTest {

  private static final String APP_A = "app-a";
  private static final String APP_B = "app-b";

  // One container, so both applications share a system database.
  @AutoClose final PgContainer pgContainer = new PgContainer();
  @AutoClose HikariDataSource dataSource;

  @AutoClose DBOS dbosA;
  @AutoClose DBOS dbosB;
  AppNameService serviceA;
  AppNameService serviceB;

  @BeforeEach
  void beforeEach() {
    dataSource = pgContainer.dataSource();

    dbosA = new DBOS(pgContainer.dbosConfig(APP_A));
    serviceA = dbosA.registerProxy(AppNameService.class, new AppNameServiceImpl(dbosA));
    dbosA.registerQueue(new Queue("queue-a"));
    dbosA.launch();

    dbosB = new DBOS(pgContainer.dbosConfig(APP_B));
    serviceB = dbosB.registerProxy(AppNameService.class, new AppNameServiceImpl(dbosB));
    dbosB.registerQueue(new Queue("queue-b"));
    dbosB.launch();
  }

  private String workflowOwner(String workflowId) throws SQLException {
    return scalar(
        "SELECT application_name FROM \"dbos\".workflow_status WHERE workflow_uuid = ?",
        workflowId);
  }

  private String stepOwner(String workflowId) throws SQLException {
    return scalar(
        "SELECT application_name FROM \"dbos\".operation_outputs WHERE workflow_uuid = ? ORDER BY function_id",
        workflowId);
  }

  private String scalar(String sql, String arg) throws SQLException {
    try (var conn = dataSource.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, arg);
      try (var rs = stmt.executeQuery()) {
        return rs.next() ? rs.getString(1) : null;
      }
    }
  }

  private String runIn(AppNameService service, String name) {
    var workflowId = UUID.randomUUID().toString();
    try (var o = new WorkflowOptions(workflowId).setContext()) {
      service.greet(name);
    }
    return workflowId;
  }

  private static List<String> idsOf(List<dev.dbos.transact.workflow.WorkflowStatus> statuses) {
    return statuses.stream().map(dev.dbos.transact.workflow.WorkflowStatus::workflowId).toList();
  }

  @Test
  void stampsWorkflowsAndStepsWithTheOwningApplication() throws Exception {
    var idA = runIn(serviceA, "a");
    var idB = runIn(serviceB, "b");

    assertEquals(APP_A, workflowOwner(idA));
    assertEquals(APP_A, stepOwner(idA));
    assertEquals(APP_B, workflowOwner(idB));
    assertEquals(APP_B, stepOwner(idB));
  }

  @Test
  void forkingClaimsTheForkForTheForkingApplication() throws Exception {
    var idB = runIn(serviceB, "b");

    // Forking is addressable across applications -- a workflow ID is global -- but the fork is the
    // forking application's own work, steps included.
    var forked = dbosA.forkWorkflow(idB, 1);
    assertEquals(APP_A, workflowOwner(forked.workflowId()));
    assertEquals(APP_A, stepOwner(forked.workflowId()));
    assertEquals(APP_B, workflowOwner(idB));
  }

  @Test
  void stepsCarryTheirOwnOwnerThroughExportAndImport() throws Exception {
    var idA = runIn(serviceA, "a");
    var exported = DBOSTestAccess.getSystemDatabase(dbosA).exportWorkflow(idA, false);

    assertEquals(1, exported.size());
    assertEquals(APP_A, exported.get(0).status().applicationName());
    assertEquals(
        List.of(APP_A),
        exported.get(0).steps().stream()
            .map(dev.dbos.transact.workflow.StepInfo::applicationName)
            .distinct()
            .toList());

    // Importing through a peer restores the owners the export carried, not the importer's.
    DBOSTestAccess.getSystemDatabase(dbosA).deleteWorkflows(List.of(idA), false);
    DBOSTestAccess.getSystemDatabase(dbosB).importWorkflow(exported);

    assertEquals(APP_A, workflowOwner(idA));
    assertEquals(APP_A, stepOwner(idA));
  }

  @Test
  void listingsCoverOnlyTheirOwnApplication() {
    var idA = runIn(serviceA, "a");
    var idB = runIn(serviceB, "b");

    assertEquals(List.of(idA), idsOf(dbosA.listWorkflows(new ListWorkflowsInput())));
    assertEquals(List.of(idB), idsOf(dbosB.listWorkflows(new ListWorkflowsInput())));
    assertEquals(APP_A, dbosA.listWorkflows(new ListWorkflowsInput()).get(0).applicationName());
  }

  @Test
  void anExplicitFilterReachesAnotherApplication() {
    var idA = runIn(serviceA, "a");
    var idB = runIn(serviceB, "b");

    assertEquals(
        List.of(idB),
        idsOf(dbosA.listWorkflows(new ListWorkflowsInput().withApplicationName(APP_B))));
    // An empty filter is unscoped: every application's rows.
    assertEquals(
        java.util.Set.of(idA, idB),
        java.util.Set.copyOf(
            idsOf(dbosA.listWorkflows(new ListWorkflowsInput().withApplicationName(List.of())))));
  }

  @Test
  void unclaimedWorkflowsBelongToEveryApplication() throws Exception {
    var idB = runIn(serviceB, "b");
    // A row written before this feature, or by an SDK that does not know the column.
    try (var conn = dataSource.getConnection();
        var stmt =
            conn.prepareStatement(
                "UPDATE \"dbos\".workflow_status SET application_name = NULL WHERE workflow_uuid = ?")) {
      stmt.setString(1, idB);
      stmt.executeUpdate();
    }

    assertEquals(List.of(idB), idsOf(dbosA.listWorkflows(new ListWorkflowsInput())));
    assertEquals(List.of(idB), idsOf(dbosB.listWorkflows(new ListWorkflowsInput())));
  }

  @Test
  void queuesAreOwnedByTheApplicationThatRegistersThem() {
    dbosA.registerQueue("dynamic-a", QueueOptions.empty());
    dbosB.registerQueue("dynamic-b", QueueOptions.empty());

    var listedByA = dbosA.listQueues().stream().map(Queue::name).toList();
    var listedByB = dbosB.listQueues().stream().map(Queue::name).toList();
    assertEquals(List.of("dynamic-a"), listedByA);
    assertEquals(List.of("dynamic-b"), listedByB);
    assertEquals(APP_A, dbosA.listQueues().stream().findFirst().orElseThrow().applicationName());
  }

  @Test
  void registeringAQueueAnotherApplicationOwnsRaises() {
    dbosA.registerQueue("contested", QueueOptions.empty());

    var conflict =
        assertThrows(
            DBOSApplicationNameConflictException.class,
            () -> dbosB.registerQueue("contested", QueueOptions.empty()));
    assertEquals(APP_A, conflict.owner());
    assertTrue(conflict.getMessage().contains("contested"));
  }

  @Test
  void schedulesAreOwnedByTheApplicationThatCreatesThem() {
    dbosA.createSchedule(
        new WorkflowSchedule(
            "sched-a", "greet", AppNameServiceImpl.class.getName(), "0 0 * * * *"));

    var listedByA = dbosA.listSchedules(null, null, null);
    assertEquals(1, listedByA.size());
    assertEquals(APP_A, listedByA.get(0).applicationName());
    assertTrue(dbosB.listSchedules(null, null, null).isEmpty());
  }

  @Test
  void creatingAScheduleAnotherApplicationOwnsRaises() {
    dbosA.createSchedule(
        new WorkflowSchedule(
            "contested", "greet", AppNameServiceImpl.class.getName(), "0 0 * * * *"));

    var conflict =
        assertThrows(
            DBOSApplicationNameConflictException.class,
            () ->
                dbosB.createSchedule(
                    new WorkflowSchedule(
                        "contested", "greet", AppNameServiceImpl.class.getName(), "0 0 * * * *")));
    assertEquals(APP_A, conflict.owner());
  }

  @Test
  void applicationVersionsAreScopedToTheirApplication() {
    var versionsA = DBOSTestAccess.getSystemDatabase(dbosA).listApplicationVersions();
    var versionsB = DBOSTestAccess.getSystemDatabase(dbosB).listApplicationVersions();

    assertEquals(1, versionsA.size());
    assertEquals(APP_A, versionsA.get(0).applicationName());
    assertEquals(1, versionsB.size());
    assertEquals(APP_B, versionsB.get(0).applicationName());
    assertEquals(
        versionsA.get(0).versionName(),
        DBOSTestAccess.getSystemDatabase(dbosA).getLatestApplicationVersion().versionName());
  }

  @Test
  void theApplicationNameIsPartOfTheComputedVersion() {
    // Same workflows, same jar: only the name separates these two versions.
    assertNotEquals(
        DBOSTestAccess.getDbosExecutor(dbosA).appVersion(),
        DBOSTestAccess.getDbosExecutor(dbosB).appVersion());
  }

  @Test
  void pinningOneVersionNameToTwoApplicationsRaises() {
    try (var pinnedA = new DBOS(pgContainer.dbosConfig("pinned-a").withAppVersion("shared-v1"))) {
      pinnedA.registerProxy(AppNameService.class, new AppNameServiceImpl(pinnedA));
      pinnedA.launch();

      try (var pinnedB = new DBOS(pgContainer.dbosConfig("pinned-b").withAppVersion("shared-v1"))) {
        pinnedB.registerProxy(AppNameService.class, new AppNameServiceImpl(pinnedB));
        var conflict = assertThrows(DBOSApplicationNameConflictException.class, pinnedB::launch);
        assertEquals("pinned-a", conflict.owner());
      }
    }
  }

  @Test
  void workflowsAreNotDequeuedAcrossApplications() throws Exception {
    // A peer application enqueues onto a queue this application polls. The row is addressable --
    // the queue name is shared -- but it is not this application's work to run.
    String foreignId;
    try (var client =
        new DBOSClient(
            pgContainer.jdbcUrl(),
            pgContainer.username(),
            pgContainer.password(),
            null,
            null,
            true,
            APP_B)) {
      var options = new DBOSClient.EnqueueOptions("greet", "queue-a");
      foreignId = client.enqueuePortableWorkflow(options, new Object[] {"peer"}, null).workflowId();
    }

    var ownId = UUID.randomUUID().toString();
    try (var o = new WorkflowOptions(ownId).setContext()) {
      dbosA
          .startWorkflow(() -> serviceA.greet("own"), new dev.dbos.transact.StartWorkflowOptions())
          .getResult();
    }

    // Give the queue service several polling intervals to prove it never takes the peer's row.
    Thread.sleep(Duration.ofSeconds(2).toMillis());

    assertEquals(APP_B, workflowOwner(foreignId));
    var foreign =
        dbosA.listWorkflows(new ListWorkflowsInput().withApplicationName(APP_B)).stream()
            .filter(w -> w.workflowId().equals(foreignId))
            .findFirst()
            .orElseThrow();
    assertEquals(WorkflowState.ENQUEUED, foreign.status());
  }

  @Test
  void aNamelessClientOwnsNothingAndSeesEveryApplication() throws Exception {
    var idA = runIn(serviceA, "a");
    var idB = runIn(serviceB, "b");

    try (var client = pgContainer.dbosClient()) {
      var seen = idsOf(client.listWorkflows(new ListWorkflowsInput()));
      assertTrue(seen.contains(idA));
      assertTrue(seen.contains(idB));

      var options = new DBOSClient.EnqueueOptions("greet", "queue-b");
      var handle = client.enqueuePortableWorkflow(options, new Object[] {"nameless"}, null);
      assertNull(workflowOwner(handle.workflowId()));
    }
  }

  // ==================== Debouncing ====================

  /**
   * The deduplication index is global across the applications sharing the system database, so a
   * peer can hold the key this debouncer wants. That holder dequeues on the peer's account: this
   * application can neither run it nor extend it, and waiting for it to acknowledge a message would
   * spin forever. Mirrors the foreign-holder arm of Python's _classify_bounce.
   */
  @Test
  void debouncingRefusesADeduplicationKeyHeldByAPeer() throws Exception {
    var dedupId = "greet-shared-key";

    try (var peer = new DBOSClient(dataSource, null, null, APP_B)) {
      peer.enqueueWorkflow(
          new DBOSClient.EnqueueOptions("greet", Constants.DBOS_INTERNAL_QUEUE)
              .withClassName(AppNameServiceImpl.class.getName())
              .withWorkflowId("wf-db-holder")
              .withDeduplicationId(dedupId),
          new Object[] {"theirs"});
    }

    var debouncer = dbosA.<String>debouncer();
    var e =
        assertThrows(
            DBOSQueueDuplicatedException.class,
            () ->
                debouncer.debounce("shared-key", Duration.ofMillis(50), () -> serviceA.greet("m")));
    assertEquals(Constants.DBOS_INTERNAL_QUEUE, e.queueName());
    assertEquals(dedupId, e.deduplicationId());
  }

  @Test
  void aClientDebouncerRefusesADeduplicationKeyHeldByAPeer() throws Exception {
    var dedupId = "greet-client-shared-key";

    try (var peer = new DBOSClient(dataSource, null, null, APP_B)) {
      peer.enqueueWorkflow(
          new DBOSClient.EnqueueOptions("greet", Constants.DBOS_INTERNAL_QUEUE)
              .withClassName(AppNameServiceImpl.class.getName())
              .withWorkflowId("wf-db-client-holder")
              .withDeduplicationId(dedupId),
          new Object[] {"theirs"});
    }

    try (var mine = new DBOSClient(dataSource, null, null, APP_A)) {
      var debouncer =
          mine.<String>debouncer("greet").withClassName(AppNameServiceImpl.class.getName());
      var e =
          assertThrows(
              DBOSQueueDuplicatedException.class,
              () -> debouncer.debounce("client-shared-key", Duration.ofMillis(50), "mine"));
      assertEquals(dedupId, e.deduplicationId());
    }
  }

  // ==================== Transaction and identity ====================

  @Test
  void applySchedulesLeavesNothingBehindWhenANameIsContested() throws Exception {
    var mine =
        new WorkflowSchedule("batch-mine", "greet", AppNameService.class.getName(), "0 * * * * *");
    var contested =
        new WorkflowSchedule(
            "batch-contested", "greet", AppNameService.class.getName(), "0 * * * * *");

    try (var peer = new DBOSClient(dataSource, null, null, APP_B)) {
      peer.createSchedule(contested);
    }
    try (var client = new DBOSClient(dataSource, null, null, APP_A)) {
      assertThrows(
          DBOSApplicationNameConflictException.class, () -> client.applySchedules(mine, contested));
      // The batch is one transaction, and the conflict is not a SQLException: the schedule written
      // before it must still be rolled back.
      assertTrue(client.listSchedules(null, null, null).isEmpty());
      // The connection it borrowed is left fit to use.
      client.applySchedules(mine);
      assertEquals(1, client.listSchedules(null, null, null).size());
    }
  }

  @Test
  void theSystemDatabaseTakesItsApplicationFromTheExecutor() {
    // On DBOS Cloud the executor's name comes from DBOS_APP_NAME rather than from the config, and
    // ownership must be the same identity that the application version hashes and that peer
    // checks compare against -- so the caller supplies it, rather than the config deciding twice.
    try (var sysdb =
        SystemDatabase.create(pgContainer.dbosConfig("configured-app"), "exec-1", "resolved-app")) {
      assertEquals("resolved-app", sysdb.applicationName());
    }
  }
}
