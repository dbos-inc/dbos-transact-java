package dev.dbos.transact.appname;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
import dev.dbos.transact.workflow.QueueConflictResolution;
import dev.dbos.transact.workflow.QueueOptions;
import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.WorkflowHandle;
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

  String enqueueGreet(
      String workflowName, String className, String queueName, String childId, String arg);
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

  @Override
  @Workflow
  public String enqueueGreet(
      String workflowName, String className, String queueName, String childId, String arg) {
    dbos.enqueueWorkflow(
        new DBOSClient.EnqueueOptions(workflowName, queueName)
            .withClassName(className)
            .withWorkflowId(childId),
        new Object[] {arg});
    return childId;
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

  private String workflowAppName(String workflowId) throws SQLException {
    return scalar(
        "SELECT application_name FROM \"dbos\".workflow_status WHERE workflow_uuid = ?",
        workflowId);
  }

  private String stepAppName(String workflowId) throws SQLException {
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

    assertEquals(APP_A, workflowAppName(idA));
    assertEquals(APP_A, stepAppName(idA));
    assertEquals(APP_B, workflowAppName(idB));
    assertEquals(APP_B, stepAppName(idB));
  }

  @Test
  void forkingInheritsTheSourceApplicationsOwnership() throws Exception {
    var idB = runIn(serviceB, "b");

    // Forking is addressable across applications -- a workflow ID is global -- but the fork is the
    // source application's work, steps included: it is that application's own workflow re-run, and
    // it must land where the application that implements it will pick it up.
    var forked = dbosA.forkWorkflow(idB, 1);
    assertEquals(APP_B, workflowAppName(forked.workflowId()));
    assertEquals(APP_B, stepAppName(forked.workflowId()));
    assertEquals(APP_B, workflowAppName(idB));
  }

  @Test
  void forkingAnUnclaimedWorkflowClaimsItForTheForkingApplication() throws Exception {
    var idB = runIn(serviceB, "b");
    // A row written before this feature, or by an SDK that does not know the column.
    try (var conn = dataSource.getConnection();
        var stmt =
            conn.prepareStatement(
                "UPDATE \"dbos\".workflow_status SET application_name = NULL WHERE workflow_uuid = ?")) {
      stmt.setString(1, idB);
      stmt.executeUpdate();
    }

    // Nothing owns the source, so the fork is the forking application's, as dequeue does.
    var forked = dbosA.forkWorkflow(idB, 1);
    assertEquals(APP_A, workflowAppName(forked.workflowId()));
    assertEquals(APP_A, stepAppName(forked.workflowId()));
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

    assertEquals(APP_A, workflowAppName(idA));
    assertEquals(APP_A, stepAppName(idA));
  }

  @Test
  void stepsExportedWithoutAnOwnerImportUnclaimed() throws Exception {
    var idA = runIn(serviceA, "a");
    var exported = DBOSTestAccess.getSystemDatabase(dbosA).exportWorkflow(idA, false);
    var original = exported.get(0);

    // An export produced before this column existed carries no ownership for its steps. Importing
    // one must not invent an owner from the workflow: unclaimed is the honest answer, and it is
    // what Python and TypeScript write.
    var ownerless =
        new dev.dbos.transact.workflow.ExportedWorkflow(
            original.status(),
            original.steps().stream()
                .map(
                    step ->
                        new dev.dbos.transact.workflow.StepInfo(
                            step.functionId(),
                            step.functionName(),
                            step.output(),
                            step.error(),
                            step.childWorkflowId(),
                            step.startedAt(),
                            step.completedAt(),
                            step.serialization(),
                            null))
                .toList(),
            original.events(),
            original.eventHistory(),
            original.streams());

    DBOSTestAccess.getSystemDatabase(dbosA).deleteWorkflows(List.of(idA), false);
    DBOSTestAccess.getSystemDatabase(dbosA).importWorkflow(List.of(ownerless));

    assertEquals(APP_A, workflowAppName(idA));
    assertNull(stepAppName(idA));
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

  /**
   * A workflow ID is a global address, so a listing that names IDs is an identity read: it answers
   * for every application rather than quietly narrowing to this one. It is how Conductor's
   * get-workflow reaches a peer's workflow, and how an application follows up on work it handed to
   * one. Mirrors the {@code workflow_ids} arm of Python's list_workflows and TypeScript's {@code
   * idKeyed}.
   */
  @Test
  void listingByIdReachesAnotherApplication() {
    var idB = runIn(serviceB, "b");

    assertEquals(
        List.of(idB),
        idsOf(dbosA.listWorkflows(new ListWorkflowsInput().withWorkflowIds(List.of(idB)))));
  }

  /**
   * The carve-out lifts the default, not an explicit filter: naming an application still narrows.
   */
  @Test
  void listingByIdStillHonoursAnExplicitFilter() {
    var idA = runIn(serviceA, "a");
    var idB = runIn(serviceB, "b");
    var bothIds = List.of(idA, idB);

    assertEquals(
        List.of(idA),
        idsOf(
            dbosA.listWorkflows(
                new ListWorkflowsInput().withWorkflowIds(bothIds).withApplicationName(APP_A))));
    assertEquals(
        java.util.Set.of(idA, idB),
        java.util.Set.copyOf(
            idsOf(dbosA.listWorkflows(new ListWorkflowsInput().withWorkflowIds(bothIds)))));
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

  /**
   * The write side of naming a peer. A queue is registered for the application that will poll it,
   * so a deploy tool acting for several applications can set each one up without running as it.
   */
  @Test
  void registeringAQueueForAPeerGivesThatPeerTheQueue() throws Exception {
    try (var client = pgContainer.dbosClient(APP_A)) {
      client.registerQueue(
          "peer-queue", QueueOptions.empty(), QueueConflictResolution.ALWAYS_UPDATE, APP_B);
    }

    assertEquals(List.of("peer-queue"), dbosB.listQueues().stream().map(Queue::name).toList());
    assertTrue(dbosA.listQueues().isEmpty());
  }

  /** The same for schedules, where the owner rides on the record rather than on an argument. */
  @Test
  void creatingAScheduleForAPeerGivesThatPeerTheSchedule() {
    dbosA.createSchedule(
        new WorkflowSchedule(
                "peer-sched", "greet", AppNameServiceImpl.class.getName(), "0 0 * * * *")
            .withApplicationName(APP_B));

    var listedByB = dbosB.listSchedules(null, null, null);
    assertEquals(1, listedByB.size());
    assertEquals(APP_B, listedByB.get(0).applicationName());
    assertTrue(dbosA.listSchedules(null, null, null).isEmpty());
  }

  /** Acting for a peer is naming it, not impersonating it: a third application still collides. */
  @Test
  void registeringAQueueForAPeerStillCollidesWithAThirdApplication() throws Exception {
    dbosA.registerQueue("contested", QueueOptions.empty());

    try (var client = pgContainer.dbosClient(APP_B)) {
      var conflict =
          assertThrows(
              DBOSApplicationNameConflictException.class,
              () ->
                  client.registerQueue(
                      "contested",
                      QueueOptions.empty(),
                      QueueConflictResolution.ALWAYS_UPDATE,
                      APP_B));
      assertEquals(APP_A, conflict.owner());
    }
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

    assertEquals(APP_B, workflowAppName(foreignId));
    var foreign =
        dbosA.listWorkflows(new ListWorkflowsInput().withApplicationName(APP_B)).stream()
            .filter(w -> w.workflowId().equals(foreignId))
            .findFirst()
            .orElseThrow();
    assertEquals(WorkflowState.ENQUEUED, foreign.status());
  }

  /**
   * Dequeue claims as it takes. A row nobody owns -- written before this feature, or by a nameless
   * client -- would otherwise sit PENDING and unclaimed, still inside every peer's recovery and
   * global-timeout sweeps, until the executor that took it got as far as starting it. The claim
   * belongs on the statement that flips the status, which is where Python and TypeScript put it.
   */
  @Test
  void dequeuingAnUnclaimedWorkflowClaimsIt() throws Exception {
    // A queue neither application polls, so the only dequeue is the one this test drives.
    var queueName = "unpolled-queue";
    String unclaimedId;
    try (var client = pgContainer.dbosClient()) {
      unclaimedId =
          client
              .enqueuePortableWorkflow(
                  new DBOSClient.EnqueueOptions("greet", queueName), new Object[] {"nobody"}, null)
              .workflowId();
    }
    assertNull(workflowAppName(unclaimedId));

    var appVersion = DBOSTestAccess.getDbosExecutor(dbosA).appVersion();
    var dequeued =
        DBOSTestAccess.getSystemDatabase(dbosA)
            .startQueuedWorkflows(new Queue(queueName), "exec-a", appVersion, null, 0);

    assertEquals(List.of(unclaimedId), dequeued);
    assertEquals(APP_A, workflowAppName(unclaimedId));
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
      assertNull(workflowAppName(handle.workflowId()));
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

  // ==================== Enqueue by name ====================

  @Test
  void enqueueByNameWithNoTimeoutInheritsTheParentsTimeout() throws Exception {
    try (var o =
        new WorkflowOptions("wf-dl-parent").withTimeout(Duration.ofMinutes(5)).setContext()) {
      serviceA.enqueueGreet(
          "greet", AppNameServiceImpl.class.getName(), "queue-a", "wf-dl-child", "x");
    }

    // An unset timeout must fall through to what the parent propagates. Treating it as an explicit
    // "no timeout" instead would leave the child unbounded. A queued workflow carries the timeout
    // rather than a deadline, since its clock only starts when it is dequeued.
    var parentTimeout = timeoutMs("wf-dl-parent");
    assertNotNull(parentTimeout);
    assertEquals(parentTimeout, timeoutMs("wf-dl-child"));
  }

  /**
   * The one place applications deliberately interoperate. Naming a peer hands it the row: the
   * enqueue stamps that peer as the owner rather than the enqueuer, which is what makes the row
   * visible to the peer's dequeue predicate and invisible to this one's.
   */
  @Test
  void enqueuingForAPeerLetsThatPeerRunIt() throws Exception {
    var childId = UUID.randomUUID().toString();

    WorkflowHandle<String, RuntimeException> handle =
        dbosA.enqueueWorkflow(
            new DBOSClient.EnqueueOptions("greet", "queue-b")
                .withClassName(AppNameServiceImpl.class.getName())
                .withWorkflowId(childId)
                .withApplicationName(APP_B),
            new Object[] {"peer"});

    assertEquals("hello peer", handle.getResult());
    // Owned by B throughout: A wrote the row but never claimed it, and B's dequeue found it
    // because it did not.
    assertEquals(APP_B, workflowAppName(childId));
    assertEquals(APP_B, stepAppName(childId));
    // A enqueued it and still does not see it in its own listing.
    assertTrue(idsOf(dbosA.listWorkflows(new ListWorkflowsInput())).isEmpty());
    assertEquals(List.of(childId), idsOf(dbosB.listWorkflows(new ListWorkflowsInput())));
  }

  /** Unnamed, the enqueue belongs to the enqueueing application, as every other write does. */
  @Test
  void enqueuingWithoutNamingAnApplicationKeepsTheEnqueuersOwn() throws Exception {
    var childId = UUID.randomUUID().toString();

    dbosA.enqueueWorkflow(
        new DBOSClient.EnqueueOptions("greet", "queue-a")
            .withClassName(AppNameServiceImpl.class.getName())
            .withWorkflowId(childId),
        new Object[] {"own"});

    assertEquals(APP_A, workflowAppName(childId));
  }

  // ==================== Rename ====================

  /**
   * The escape hatch the conflict error names. An application that is renamed leaves its rows
   * behind under the old name, where nothing it now runs can see them, so re-owning them is the
   * only way back.
   */
  @Test
  void renamingAnApplicationMovesEveryTableItOwns() throws Exception {
    dbosA.registerQueue("renamed-queue", QueueOptions.empty());
    dbosA.createSchedule(
        new WorkflowSchedule(
            "renamed-sched", "greet", AppNameServiceImpl.class.getName(), "0 0 * * * *"));
    var idA = runIn(serviceA, "a");
    var idB = runIn(serviceB, "b");

    try (var client = pgContainer.dbosClient()) {
      var moved = client.renameApplication(APP_A, "app-c", null, false);

      assertEquals(1, moved.queues());
      assertEquals(1, moved.schedules());
      assertEquals(1, moved.workflows());
      assertEquals(1, moved.steps());
      // The version A registered at launch.
      assertEquals(1, moved.versions());
    }

    assertEquals("app-c", workflowAppName(idA));
    assertEquals("app-c", stepAppName(idA));
    // A peer's rows are untouched.
    assertEquals(APP_B, workflowAppName(idB));
  }

  /** Batching is a resumption strategy, not a different result: every matching row still moves. */
  @Test
  void renamingInBatchesMovesEveryRow() throws Exception {
    var ids = new java.util.ArrayList<String>();
    for (int i = 0; i < 5; i++) {
      ids.add(runIn(serviceA, "a" + i));
    }

    try (var client = pgContainer.dbosClient()) {
      // A batch size well below the row count, so the watermark advances several times and the
      // final partial batch has to drop it.
      var moved = client.renameApplication(APP_A, "app-c", 2, false);
      assertEquals(ids.size(), moved.workflows());
    }

    for (var id : ids) {
      assertEquals("app-c", workflowAppName(id));
    }
  }

  /** Adopting is the upgrade path: rows written before the column belong to whoever claims them. */
  @Test
  void adoptingUnclaimedRowsTakesOnlyThem() throws Exception {
    var idA = runIn(serviceA, "a");
    var idB = runIn(serviceB, "b");
    try (var conn = dataSource.getConnection();
        var stmt =
            conn.prepareStatement(
                "UPDATE \"dbos\".workflow_status SET application_name = NULL WHERE workflow_uuid = ?")) {
      stmt.setString(1, idB);
      stmt.executeUpdate();
    }

    try (var client = pgContainer.dbosClient()) {
      client.renameApplication(null, "app-c", null, true);
    }

    assertEquals("app-c", workflowAppName(idB));
    // Named rows are not swept up: adopting is not renaming.
    assertEquals(APP_A, workflowAppName(idA));
  }

  @Test
  void aRenameThatWouldMoveNothingIsRejected() throws Exception {
    try (var client = pgContainer.dbosClient()) {
      // Neither a source application nor unclaimed rows: nothing to re-own.
      assertThrows(
          IllegalArgumentException.class,
          () -> client.renameApplication(null, "app-c", null, false));
      assertThrows(
          IllegalArgumentException.class,
          () -> client.renameApplication(APP_A, APP_A, null, false));
      // A name is still required; only its shape is not.
      assertThrows(
          IllegalArgumentException.class, () -> client.renameApplication(APP_A, "", null, false));
      assertThrows(
          IllegalArgumentException.class, () -> client.renameApplication(APP_A, "app-c", 0, false));
    }
  }

  /**
   * Conductor and Cloud accept only {@code ^[a-z0-9-_]{3,30}$} at registration, but nothing in
   * Transact does: the column is TEXT and the value is always a bound parameter. A self-hosted
   * application that never registers can hold any name, so a rename onto one warns rather than
   * failing -- and the rows really move.
   */
  @Test
  void renamingOntoANameConductorWouldRejectStillMovesTheRows() throws Exception {
    var idA = runIn(serviceA, "a");

    try (var client = pgContainer.dbosClient()) {
      var moved = client.renameApplication(APP_A, "App-C", null, false);
      assertEquals(1, moved.workflows());
    }

    assertEquals("App-C", workflowAppName(idA));
    assertEquals("App-C", stepAppName(idA));
  }

  private String timeoutMs(String workflowId) throws SQLException {
    return scalar(
        "SELECT workflow_timeout_ms FROM \"dbos\".workflow_status WHERE workflow_uuid = ?",
        workflowId);
  }
}
