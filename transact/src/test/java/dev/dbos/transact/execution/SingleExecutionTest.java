package dev.dbos.transact.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.workflow.Step;
import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.WorkflowHandle;
import dev.dbos.transact.workflow.WorkflowState;

import java.sql.SQLException;
import java.util.UUID;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SingleExecutionTest {
  public static interface TryConcExecIfc {
    public void testConcStep() throws InterruptedException;

    public void testConcWorkflow() throws InterruptedException;
  }

  public static class TryConcExec implements TryConcExecIfc {
    static int concExec = 0;
    static int maxConc = 0;

    static int concWf = 0;
    static int maxWf = 0;

    TryConcExecIfc self;

    @Step()
    public void testConcStep() throws InterruptedException {
      ++TryConcExec.concExec;
      TryConcExec.maxConc = Math.max(TryConcExec.concExec, TryConcExec.maxConc);
      Thread.sleep(1000);
      --TryConcExec.concExec;
    }

    @Workflow()
    public void testConcWorkflow() throws InterruptedException {
      ++TryConcExec.concWf;
      TryConcExec.maxWf = Math.max(TryConcExec.concWf, TryConcExec.maxWf);
      Thread.sleep(500);
      self.testConcStep();
      Thread.sleep(500);
      --TryConcExec.concWf;
    }
  }

  public static interface CatchPlainException1Ifc {
    void testStartAction() throws InterruptedException;

    void testCompleteAction() throws InterruptedException;

    void testCancelAction();

    void testConcWorkflow() throws InterruptedException;
  }

  public static class CatchPlainException1 implements CatchPlainException1Ifc {
    static int execNum = 0;
    static boolean started = false;
    static boolean completed = false;
    static boolean aborted = false;
    static boolean trouble = false;

    CatchPlainException1Ifc self;

    @Step()
    public void testStartAction() throws InterruptedException {
      Thread.sleep(1000);
      CatchPlainException1.started = true;
    }

    @Step()
    public void testCompleteAction() throws InterruptedException {
      // TODO assertEquals(CatchPlainException1.started, true);
      Thread.sleep(1000);
      CatchPlainException1.completed = true;
    }

    @Step()
    public void testCancelAction() {
      CatchPlainException1.aborted = true;
      CatchPlainException1.started = false;
    }

    static void reportTrouble() {
      CatchPlainException1.trouble = true;
      assertEquals("Trouble?", "None!");
    }

    @Workflow()
    public void testConcWorkflow() throws InterruptedException {
      try {
        // Step 1, tell external system to start processing
        self.testStartAction();
      } catch (Exception e) {
        // If we fail for any reason, try to abort
        // (We don't know if the external system even heard us)
        // I have been careful, my undo action in the other system
        // is idempotent, and will be fine if it never heard the start
        try {
          self.testCancelAction();
        } catch (Exception e2) {
          // We have no idea if we managed to get to the external system at any point
          // above
          // We may be leaving system in inconsistent state
          // Take some other notification action (sysadmin!)
          CatchPlainException1.reportTrouble();
        }
      }
      // Step 2, finish the process
      self.testCompleteAction();
    }
  }

  public static interface UsingFinallyClauseIfc {
    void testStartAction() throws InterruptedException;

    void testCompleteAction() throws InterruptedException;

    void testCancelAction();

    void testConcWorkflow() throws InterruptedException;
  }

  public static class UsingFinallyClause implements UsingFinallyClauseIfc {
    static int execNum = 0;
    static boolean started = false;
    static boolean completed = false;
    static boolean aborted = false;
    static boolean trouble = false;
    UsingFinallyClauseIfc self;

    @Step()
    public void testStartAction() throws InterruptedException {
      Thread.sleep(1000);
      UsingFinallyClause.started = true;
    }

    @Step()
    public void testCompleteAction() throws InterruptedException {
      assertTrue(UsingFinallyClause.started);
      Thread.sleep(1000);
      UsingFinallyClause.completed = true;
    }

    @Step()
    public void testCancelAction() {
      UsingFinallyClause.aborted = true;
      UsingFinallyClause.started = false;
    }

    static void reportTrouble() {
      UsingFinallyClause.trouble = true;
      // TODO assertEquals("Trouble?", "None!");
    }

    @Workflow()
    public void testConcWorkflow() throws InterruptedException {
      var finished = false;
      try {
        // Step 1, tell external system to start processing
        self.testStartAction();

        // Step 2, finish the process
        self.testCompleteAction();

        finished = true;
      } finally {
        if (!finished) {
          // If we fail for any reason, try to abort
          // (We don't know if the external system even heard us)
          // I have been careful, my undo action in the other system
          try {
            self.testCancelAction();
          } catch (Exception e2) {
            // We have no idea if we managed to get to the external system at any point
            // above
            // We may be leaving system in inconsistent state
            // Take some other notification action (sysadmin!)
            UsingFinallyClause.reportTrouble();
          }
        }
      }
    }
  }

  public static interface TryConcExec2Ifc {
    void step1() throws InterruptedException;

    void step2() throws InterruptedException;

    void testConcWorkflow() throws InterruptedException;
  }

  public static class TryConcExec2 implements TryConcExec2Ifc {
    static int curExec = 0;
    static int curStep = 0;

    TryConcExec2Ifc self;

    @Step()
    public void step1() throws InterruptedException {
      // This makes the step take a while ... sometimes.
      if (TryConcExec2.curExec++ % 2 == 0) {
        Thread.sleep(1000);
      }
      TryConcExec2.curStep = 1;
    }

    @Step()
    public void step2() {
      TryConcExec2.curStep = 2;
    }

    @Workflow()
    public void testConcWorkflow() throws InterruptedException {
      self.step1();
      self.step2();
    }
  }

  private static DBOSConfig dbosConfig;
  private static TryConcExec execImpl;
  private static TryConcExecIfc execIfc;
  private static CatchPlainException1 catchImpl;
  private static CatchPlainException1Ifc catchIfc;
  private static UsingFinallyClause finallyImpl;
  private static UsingFinallyClauseIfc finallyIfc;
  private static TryConcExec2 concImpl;
  private static TryConcExec2Ifc concIfc;

  private static HikariDataSource dataSource;

  @BeforeAll
  static void onetimeSetup() throws Exception {
    dbosConfig =
        DBOSConfig.defaultsFromEnv("systemdbtest")
            .withDatabaseUrl("jdbc:postgresql://localhost:5432/dbos_java_sys")
            .withMaximumPoolSize(2);
    dataSource = SystemDatabase.createDataSource(dbosConfig);
  }

  @BeforeEach
  void beforeEachTest() throws SQLException {
    DBUtils.recreateDB(dbosConfig);
    DBOS.reinitialize(dbosConfig);

    execImpl = new TryConcExec();
    execIfc = DBOS.registerWorkflows(TryConcExecIfc.class, execImpl);
    execImpl.self = execIfc;

    catchImpl = new CatchPlainException1();
    catchIfc = DBOS.registerWorkflows(CatchPlainException1Ifc.class, catchImpl);
    catchImpl.self = catchIfc;

    finallyImpl = new UsingFinallyClause();
    finallyIfc = DBOS.registerWorkflows(UsingFinallyClauseIfc.class, finallyImpl);
    finallyImpl.self = finallyIfc;

    concImpl = new TryConcExec2();
    concIfc = DBOS.registerWorkflows(TryConcExec2Ifc.class, concImpl);
    concImpl.self = concIfc;

    DBOS.launch();
  }

  @AfterEach
  void afterEachTest() throws Exception {
    DBOS.shutdown();
  }

  static WorkflowHandle<?, ?> reexecuteWorkflowById(String id) throws Exception {
    DBUtils.setWorkflowState(dataSource, id, WorkflowState.PENDING.toString());
    return DBOSTestAccess.getDbosExecutor().executeWorkflowById(id);
  }

  @Test
  void concStartWorkflow() throws Exception {
    var workflowUUID = UUID.randomUUID().toString();
    var wfh1 =
        DBOS.startWorkflow(
            () -> {
              execIfc.testConcWorkflow();
            },
            new StartWorkflowOptions(workflowUUID));
    var wfh2 =
        DBOS.startWorkflow(
            () -> {
              execIfc.testConcWorkflow();
            },
            new StartWorkflowOptions(workflowUUID));

    wfh1.getResult();
    wfh2.getResult();
    // TODO assertEquals(TryConcExec.maxConc, 1);
    // TODO assertEquals(TryConcExec.maxWf, 1);

    var wfh1r = reexecuteWorkflowById(workflowUUID);
    var wfh2r = reexecuteWorkflowById(workflowUUID);
    wfh1r.getResult();
    wfh2r.getResult();
    // TODO assertEquals(TryConcExec.maxConc, 1);
    // TODO assertEquals(TryConcExec.maxWf, 1);
  }

  @Test
  void testUndoRedo1() throws Exception {
    var workflowUUID = UUID.randomUUID().toString();

    var wfh1 =
        DBOS.startWorkflow(
            () -> {
              catchIfc.testConcWorkflow();
            },
            new StartWorkflowOptions(workflowUUID));
    var wfh2 =
        DBOS.startWorkflow(
            () -> {
              catchIfc.testConcWorkflow();
            },
            new StartWorkflowOptions(workflowUUID));

    wfh1.getResult();
    wfh2.getResult();

    // In our invocations above, there are no errors
    // TODO assertTrue(CatchPlainException1.started);
    // TODO assertTrue(CatchPlainException1.completed);
    // assertTrue(!CatchPlainException1.trouble);
  }

  @Test
  void testUndoRedo2() throws Exception {
    var workflowUUID = UUID.randomUUID().toString();

    var wfh1 =
        DBOS.startWorkflow(
            () -> {
              finallyIfc.testConcWorkflow();
            },
            new StartWorkflowOptions(workflowUUID));

    /* TODO
    var wfh2 =
        DBOS.startWorkflow(
            () -> {
              finallyIfc.testConcWorkflow();
            },
            new StartWorkflowOptions(workflowUUID));

    wfh2.getResult();
    */

    wfh1.getResult();

    // In our invocations above, there are no errors
    assertTrue(UsingFinallyClause.started);
    assertTrue(UsingFinallyClause.completed);
    assertTrue(!UsingFinallyClause.trouble);
  }
}
