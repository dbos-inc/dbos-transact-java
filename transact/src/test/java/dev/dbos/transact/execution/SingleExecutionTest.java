package dev.dbos.transact.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.internal.DebugTriggers;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.utils.DBUtils.DBSettings;
import dev.dbos.transact.workflow.Step;
import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.WorkflowHandle;
import dev.dbos.transact.workflow.WorkflowState;

import java.sql.SQLException;
import java.sql.SQLTransientException;
import java.util.UUID;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
public class SingleExecutionTest {
  public static interface TryConcExecIfc {
    public void testConcStep() throws InterruptedException;

    public void testConcWorkflow() throws InterruptedException;

    public String step1() throws InterruptedException;

    public String testWorkflow() throws InterruptedException;
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

    @Step()
    public String step1() throws InterruptedException {
      Thread.sleep(1000);
      return "Yay!";
    }

    @Workflow()
    public String testWorkflow() throws InterruptedException {
      return self.step1();
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
      assertEquals(CatchPlainException1.started, true);
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
      assertEquals("Trouble?", "None!");
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

  private static final DBSettings db = DBSettings.get();
  private DBOSConfig dbosConfig;
  private HikariDataSource dataSource;

  private TryConcExec execImpl;
  private TryConcExecIfc execIfc;
  private CatchPlainException1 catchImpl;
  private CatchPlainException1Ifc catchIfc;
  private UsingFinallyClause finallyImpl;
  private UsingFinallyClauseIfc finallyIfc;
  private TryConcExec2 concImpl;
  private TryConcExec2Ifc concIfc;

  @BeforeEach
  void beforeEachTest() throws SQLException {
    db.recreate();

    dataSource = db.dataSource();
    dbosConfig = DBOSConfig.defaults("systemdbtest").withDataSource(dataSource);

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
    dataSource.close();
  }

  WorkflowHandle<?, ?> reexecuteWorkflowById(String id) throws Exception {
    DBUtils.setWorkflowState(dataSource, id, WorkflowState.PENDING.toString());
    return DBOSTestAccess.getDbosExecutor().executeWorkflowById(id, true, false);
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
    assertEquals(1, TryConcExec.maxConc);
    assertEquals(1, TryConcExec.maxWf);

    var wfh1r = reexecuteWorkflowById(workflowUUID);
    var wfh2r = reexecuteWorkflowById(workflowUUID);
    wfh1r.getResult();
    wfh2r.getResult();
    assertEquals(1, TryConcExec.maxConc);
    assertEquals(1, TryConcExec.maxWf);
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
    assertTrue(CatchPlainException1.started);
    assertTrue(CatchPlainException1.completed);
    assertTrue(!CatchPlainException1.trouble);
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

    var wfh2 =
        DBOS.startWorkflow(
            () -> {
              finallyIfc.testConcWorkflow();
            },
            new StartWorkflowOptions(workflowUUID));

    wfh2.getResult();

    wfh1.getResult();

    // In our invocations above, there are no errors
    assertTrue(UsingFinallyClause.started);
    assertTrue(UsingFinallyClause.completed);
    assertTrue(!UsingFinallyClause.trouble);
  }

  @Test
  void testStepSequence() throws Exception {
    var workflowUUID = UUID.randomUUID().toString();

    var wfh1 =
        DBOS.startWorkflow(
            () -> {
              concIfc.testConcWorkflow();
            },
            new StartWorkflowOptions(workflowUUID));
    var wfh2 =
        DBOS.startWorkflow(
            () -> {
              concIfc.testConcWorkflow();
            },
            new StartWorkflowOptions(workflowUUID));

    wfh1.getResult();
    wfh2.getResult();
    assertEquals(2, TryConcExec2.curStep);
  }

  @Test
  void testCommitHiccups() throws InterruptedException {
    assertEquals("Yay!", execIfc.testWorkflow());

    DebugTriggers.setDebugTrigger(
        DebugTriggers.DEBUG_TRIGGER_STEP_COMMIT,
        new DebugTriggers.DebugAction().setSqlExceptionToThrow(new SQLTransientException()));
    assertEquals("Yay!", execIfc.testWorkflow());

    DebugTriggers.setDebugTrigger(
        DebugTriggers.DEBUG_TRIGGER_INITWF_COMMIT,
        new DebugTriggers.DebugAction().setSqlExceptionToThrow(new SQLTransientException()));
    assertEquals("Yay!", execIfc.testWorkflow());
  }
}
