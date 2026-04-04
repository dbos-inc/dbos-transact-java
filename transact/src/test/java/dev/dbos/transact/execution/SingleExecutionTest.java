package dev.dbos.transact.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.internal.DebugTriggers;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.Step;
import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.WorkflowHandle;
import dev.dbos.transact.workflow.WorkflowState;

import java.sql.SQLTransientException;
import java.util.UUID;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

interface TryConcExecIfc {
  void testConcStep() throws InterruptedException;

  void testConcWorkflow() throws InterruptedException;

  String step1() throws InterruptedException;

  String testWorkflow() throws InterruptedException;
}

class TryConcExec implements TryConcExecIfc {
  int concExec = 0;
  int maxConc = 0;

  int concWf = 0;
  int maxWf = 0;

  TryConcExecIfc self;

  @Override
  @Step()
  public void testConcStep() throws InterruptedException {
    ++this.concExec;
    this.maxConc = Math.max(this.concExec, this.maxConc);
    Thread.sleep(1000);
    --this.concExec;
  }

  @Override
  @Workflow()
  public void testConcWorkflow() throws InterruptedException {
    ++this.concWf;
    this.maxWf = Math.max(this.concWf, this.maxWf);
    Thread.sleep(500);
    self.testConcStep();
    Thread.sleep(500);
    --this.concWf;
  }

  @Override
  @Step()
  public String step1() throws InterruptedException {
    Thread.sleep(1000);
    return "Yay!";
  }

  @Override
  @Workflow()
  public String testWorkflow() throws InterruptedException {
    return self.step1();
  }
}

interface CatchPlainException1Ifc {
  void testStartAction() throws InterruptedException;

  void testCompleteAction() throws InterruptedException;

  void testCancelAction();

  void testConcWorkflow() throws InterruptedException;
}

class CatchPlainException1 implements CatchPlainException1Ifc {
  int execNum = 0;
  boolean started = false;
  boolean completed = false;
  boolean aborted = false;
  boolean trouble = false;

  CatchPlainException1Ifc self;

  @Override
  @Step()
  public void testStartAction() throws InterruptedException {
    Thread.sleep(1000);
    this.started = true;
  }

  @Override
  @Step()
  public void testCompleteAction() throws InterruptedException {
    assertEquals(this.started, true);
    Thread.sleep(1000);
    this.completed = true;
  }

  @Override
  @Step()
  public void testCancelAction() {
    this.aborted = true;
    this.started = false;
  }

  void reportTrouble() {
    this.trouble = true;
    assertEquals("Trouble?", "None!");
  }

  @Override
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
        this.reportTrouble();
      }
    }
    // Step 2, finish the process
    self.testCompleteAction();
  }
}

interface UsingFinallyClauseIfc {
  void testStartAction() throws InterruptedException;

  void testCompleteAction() throws InterruptedException;

  void testCancelAction();

  void testConcWorkflow() throws InterruptedException;
}

class UsingFinallyClause implements UsingFinallyClauseIfc {
  int execNum = 0;
  boolean started = false;
  boolean completed = false;
  boolean aborted = false;
  boolean trouble = false;
  UsingFinallyClauseIfc self;

  @Override
  @Step()
  public void testStartAction() throws InterruptedException {
    Thread.sleep(1000);
    this.started = true;
  }

  @Override
  @Step()
  public void testCompleteAction() throws InterruptedException {
    assertTrue(this.started);
    Thread.sleep(1000);
    this.completed = true;
  }

  @Override
  @Step()
  public void testCancelAction() {
    this.aborted = true;
    this.started = false;
  }

  void reportTrouble() {
    this.trouble = true;
    assertEquals("Trouble?", "None!");
  }

  @Override
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
          this.reportTrouble();
        }
      }
    }
  }
}

interface TryConcExec2Ifc {
  void step1() throws InterruptedException;

  void step2() throws InterruptedException;

  void testConcWorkflow() throws InterruptedException;
}

class TryConcExec2 implements TryConcExec2Ifc {
  int curExec = 0;
  int curStep = 0;

  TryConcExec2Ifc self;

  @Override
  @Step()
  public void step1() throws InterruptedException {
    // This makes the step take a while ... sometimes.
    if (this.curExec++ % 2 == 0) {
      Thread.sleep(1000);
    }
    this.curStep = 1;
  }

  @Override
  @Step()
  public void step2() {
    this.curStep = 2;
  }

  @Override
  @Workflow()
  public void testConcWorkflow() throws InterruptedException {
    self.step1();
    self.step2();
  }
}

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
public class SingleExecutionTest {
  @AutoClose final PgContainer pgContainer = new PgContainer();

  DBOSConfig dbosConfig;
  @AutoClose DBOS dbos;
  @AutoClose HikariDataSource dataSource;

  TryConcExecIfc execIfc;
  CatchPlainException1Ifc catchIfc;
  UsingFinallyClauseIfc finallyIfc;
  TryConcExec2Ifc concIfc;

  TryConcExec execImpl;
  CatchPlainException1 catchImpl;
  UsingFinallyClause finallyImpl;
  TryConcExec2 concImpl;

  @BeforeEach
  void beforeEach() {
    dbosConfig = pgContainer.dbosConfig();
    dbos = new DBOS(dbosConfig);
    dataSource = pgContainer.dataSource();

    execImpl = new TryConcExec();
    execIfc = dbos.registerProxy(TryConcExecIfc.class, execImpl);
    execImpl.self = execIfc;

    catchImpl = new CatchPlainException1();
    catchIfc = dbos.registerProxy(CatchPlainException1Ifc.class, catchImpl);
    catchImpl.self = catchIfc;

    finallyImpl = new UsingFinallyClause();
    finallyIfc = dbos.registerProxy(UsingFinallyClauseIfc.class, finallyImpl);
    finallyImpl.self = finallyIfc;

    concImpl = new TryConcExec2();
    concIfc = dbos.registerProxy(TryConcExec2Ifc.class, concImpl);
    concImpl.self = concIfc;

    dbos.launch();
  }

  WorkflowHandle<?, ?> reexecuteWorkflowById(String id) throws Exception {
    DBUtils.setWorkflowState(dataSource, id, WorkflowState.PENDING.toString());
    return DBOSTestAccess.getDbosExecutor(dbos).executeWorkflowById(id, true, false);
  }

  @Test
  void concStartWorkflow() throws Exception {
    var workflowUUID = UUID.randomUUID().toString();
    var wfh1 =
        dbos.startWorkflow(
            () -> {
              execIfc.testConcWorkflow();
            },
            new StartWorkflowOptions(workflowUUID));
    var wfh2 =
        dbos.startWorkflow(
            () -> {
              execIfc.testConcWorkflow();
            },
            new StartWorkflowOptions(workflowUUID));

    wfh1.getResult();
    wfh2.getResult();
    assertEquals(1, execImpl.maxConc);
    assertEquals(1, execImpl.maxWf);

    var wfh1r = reexecuteWorkflowById(workflowUUID);
    var wfh2r = reexecuteWorkflowById(workflowUUID);
    wfh1r.getResult();
    wfh2r.getResult();
    assertEquals(1, execImpl.maxConc);
    assertEquals(1, execImpl.maxWf);
  }

  @Test
  void testUndoRedo1() throws Exception {
    var workflowUUID = UUID.randomUUID().toString();

    var wfh1 =
        dbos.startWorkflow(
            () -> {
              catchIfc.testConcWorkflow();
            },
            new StartWorkflowOptions(workflowUUID));
    var wfh2 =
        dbos.startWorkflow(
            () -> {
              catchIfc.testConcWorkflow();
            },
            new StartWorkflowOptions(workflowUUID));

    wfh1.getResult();
    wfh2.getResult();

    // In our invocations above, there are no errors
    assertTrue(catchImpl.started);
    assertTrue(catchImpl.completed);
    assertTrue(!catchImpl.trouble);
  }

  @Test
  void testUndoRedo2() throws Exception {
    var workflowUUID = UUID.randomUUID().toString();

    var wfh1 =
        dbos.startWorkflow(
            () -> {
              finallyIfc.testConcWorkflow();
            },
            new StartWorkflowOptions(workflowUUID));

    var wfh2 =
        dbos.startWorkflow(
            () -> {
              finallyIfc.testConcWorkflow();
            },
            new StartWorkflowOptions(workflowUUID));

    wfh2.getResult();

    wfh1.getResult();

    // In our invocations above, there are no errors
    assertTrue(finallyImpl.started);
    assertTrue(finallyImpl.completed);
    assertTrue(!finallyImpl.trouble);
  }

  @Test
  void testStepSequence() throws Exception {
    var workflowUUID = UUID.randomUUID().toString();

    var wfh1 =
        dbos.startWorkflow(
            () -> {
              concIfc.testConcWorkflow();
            },
            new StartWorkflowOptions(workflowUUID));
    var wfh2 =
        dbos.startWorkflow(
            () -> {
              concIfc.testConcWorkflow();
            },
            new StartWorkflowOptions(workflowUUID));

    wfh1.getResult();
    wfh2.getResult();
    assertEquals(2, concImpl.curStep);
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
