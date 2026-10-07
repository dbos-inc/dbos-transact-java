package dev.dbos.transact.workflow;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.txstep.JdbcStepFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

interface RewindTestService {

  int counter(String name);

  String fiveSteps(String name);

  String step(String label);

  String receiver(String name);

  String publisher(String name);

  String streamWriter(String name);

  String streamCloser(String name);

  int parent(String name) throws Exception;

  int failingChild(int value);

  int adoptingParent(String name) throws Exception;

  int doubler(int value);

  int repairer(String targetId);

  String stepRewinder(String targetId);

  String rewindInStep(String targetId);

  String blocker() throws InterruptedException;

  int txWriter(String name) throws SQLException;

  void txBlocker() throws Exception;
}

class RewindTestServiceImpl implements RewindTestService {

  private final DBOS dbos;
  private RewindTestService proxy;
  private JdbcStepFactory first;
  private JdbcStepFactory second;

  final Map<String, AtomicInteger> runs = new ConcurrentHashMap<>();
  final Map<String, AtomicInteger> stepRuns = new ConcurrentHashMap<>();
  final AtomicInteger childRuns = new AtomicInteger();
  final AtomicInteger doublerRuns = new AtomicInteger();

  // The blocker holds a queue's only slot until released.
  volatile CountDownLatch blockerStarted = new CountDownLatch(1);
  volatile CountDownLatch blockerReleased = new CountDownLatch(1);

  // The tx blocker parks after its first transactional step.
  final CountDownLatch txCheckpointed = new CountDownLatch(1);
  final CountDownLatch txReleased = new CountDownLatch(1);

  RewindTestServiceImpl(DBOS dbos) {
    this.dbos = dbos;
  }

  void setProxy(RewindTestService proxy) {
    this.proxy = proxy;
  }

  void setStepFactories(JdbcStepFactory first, JdbcStepFactory second) {
    this.first = first;
    this.second = second;
  }

  int runCount(String name) {
    return runs.computeIfAbsent(name, k -> new AtomicInteger()).incrementAndGet();
  }

  int runsOf(String name) {
    var count = runs.get(name);
    return count == null ? 0 : count.get();
  }

  int stepRunsOf(String label) {
    var count = stepRuns.get(label);
    return count == null ? 0 : count.get();
  }

  @Override
  @Workflow
  public int counter(String name) {
    return runCount(name);
  }

  @Override
  @Workflow
  public String fiveSteps(String name) {
    var run = runCount(name);
    proxy.step("one");
    proxy.step("two");
    proxy.step("three");
    proxy.step("four");
    proxy.step("five");
    return "run" + run;
  }

  @Override
  @Step
  public String step(String label) {
    stepRuns.computeIfAbsent(label, k -> new AtomicInteger()).incrementAndGet();
    return label;
  }

  @Override
  @Workflow
  public String receiver(String name) {
    var run = runCount(name);
    String first = dbos.<String>recv("cmd", Duration.ofSeconds(10)).orElse(null);
    String second = dbos.<String>recv("cmd", Duration.ofSeconds(10)).orElse(null);
    return first + second + ":" + run;
  }

  @Override
  @Workflow
  public String publisher(String name) {
    var run = runCount(name);
    dbos.setEvent("below", "kept");
    dbos.setEvent("both", "old");
    if (run == 1) {
      dbos.setEvent("both", "new");
      dbos.setEvent("above", "doomed");
      return "first";
    }
    dbos.setEvent("both", "republished");
    return "second";
  }

  @Override
  @Workflow
  public String streamWriter(String name) {
    var run = runCount(name);
    dbos.writeStream("log", "a" + run);
    dbos.writeStream("log", "b" + run);
    return "run" + run;
  }

  @Override
  @Workflow
  public String streamCloser(String name) {
    var run = runCount(name);
    dbos.writeStream("out", "v" + run);
    dbos.closeStream("out");
    return "run" + run;
  }

  @Override
  @Workflow
  public int parent(String name) throws Exception {
    runCount(name);
    WorkflowHandle<Integer, RuntimeException> handle =
        dbos.startWorkflow(() -> proxy.failingChild(21));
    return handle.getResult();
  }

  @Override
  @Workflow
  public int failingChild(int value) {
    if (childRuns.incrementAndGet() == 1) {
      throw new IllegalStateException("child is bogus");
    }
    return value * 2;
  }

  @Override
  @Workflow
  public int adoptingParent(String name) throws Exception {
    var run = runCount(name);
    WorkflowHandle<Integer, RuntimeException> handle = dbos.startWorkflow(() -> proxy.doubler(21));
    return handle.getResult() + run;
  }

  @Override
  @Workflow
  public int doubler(int value) {
    doublerRuns.incrementAndGet();
    return value * 2;
  }

  @Override
  @Workflow
  public int repairer(String targetId) {
    runCount("repairer");
    WorkflowHandle<Integer, RuntimeException> handle = dbos.rewindWorkflow(targetId, 0);
    return handle.getResult();
  }

  @Override
  @Workflow
  public String stepRewinder(String targetId) {
    runCount("stepRewinder");
    return proxy.rewindInStep(targetId);
  }

  @Override
  @Step
  public String rewindInStep(String targetId) {
    dbos.rewindWorkflow(targetId, 0);
    return targetId;
  }

  @Override
  @Workflow
  public String blocker() throws InterruptedException {
    blockerStarted.countDown();
    blockerReleased.await(30, TimeUnit.SECONDS);
    return "held";
  }

  @Override
  @Workflow
  public int txWriter(String name) throws SQLException {
    var run = runCount(name);
    first.txStep((Connection conn) -> insertRow(conn, "a"), "insertA");
    second.txStep((Connection conn) -> insertRow(conn, "b"), "insertB");
    first.txStep((Connection conn) -> insertRow(conn, "c"), "insertC");
    second.txStep((Connection conn) -> insertRow(conn, "d"), "insertD");
    return run;
  }

  @Override
  @Workflow
  public void txBlocker() throws Exception {
    first.txStep((Connection conn) -> insertRow(conn, "a"), "insertA");
    txCheckpointed.countDown();
    txReleased.await(30, TimeUnit.SECONDS);
  }

  static void insertRow(Connection conn, String value) throws SQLException {
    try (var stmt = conn.prepareStatement("INSERT INTO rewind_rows (v) VALUES (?)")) {
      stmt.setString(1, value);
      stmt.executeUpdate();
    }
  }
}
