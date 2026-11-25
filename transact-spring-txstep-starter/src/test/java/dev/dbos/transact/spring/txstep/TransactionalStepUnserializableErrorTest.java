package dev.dbos.transact.spring.txstep;

import static org.assertj.core.api.Assertions.assertThat;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.workflow.Workflow;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

/**
 * A transactional step's error the serializer refuses is recorded in {@code tx_step_outputs}, and
 * thrown, as the same stand-in the system database records for the step, so the first run and a
 * replay see the same error.
 */
class TransactionalStepUnserializableErrorTest {

  /** Java serialization refuses it: its payload is not {@link java.io.Serializable}. */
  public static class UnserializableException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    @SuppressWarnings({"unused", "serial"})
    private final Object payload = new Object();

    public UnserializableException(String message) {
      super(message);
    }
  }

  interface TxService {
    String catchUnserializableError();
  }

  static class TxServiceImpl implements TxService {
    private final DBOS dbos;
    private final TransactionalStepFactory factory;
    final AtomicInteger runs = new AtomicInteger();

    TxServiceImpl(DBOS dbos, TransactionalStepFactory factory) {
      this.dbos = dbos;
      this.factory = factory;
    }

    @Override
    @Workflow
    public String catchUnserializableError() {
      try {
        factory.runTransactionalStep(
            () -> {
              runs.incrementAndGet();
              throw new UnserializableException("the transaction failed");
            },
            "unserializableTxError");
        return "returned";
      } catch (RuntimeException e) {
        return "caught " + e.getClass().getName() + ": " + e.getMessage();
      } finally {
        dbos.runStep(() -> "after", "afterStep");
      }
    }
  }

  @AutoClose TransactionalStepTest.TestDatabase db;
  @AutoClose DBOS dbos;
  TxServiceImpl impl;
  TxService proxy;

  @BeforeEach
  void setup() {
    db = new TransactionalStepTest.TestDatabase();
    dbos = new DBOS(db.dbosConfig());
    // The test reads the checkpoint the step wrote, after the workflow finishes.
    DBOSTestAccess.keepCompletedStepCheckpoints(dbos);
    var factory =
        new TransactionalStepFactory(
            dbos, db.dataSource, new DataSourceTransactionManager(db.dataSource), null);
    factory.initialize();
    impl = new TxServiceImpl(dbos, factory);
    proxy = dbos.registerProxy(TxService.class, impl);
    dbos.launch();
  }

  @Test
  void anErrorTheSerializerRefusesIsRecordedAndThrownAsAStandIn() throws Exception {
    var workflowId = "unserializable-spring-tx-error";
    String firstRun;
    try (var _o = new WorkflowOptions(workflowId).setContext()) {
      firstRun = proxy.catchUnserializableError();
    }
    assertThat(firstRun)
        .isEqualTo(
            "caught java.lang.RuntimeException: "
                + UnserializableException.class.getName()
                + ": the transaction failed");

    var rows = TransactionalStepTest.getTxRows(db.dataSource, workflowId);
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).output()).isNull();
    assertThat(rows.get(0).error()).isNotNull();

    var fork = dbos.<String, RuntimeException>forkWorkflow(workflowId, 1);
    assertThat(fork.getResult()).isEqualTo(firstRun);
    assertThat(impl.runs.get()).isEqualTo(1);
  }
}
