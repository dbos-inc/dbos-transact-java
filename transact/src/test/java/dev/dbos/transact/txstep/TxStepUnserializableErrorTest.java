package dev.dbos.transact.txstep;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.json.DBOSJavaSerializer;
import dev.dbos.transact.json.DBOSSerializer;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.Workflow;

import java.sql.Connection;
import java.util.concurrent.atomic.AtomicInteger;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A transactional step's error the serializer refuses is recorded, and thrown, as the same stand-in
 * the system database records for the step, so the first run and a replay see the same error.
 */
class TxStepUnserializableErrorTest {

  /** Java serialization refuses it: its payload is not {@link java.io.Serializable}. */
  public static class UnserializableException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    @SuppressWarnings({"unused", "serial"})
    private final Object payload = new Object();

    public UnserializableException(String message) {
      super(message);
    }
  }

  /** Writes values as the built-in serializer does, and refuses every error. */
  static class RefusesErrorsSerializer implements DBOSSerializer {
    @Override
    public String name() {
      return "refuses_errors";
    }

    @Override
    public String serialize(Object value) {
      return DBOSJavaSerializer.INSTANCE.serialize(value);
    }

    @Override
    public Object deserialize(String text) {
      return DBOSJavaSerializer.INSTANCE.deserialize(text);
    }

    @Override
    public String serializeThrowable(Throwable throwable) {
      throw new UnsupportedOperationException("this serializer refuses errors");
    }

    @Override
    public Throwable deserializeThrowable(String text) {
      return DBOSJavaSerializer.INSTANCE.deserializeThrowable(text);
    }
  }

  public interface TxService {
    String catchUnserializableError();
  }

  public static class TxServiceImpl implements TxService {
    private final DBOS dbos;
    private final JdbcStepFactory stepFactory;
    final AtomicInteger runs = new AtomicInteger();

    TxServiceImpl(DBOS dbos, JdbcStepFactory stepFactory) {
      this.dbos = dbos;
      this.stepFactory = stepFactory;
    }

    @Override
    @Workflow
    public String catchUnserializableError() {
      try {
        stepFactory.txStep(
            (Connection c) -> {
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

  @AutoClose final PgContainer pgContainer = new PgContainer();
  @AutoClose DBOS dbos;
  @AutoClose HikariDataSource dataSource;
  TxServiceImpl impl;
  TxService proxy;

  @BeforeEach
  void beforeEach() {
    dataSource = pgContainer.dataSource();
    launch(null);
  }

  private void launch(DBOSSerializer serializer) {
    dbos = new DBOS(pgContainer.dbosConfig().withSerializer(serializer));
    // The tests read the checkpoint the step wrote, after the workflow finishes.
    DBOSTestAccess.keepCompletedStepCheckpoints(dbos);
    var stepFactory = new JdbcStepFactory(dbos, dataSource);
    impl = new TxServiceImpl(dbos, stepFactory);
    proxy = dbos.registerProxy(TxService.class, impl);
    dbos.launch();
  }

  @Test
  void anErrorTheSerializerRefusesIsRecordedAndThrownAsAStandIn() throws Exception {
    var workflowId = "unserializable-tx-error";
    String firstRun;
    try (var _o = new WorkflowOptions(workflowId).setContext()) {
      firstRun = proxy.catchUnserializableError();
    }
    assertEquals(
        "caught java.lang.RuntimeException: "
            + UnserializableException.class.getName()
            + ": the transaction failed",
        firstRun);
    assertNotNull(DBUtils.getStepRows(dataSource, workflowId).get(0).error());

    var fork = dbos.<String, RuntimeException>forkWorkflow(workflowId, 1);
    assertEquals(firstRun, fork.getResult());
    assertEquals(1, impl.runs.get());
  }

  @Test
  void aStandInTheSerializerRefusesTooIsRecordedInTheBuiltInFormat() throws Exception {
    dbos.close();
    launch(new RefusesErrorsSerializer());

    var workflowId = "unserializable-tx-error-refused-stand-in";
    String firstRun;
    try (var _o = new WorkflowOptions(workflowId).setContext()) {
      firstRun = proxy.catchUnserializableError();
    }
    // The body is handed the stand-in the factory recorded, not the serializer's refusal.
    assertEquals(
        "caught java.lang.RuntimeException: "
            + UnserializableException.class.getName()
            + ": the transaction failed",
        firstRun);
    var txRows = DBUtils.getTxStepRows(dataSource, workflowId);
    assertEquals(1, txRows.size());
    assertNotNull(txRows.get(0).error());
    assertEquals(DBOSJavaSerializer.NAME, txRows.get(0).serialization());

    var fork = dbos.<String, RuntimeException>forkWorkflow(workflowId, 1);
    assertEquals(firstRun, fork.getResult());
    assertEquals(1, impl.runs.get());
  }
}
