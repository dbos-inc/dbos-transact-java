package dev.dbos.transact.spring.txstep;

import static org.springframework.transaction.TransactionDefinition.PROPAGATION_REQUIRED;
import static org.springframework.transaction.TransactionDefinition.PROPAGATION_REQUIRES_NEW;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.execution.ThrowingSupplier;
import dev.dbos.transact.json.DBOSSerializer;
import dev.dbos.transact.json.SerializationUtil;
import dev.dbos.transact.txstep.PostgresStepFactory;
import dev.dbos.transact.txstep.TxStepSchema;
import dev.dbos.transact.workflow.internal.StepResult;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Optional;

import javax.sql.DataSource;

import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

/**
 * A step factory that integrates with Spring's {@link PlatformTransactionManager} to provide
 * exactly-once transactional steps via the {@code @TransactionalStep} annotation.
 *
 * <p>Each step runs inside a {@code REQUIRES_NEW} Spring transaction. The step output is written to
 * {@code tx_step_outputs} atomically with the user's database work, using the transaction-bound
 * connection obtained from {@link DataSourceUtils#getConnection(DataSource)}.
 *
 * <p>This class is created and managed by {@code TransactionalStepAutoConfiguration}. Call {@link
 * #initialize()} before processing any steps (the registrar does this automatically when it detects
 * annotated methods).
 */
public class TransactionalStepFactory {

  private final DBOS dbos;
  private final DataSource dataSource;
  private final PlatformTransactionManager txManager;
  private final String schema;
  private final DBOSSerializer serializer;

  public TransactionalStepFactory(
      DBOS dbos, DataSource dataSource, PlatformTransactionManager txManager, String schema) {
    this.dbos = Objects.requireNonNull(dbos);
    this.dataSource = Objects.requireNonNull(dataSource);
    this.txManager = Objects.requireNonNull(txManager);
    var config = dbos.integration().config();
    this.schema = SystemDatabase.sanitizeSchema(schema != null ? schema : config.databaseSchema());
    this.serializer = config.serializer();
  }

  /**
   * Verifies the datasource is PostgreSQL and creates the {@code tx_step_outputs} table if it does
   * not already exist. Called lazily by {@code TransactionalStepRegistrar} only when annotated
   * methods are found — avoids any DB contact for applications that never use this starter.
   */
  public void initialize() {
    try (var conn = dataSource.getConnection()) {
      TxStepSchema.verifyPostgres(conn);
      TxStepSchema.createTable(conn, schema);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private Optional<StepResult> checkExecution(String workflowId, int stepId, String stepName) {
    try (var conn = dataSource.getConnection();
        var stmt = conn.prepareStatement(TxStepSchema.checkSql(schema))) {
      stmt.setString(1, workflowId);
      stmt.setInt(2, stepId);
      try (var rs = stmt.executeQuery()) {
        return TxStepSchema.readResult(rs, workflowId, stepId, stepName);
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void recordOutput(Connection conn, String workflowId, int stepId, Object result) {
    var value = SerializationUtil.serializeValue(result, null, serializer);
    recordResult(conn, workflowId, stepId, value.serializedValue(), null, value.serialization());
  }

  private void recordError(String workflowId, int stepId, Exception exception) {
    var value = SerializationUtil.serializeError(exception, null, serializer);
    try (var conn = dataSource.getConnection()) {
      conn.setAutoCommit(false);
      try {
        recordResult(
            conn, workflowId, stepId, null, value.serializedValue(), value.serialization());
        conn.commit();
      } catch (PostgresStepFactory.StepConflictException ignored) {
        conn.rollback();
      } catch (SQLException ex) {
        conn.rollback();
        throw ex;
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void recordResult(
      Connection conn,
      String workflowId,
      int stepId,
      String output,
      String error,
      String serialization) {
    try (var stmt = conn.prepareStatement(TxStepSchema.upsertSql(schema))) {
      stmt.setString(1, workflowId);
      stmt.setInt(2, stepId);
      stmt.setString(3, output);
      stmt.setString(4, error);
      stmt.setString(5, serialization);
      stmt.executeUpdate();
    } catch (SQLException e) {
      if (PostgresStepFactory.isUniqueViolation(e))
        throw new PostgresStepFactory.StepConflictException(e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Runs {@code supplier} as an idempotent DBOS step inside a {@code REQUIRES_NEW} Spring
   * transaction. Called by the {@code TransactionalStepAspect}.
   *
   * <p>The step output is written to {@code tx_step_outputs} atomically with the transaction
   * started for the user's work. On failure the transaction is rolled back and the error is
   * recorded in a separate connection so retries can replay the error without re-executing.
   *
   * @param supplier the step body; typically {@code () -> pjp.proceed()} from the aspect
   * @param stepName stable name for the step within the workflow
   */
  @SuppressWarnings("unchecked")
  public <E extends Exception> Object runTransactionalStep(
      ThrowingSupplier<Object, E> supplier, String stepName) throws E {
    // If a @TransactionalStep method is called outside a workflow or inside a step, execute the
    // supplier as a standard @Transactional method without any of the durable execution bookkeeping
    if (!DBOS.inWorkflow() || DBOS.inStep()) {
      var txDef = new DefaultTransactionDefinition(PROPAGATION_REQUIRED);  
      TransactionStatus status = txManager.getTransaction(txDef);
      try {
        Object result = supplier.execute();
        txManager.commit(status);
        return result;
      } catch (RuntimeException | Error e) {
        txManager.rollback(status);
        throw e;
      } catch (Exception e) {
        txManager.commit(status);
        throw (E) e;
      }
    }

    return dbos.<Object, E>runStep(
        () -> {
          var workflowId = Objects.requireNonNull(DBOS.workflowId());
          int stepId = Objects.requireNonNull(DBOS.stepId());

          var prev = checkExecution(workflowId, stepId, stepName);
          if (prev.isPresent()) {
            return prev.get().<Object, E>toResult(serializer);
          }

          var txDef = new DefaultTransactionDefinition(PROPAGATION_REQUIRES_NEW);
          TransactionStatus status = txManager.getTransaction(txDef);
          try {
            Object result = supplier.execute();
            Connection conn = DataSourceUtils.getConnection(dataSource);
            recordOutput(conn, workflowId, stepId, result);
            txManager.commit(status);
            return result;
          } catch (PostgresStepFactory.StepConflictException conflict) {
            txManager.rollback(status);
            return checkExecution(workflowId, stepId, stepName)
                .orElseThrow(
                    () ->
                        new IllegalStateException(
                            "Conflict on tx_step_outputs but winner row not found: workflowId=%s stepId=%d stepName=%s"
                                .formatted(workflowId, stepId, stepName),
                            conflict))
                .<Object, E>toResult(serializer);
          } catch (Exception e) {
            txManager.rollback(status);
            recordError(workflowId, stepId, e);
            throw (E) e;
          }
        },
        stepName);
  }
}
