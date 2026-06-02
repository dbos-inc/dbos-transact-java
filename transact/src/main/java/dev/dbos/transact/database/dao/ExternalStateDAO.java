package dev.dbos.transact.database.dao;

import dev.dbos.transact.database.DbContext;
import dev.dbos.transact.database.ExternalState;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

public class ExternalStateDAO {

  private ExternalStateDAO() {}

  public static Optional<ExternalState> getExternalState(
      DbContext ctx, String service, String workflowName, String key) throws SQLException {
    final String sql =
        """
          SELECT value, update_seq, update_time FROM "%s".event_dispatch_kv WHERE service_name = ? AND workflow_fn_name = ? AND key = ?
        """
            .formatted(ctx.schema());

    try (var conn = ctx.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, Objects.requireNonNull(service, "service must not be null"));
      stmt.setString(2, Objects.requireNonNull(workflowName, "workflowName must not be null"));
      stmt.setString(3, Objects.requireNonNull(key, "key must not be null"));

      try (var rs = stmt.executeQuery()) {
        if (rs.next()) {
          var value = rs.getString("value");
          BigDecimal seqDecimal = rs.getBigDecimal("update_seq");
          BigInteger seq = seqDecimal != null ? seqDecimal.toBigInteger() : null;
          BigDecimal timeDecimal = rs.getBigDecimal("update_time");
          Instant time = timeDecimal != null ? bigDecimalToInstant(timeDecimal) : null;
          return Optional.of(new ExternalState(service, workflowName, key, value, time, seq));
        } else {
          return Optional.empty();
        }
      }
    }
  }

  public static ExternalState upsertExternalState(DbContext ctx, ExternalState state)
      throws SQLException {
    final var sql =
        """
          INSERT INTO "%s".event_dispatch_kv (
          service_name, workflow_fn_name, key, value, update_time, update_seq)
          VALUES (?, ?, ?, ?, ?, ?)
          ON CONFLICT (service_name, workflow_fn_name, key)
          DO UPDATE SET
            update_time = GREATEST(EXCLUDED.update_time, event_dispatch_kv.update_time),
            update_seq =  GREATEST(EXCLUDED.update_seq,  event_dispatch_kv.update_seq),
            value = CASE WHEN (EXCLUDED.update_time > event_dispatch_kv.update_time
              OR EXCLUDED.update_seq > event_dispatch_kv.update_seq
              OR (event_dispatch_kv.update_time IS NULL and event_dispatch_kv.update_seq IS NULL)
            ) THEN EXCLUDED.value ELSE event_dispatch_kv.value END
          RETURNING value, update_time, update_seq
        """
            .formatted(ctx.schema());

    try (var conn = ctx.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, Objects.requireNonNull(state.service(), "service must not be null"));
      stmt.setString(
          2, Objects.requireNonNull(state.workflowName(), "workflowName must not be null"));
      stmt.setString(3, Objects.requireNonNull(state.key(), "key must not be null"));
      stmt.setString(4, state.value());
      Instant updateTime = state.updateTime();
      stmt.setBigDecimal(5, updateTime != null ? instantToBigDecimal(updateTime) : null);
      stmt.setObject(6, state.updateSeq());

      try (var rs = stmt.executeQuery()) {
        if (rs.next()) {
          var value = rs.getString("value");
          BigDecimal seqDecimal = rs.getBigDecimal("update_seq");
          BigInteger seq = seqDecimal != null ? seqDecimal.toBigInteger() : null;
          BigDecimal timeDecimal = rs.getBigDecimal("update_time");
          Instant time = timeDecimal != null ? bigDecimalToInstant(timeDecimal) : null;
          return new ExternalState(
              state.service(), state.workflowName(), state.key(), value, time, seq);
        } else {
          throw new RuntimeException(
              "Attempted to upsert external state %s / %s / %s"
                  .formatted(state.service(), state.workflowName(), state.key()));
        }
      }
    }
  }

  private static BigDecimal instantToBigDecimal(Instant instant) {
    return BigDecimal.valueOf(instant.getEpochSecond())
        .add(BigDecimal.valueOf(instant.getNano(), 9));
  }

  private static Instant bigDecimalToInstant(BigDecimal value) {
    BigDecimal floor = value.setScale(0, RoundingMode.FLOOR);
    long seconds = floor.longValue();
    int nanos = value.subtract(floor).movePointRight(9).intValue();
    return Instant.ofEpochSecond(seconds, nanos);
  }
}
