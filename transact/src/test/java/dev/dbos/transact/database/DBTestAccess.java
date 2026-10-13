/**
 * Utility class providing test access to database configuration. This class exposes package-private
 * or protected methods from the database layer for testing purposes, allowing test classes to
 * access internal configuration without breaking encapsulation in production code.
 */
package dev.dbos.transact.database;

import java.sql.SQLException;
import java.util.Optional;
import java.util.UUID;

import com.zaxxer.hikari.HikariConfig;

public class DBTestAccess {
  public static Optional<HikariConfig> findHikariConfig(SystemDatabase sysdb) {
    return sysdb.getConfig();
  }

  /**
   * Gives a workflow a fresh owner token, as a queue claim does, and returns it. Whatever execution
   * held the workflow before no longer owns it.
   */
  public static String takeOwnership(SystemDatabase sysdb, String workflowId) {
    var ownerXid = UUID.randomUUID().toString();
    var ctx = sysdb.context();
    var sql =
        "UPDATE \"%s\".workflow_status SET owner_xid = ? WHERE workflow_uuid = ?"
            .formatted(ctx.schema());
    try (var conn = ctx.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, ownerXid);
      stmt.setString(2, workflowId);
      if (stmt.executeUpdate() != 1) {
        throw new IllegalStateException("No workflow " + workflowId);
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    return ownerXid;
  }
}
