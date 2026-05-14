package dev.dbos.transact.database;

import dev.dbos.transact.workflow.VersionInfo;

import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

class ApplicationVersionDAO {

  private ApplicationVersionDAO() {}

  static void createApplicationVersion(DbContext ctx, String versionName) throws SQLException {
    String sql =
        """
          INSERT INTO "%s".application_versions (version_id, version_name)
          VALUES (?, ?)
          ON CONFLICT (version_name) DO NOTHING
        """
            .formatted(ctx.schema());
    try (var conn = ctx.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, UUID.randomUUID().toString());
      stmt.setString(2, versionName);
      stmt.executeUpdate();
    }
  }

  static void updateApplicationVersionTimestamp(
      DbContext ctx, String versionName, Instant newTimestamp) throws SQLException {
    String sql =
        """
          UPDATE "%s".application_versions
          SET version_timestamp = ?
          WHERE version_name = ?
        """
            .formatted(ctx.schema());
    try (var conn = ctx.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setLong(1, newTimestamp.toEpochMilli());
      stmt.setString(2, versionName);
      stmt.executeUpdate();
    }
  }

  static List<VersionInfo> listApplicationVersions(DbContext ctx) throws SQLException {
    String sql =
        """
          SELECT version_id, version_name, version_timestamp, created_at
          FROM "%s".application_versions
          ORDER BY version_timestamp DESC
        """
            .formatted(ctx.schema());
    List<VersionInfo> results = new ArrayList<>();
    try (var conn = ctx.getConnection();
        var stmt = conn.prepareStatement(sql);
        var rs = stmt.executeQuery()) {
      while (rs.next()) {
        results.add(
            new VersionInfo(
                rs.getString("version_id"),
                rs.getString("version_name"),
                Instant.ofEpochMilli(rs.getLong("version_timestamp")),
                Instant.ofEpochMilli(rs.getLong("created_at"))));
      }
    }
    return results;
  }

  static VersionInfo getLatestApplicationVersion(DbContext ctx) throws SQLException {
    String sql =
        """
          SELECT version_id, version_name, version_timestamp, created_at
          FROM "%s".application_versions
          ORDER BY version_timestamp DESC
          LIMIT 1
        """
            .formatted(ctx.schema());
    try (var conn = ctx.getConnection();
        var stmt = conn.prepareStatement(sql);
        var rs = stmt.executeQuery()) {
      if (rs.next()) {
        return new VersionInfo(
            rs.getString("version_id"),
            rs.getString("version_name"),
            Instant.ofEpochMilli(rs.getLong("version_timestamp")),
            Instant.ofEpochMilli(rs.getLong("created_at")));
      }
    }
    throw new RuntimeException("No application versions found");
  }
}
