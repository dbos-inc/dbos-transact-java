package dev.dbos.transact.database.dao;

import dev.dbos.transact.database.DbContext;
import dev.dbos.transact.workflow.VersionInfo;

import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class ApplicationVersionDAO {

  private ApplicationVersionDAO() {}

  /**
   * Registers this version, claiming the row if nobody owns it yet so a version registered before
   * this application had a name does not stay unclaimed. A peer's name is a collision, which is why
   * this raises.
   */
  public static void createApplicationVersion(DbContext ctx, String versionName)
      throws SQLException {
    // Claim a pre-upgrade row in place, so the version is neither recreated nor retimed.
    String claimSql =
        """
          UPDATE "%s".application_versions
          SET application_name = ?
          WHERE version_name = ? AND application_name IS NULL
        """
            .formatted(ctx.schema());
    // Targetless DO NOTHING: it names no arbiter, so it survives version_name's global uniqueness
    // being dropped while still absorbing a concurrent registrar.
    String insertSql =
        """
          INSERT INTO "%s".application_versions (version_id, version_name, application_name)
          VALUES (?, ?, ?)
          ON CONFLICT DO NOTHING
        """
            .formatted(ctx.schema());

    try (var conn = ctx.getConnection()) {
      int claimed = 0;
      if (ctx.appName() != null) {
        try (var stmt = conn.prepareStatement(claimSql)) {
          stmt.setString(1, ctx.appName());
          stmt.setString(2, versionName);
          claimed = stmt.executeUpdate();
        }
      }
      if (claimed == 0) {
        try (var stmt = conn.prepareStatement(insertSql)) {
          stmt.setString(1, UUID.randomUUID().toString());
          stmt.setString(2, versionName);
          stmt.setString(3, ctx.appName());
          stmt.executeUpdate();
        }
      }
      // Read back: the writes above are silent about why they declined to claim.
      RowOwner.resolve(
          conn,
          ctx.schema(),
          "application_versions",
          "version_name",
          versionName,
          ctx.appName(),
          "Application version");
    }
  }

  /**
   * Promotes a version to latest. Promoting a peer's is a collision, not a retiming; promotion also
   * claims an unclaimed row, which would otherwise read as every peer's latest.
   */
  public static void updateApplicationVersionTimestamp(
      DbContext ctx, String versionName, Instant newTimestamp) throws SQLException {
    try (var conn = ctx.getConnection()) {
      var owner =
          RowOwner.resolve(
              conn,
              ctx.schema(),
              "application_versions",
              "version_name",
              versionName,
              ctx.appName(),
              "Application version");
      // Scoped to the row this writer resolved to: once version_name is no longer globally unique,
      // a bare name match would retime every peer's version of the same name.
      String sql =
          """
            UPDATE "%s".application_versions
            SET version_timestamp = ?, application_name = ?
            WHERE version_name = ?
              AND (application_name IS NULL%s)
          """
              .formatted(ctx.schema(), owner == null ? "" : " OR application_name = ?");
      try (var stmt = conn.prepareStatement(sql)) {
        stmt.setLong(1, newTimestamp.toEpochMilli());
        stmt.setString(2, owner);
        stmt.setString(3, versionName);
        if (owner != null) {
          stmt.setString(4, owner);
        }
        stmt.executeUpdate();
      }
    }
  }

  public static List<VersionInfo> listApplicationVersions(DbContext ctx) throws SQLException {
    String sql =
        """
          SELECT version_id, version_name, version_timestamp, created_at, application_name
          FROM "%s".application_versions
        """
                .formatted(ctx.schema())
            + ctx.whereAppScope()
            + " ORDER BY version_timestamp DESC";
    List<VersionInfo> results = new ArrayList<>();
    try (var conn = ctx.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      ctx.bindAppScope(stmt, 1);
      try (var rs = stmt.executeQuery()) {
        while (rs.next()) {
          results.add(versionFromResultSet(rs));
        }
      }
    }
    return results;
  }

  public static VersionInfo getLatestApplicationVersion(DbContext ctx) throws SQLException {
    String sql =
        """
          SELECT version_id, version_name, version_timestamp, created_at, application_name
          FROM "%s".application_versions
        """
                .formatted(ctx.schema())
            + ctx.whereAppScope()
            + " ORDER BY version_timestamp DESC LIMIT 1";
    try (var conn = ctx.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      ctx.bindAppScope(stmt, 1);
      try (var rs = stmt.executeQuery()) {
        if (rs.next()) {
          return versionFromResultSet(rs);
        }
      }
    }
    throw new RuntimeException("No application versions found");
  }

  private static VersionInfo versionFromResultSet(java.sql.ResultSet rs) throws SQLException {
    return new VersionInfo(
        rs.getString("version_id"),
        rs.getString("version_name"),
        Instant.ofEpochMilli(rs.getLong("version_timestamp")),
        Instant.ofEpochMilli(rs.getLong("created_at")),
        rs.getString("application_name"));
  }
}
