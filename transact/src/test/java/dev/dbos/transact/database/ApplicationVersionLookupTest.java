package dev.dbos.transact.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.dao.ApplicationVersionDAO;
import dev.dbos.transact.migrations.MigrationManager;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.VersionInfo;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import com.zaxxer.hikari.HikariDataSource;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * The latest application version is found with bounded index probes, however long the version
 * history grows: this application's newest and the newest unclaimed one, or the newest of all for a
 * nameless owner.
 */
public class ApplicationVersionLookupTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();

  DBOSConfig dbosConfig;
  @AutoClose HikariDataSource dataSource;
  String schema;

  @BeforeEach
  void beforeEach() throws Exception {
    dbosConfig = pgContainer.dbosConfig();
    MigrationManager.runMigrations(dbosConfig);
    dataSource = pgContainer.dataSource();
    try (var sysdb = systemDatabase(null)) {
      schema = sysdb.context().schema();
    }
  }

  /**
   * Registers {@code 3 * perOwner} versions, numbered in timestamp order: the oldest third this
   * application's, the next unclaimed, and the newest a peer's, so a global timestamp scan with an
   * owner filter is insufficient.
   */
  private void seedVersions(int perOwner) throws Exception {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute(
          """
          INSERT INTO "%s".application_versions
            (version_id, version_name, version_timestamp, created_at, application_name)
          SELECT i::text, 'version-' || i, i, i,
                 CASE WHEN i <= %d THEN 'ours' WHEN i <= %d THEN NULL ELSE 'peer' END
          FROM generate_series(1, %d) AS i
          """
              .formatted(schema, perOwner, 2 * perOwner, 3 * perOwner));
      if (!PgContainer.USE_COCKROACH_DB) {
        // The pooled database is cleared with DELETE, which leaves the previous run's dead rows
        // under the same keys for the probes to step over.
        stmt.execute("VACUUM ANALYZE \"%s\".application_versions".formatted(schema));
      }
    }
  }

  private SystemDatabase systemDatabase(@Nullable String owner) {
    return SystemDatabase.create(dbosConfig, null, owner);
  }

  @ParameterizedTest
  @CsvSource(
      value = {"ours, 2", "absent, 2", "peer, 3", "NULL, 3"},
      nullValues = "NULL")
  void findsTheLatestWithoutScanningVersionHistory(@Nullable String owner, int expectedThird)
      throws Exception {
    // A history long enough to tell an index probe from a scan, on Postgres, where EXPLAIN's JSON
    // output can show which ran.
    int perOwner = PgContainer.USE_COCKROACH_DB ? 10 : 10_000;
    seedVersions(perOwner);
    try (var sysdb = systemDatabase(owner)) {
      assertEquals(
          "version-" + expectedThird * perOwner, sysdb.getLatestApplicationVersion().versionName());
      if (PgContainer.USE_COCKROACH_DB) {
        return;
      }
      var ctx = sysdb.context();
      var sql =
          "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "
              + ApplicationVersionDAO.latestApplicationVersionSql(ctx);
      JsonNode plan;
      try (var conn = dataSource.getConnection();
          var stmt = conn.prepareStatement(sql)) {
        ctx.bindAppScope(stmt, 1);
        try (var rs = stmt.executeQuery()) {
          assertTrue(rs.next());
          plan = JsonMapper.builder().build().readTree(rs.getString(1)).get(0).get("Plan");
        }
      }
      assertTrue(
          plan.get("Shared Hit Blocks").asInt() + plan.get("Shared Read Blocks").asInt() < 32,
          plan.toString());
      var scans =
          planNodes(plan).stream()
              .filter(node -> node.get("Node Type").stringValue().contains("Scan"))
              .toList();
      assertTrue(!scans.isEmpty(), plan.toString());
      for (var scan : scans) {
        assertTrue(scan.get("Node Type").stringValue().contains("Index"), scan.toString());
        assertTrue(
            scan.get("Actual Rows").asDouble() * scan.get("Actual Loops").asDouble() <= 1,
            scan.toString());
        var removed = scan.get("Rows Removed by Filter");
        assertEquals(0, removed == null ? 0 : removed.asDouble(), scan.toString());
      }
    }
  }

  @Test
  void promotionAndReRegistrationPreserveVersionIdentityAndCreationTime() throws Exception {
    seedVersions(10);
    try (var sysdb = systemDatabase("ours")) {
      sysdb.updateApplicationVersionTimestamp("version-1", Instant.ofEpochMilli(100), null);
      var promoted = sysdb.getLatestApplicationVersion();
      assertEquals(
          new VersionInfo(
              "1", "version-1", Instant.ofEpochMilli(100), Instant.ofEpochMilli(1), "ours"),
          promoted);
      sysdb.createApplicationVersion("version-1", null);
      assertEquals(promoted, sysdb.getLatestApplicationVersion());
    }
  }

  @Test
  void anEmptyScopeKeepsTheMissingVersionError() throws Exception {
    seedVersions(10);
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute(
          "DELETE FROM \"%s\".application_versions WHERE application_name IS NULL"
              .formatted(schema));
    }
    try (var sysdb = systemDatabase("absent")) {
      var e = assertThrows(RuntimeException.class, sysdb::getLatestApplicationVersion);
      assertEquals("No application versions found", e.getMessage());
    }
  }

  private static List<JsonNode> planNodes(JsonNode plan) {
    var nodes = new ArrayList<JsonNode>();
    nodes.add(plan);
    var children = plan.get("Plans");
    if (children != null) {
      for (var child : children) {
        nodes.addAll(planNodes(child));
      }
    }
    return nodes;
  }
}
