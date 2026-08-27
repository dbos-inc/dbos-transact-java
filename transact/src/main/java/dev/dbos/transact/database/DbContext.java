package dev.dbos.transact.database;

import dev.dbos.transact.json.DBOSSerializer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.function.BooleanSupplier;

import javax.sql.DataSource;

import org.jspecify.annotations.Nullable;

public record DbContext(
    DataSource dataSource,
    String schema,
    DBOSSerializer serializer,
    BooleanSupplier closed,
    String executorId,
    @Nullable String appName,
    PollingLimiter pollingLimiter) {

  public Connection getConnection() throws SQLException {
    return dataSource.getConnection();
  }

  /**
   * Acquire a permit for one polling read, to be held across the connection it checks out. Only the
   * wait loops take one; see {@link PollingLimiter}.
   */
  public PollingLimiter.Permit acquirePollPermit() {
    return pollingLimiter.acquire();
  }

  /**
   * A predicate matching rows this application owns plus unclaimed ones, which belong to every
   * application, prefixed with {@code AND} so it appends to an existing WHERE clause. Empty for a
   * nameless owner -- a client configured with no application -- which sees every row. Bind its
   * parameter with {@link #bindAppScope}.
   */
  public String andAppScope() {
    return appName == null ? "" : " AND (application_name = ? OR application_name IS NULL)";
  }

  /** {@link #andAppScope} as a standalone WHERE clause, for a query with no other conditions. */
  public String whereAppScope() {
    return appName == null ? "" : " WHERE (application_name = ? OR application_name IS NULL)";
  }

  /** Binds {@link #andAppScope}'s parameter, if it has one, and returns the next free index. */
  public int bindAppScope(PreparedStatement stmt, int index) throws SQLException {
    if (appName == null) {
      return index;
    }
    stmt.setString(index, appName);
    return index + 1;
  }

  /**
   * The applications a listing is scoped to: the ones asked for, or this one by default. Null means
   * unscoped -- an explicitly empty request, or a nameless owner -- and lists every application's
   * rows.
   */
  public @Nullable List<String> scopeNames(@Nullable List<String> requested) {
    if (requested == null) {
      return appName == null ? null : List.of(appName);
    }
    return requested.isEmpty() ? null : requested;
  }

  /**
   * The applications an identity read is scoped to: exactly the ones asked for, and never defaulted
   * to this one. A workflow ID is a global address, so a read keyed by one answers for every
   * application until the caller narrows it. Null means unscoped -- unasked, or asked emptily.
   */
  public @Nullable List<String> requestedNames(@Nullable List<String> requested) {
    return requested == null || requested.isEmpty() ? null : requested;
  }

  public void checkClosed() {
    if (closed.getAsBoolean()) {
      throw new IllegalStateException("Database is closed");
    }
  }
}
