package dev.dbos.transact.database.dao;

import dev.dbos.transact.exceptions.DBOSApplicationNameConflictException;

import java.sql.Connection;
import java.sql.SQLException;

import org.jspecify.annotations.Nullable;

/**
 * Ownership of the system-database rows applications address by name: queues, schedules, and
 * application versions. Unlike a listing, which quietly narrows to what an application owns, a
 * write to a name another application holds is a collision and raises.
 */
final class RowOwner {

  private RowOwner() {}

  /**
   * The owner to persist when writing a row that may already exist. A nameless writer leaves the
   * existing owner intact; a named one collides only with a different name, and claims a row nobody
   * owns.
   *
   * @param kind what to call the object in an error message, capitalised: {@code Queue}, {@code
   *     Schedule}, {@code Application version}
   */
  static @Nullable String resolve(
      Connection conn,
      String schema,
      String table,
      String keyColumn,
      String name,
      @Nullable String owner,
      String kind)
      throws SQLException {
    var sql =
        "SELECT application_name FROM \"%s\".%s WHERE %s = ?".formatted(schema, table, keyColumn);
    String current = null;
    try (var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, name);
      try (var rs = stmt.executeQuery()) {
        if (rs.next()) {
          current = rs.getString("application_name");
        }
      }
    }
    if (current == null) {
      return owner;
    }
    if (owner == null || current.equals(owner)) {
      return current;
    }
    // A version name is computed or pinned, so "pick another" is configuration advice, not a
    // rename.
    var remedy =
        kind.equals("Application version")
            ? "set a distinct applicationVersion for '%s'".formatted(owner)
            : "give '%s' a different %s name".formatted(owner, kind.toLowerCase());
    throw new DBOSApplicationNameConflictException(kind, name, current, owner, remedy);
  }
}
