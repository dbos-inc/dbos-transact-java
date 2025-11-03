/**
 * Utility class providing test access to database configuration.
 * This class exposes package-private or protected methods from the database layer
 * for testing purposes, allowing test classes to access internal configuration
 * without breaking encapsulation in production code.
 */
package dev.dbos.transact.database;

import com.zaxxer.hikari.HikariConfig;


public class DBTestAccess {
  public static HikariConfig getHikariConfig(SystemDatabase sysdb) {
    return sysdb.getConfig();
  }
}
