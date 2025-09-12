package dev.dbos.transact.migrations;

public class MigrationFile {
  private final String filename;
  private final String sql;

  public MigrationFile(String filename, String sql) {
    this.filename = filename;
    this.sql = sql;
  }

  public String getFilename() {
    return filename;
  }

  public String getSql() {
    return sql;
  }
}
