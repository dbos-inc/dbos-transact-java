package dev.dbos.transact.exceptions;

/**
 * Thrown when a named object -- a queue, a schedule, or an application version -- is already
 * registered in the system database by a different application.
 *
 * <p>Applications sharing a system database are isolated by name, but the objects they register by
 * name are not: a name is an address, so two applications claiming the same one is a collision
 * neither of them can resolve. Give this application's object a different name, or, if the row
 * belongs to this application under a former name, re-own it first.
 */
public class DBOSApplicationNameConflictException extends RuntimeException {

  private final String kind;
  private final String name;
  private final String owner;

  public DBOSApplicationNameConflictException(
      String kind, String name, String owner, String claimant, String remedy) {
    super(
        "%s '%s' is already registered by application '%s' in this system database. %s names must be unique across applications sharing a system database. Either %s, or, if '%s' was renamed to '%s', re-own its rows first with the dbos rename-application command."
            .formatted(kind, name, owner, kind, remedy, owner, claimant));
    this.kind = kind;
    this.name = name;
    this.owner = owner;
  }

  /**
   * What kind of object collided: {@code Queue}, {@code Schedule}, or {@code Application version}.
   */
  public String kind() {
    return kind;
  }

  /** The name both applications claim. */
  public String name() {
    return name;
  }

  /** The application already holding the name. */
  public String owner() {
    return owner;
  }
}
