package dev.dbos.transact.json;

/**
 * Portable representation of workflow errors. This format can be serialized/deserialized by any
 * language.
 */
public record JsonWorkflowErrorData(String name, String message, Object code, Object data) {

  public JsonWorkflowErrorData(String name, String message) {
    this(name, message, null, null);
  }
}
