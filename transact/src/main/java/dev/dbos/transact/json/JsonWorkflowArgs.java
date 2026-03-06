package dev.dbos.transact.json;

import java.util.Map;

/**
 * Portable representation of workflow arguments. This format can be serialized/deserialized by any
 * language.
 */
public record JsonWorkflowArgs(Object[] positionalArgs, Map<String, Object> namedArgs) {

  public JsonWorkflowArgs() {
    this(null, null);
  }

  public JsonWorkflowArgs(Object[] positionalArgs) {
    this(positionalArgs, null);
  }
}
