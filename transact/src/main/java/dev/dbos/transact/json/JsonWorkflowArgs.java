package dev.dbos.transact.json;

import java.util.List;
import java.util.Map;

/**
 * Portable representation of workflow arguments. This format can be serialized/deserialized by any
 * language.
 */
public record JsonWorkflowArgs(List<Object> positionalArgs, Map<String, Object> namedArgs) {

  public JsonWorkflowArgs() {
    this(null, null);
  }

  public JsonWorkflowArgs(List<Object> positionalArgs) {
    this(positionalArgs, null);
  }
}
