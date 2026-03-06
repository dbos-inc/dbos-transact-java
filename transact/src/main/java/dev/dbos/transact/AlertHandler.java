package dev.dbos.transact;

import java.util.Map;

@FunctionalInterface
public interface AlertHandler {
  void invoke(String name, String message, Map<String, String> metadata);
}
