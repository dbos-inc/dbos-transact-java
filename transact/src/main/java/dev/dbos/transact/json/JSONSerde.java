package dev.dbos.transact.json;

public interface JSONSerde {
  String serializeArray(Object[] args);

  Object[] deserializeToArray(String json);

  String toJson(Object obj);

  <T> T fromJson(String content, Class<T> valueType);
}
