package dev.dbos.transact.workflow;

import dev.dbos.transact.json.DBOSSerializer;
import dev.dbos.transact.json.JSONUtil;

public record ErrorResult(
    String className, String message, String serializedError, Throwable throwable) {

  public static ErrorResult fromThrowable(
      Throwable error, String serialization, DBOSSerializer serializer) {
    if (error == null) {
      return null;
    }
    var serializedError = JSONUtil.serializeAppException(error);
    return deserialize(serializedError, serialization, serializer);
  }

  public static ErrorResult deserialize(
      String serializedError, String serialization, DBOSSerializer serializer) {
    if (serializedError == null) return null;

    var wrapper = JSONUtil.deserializeAppExceptionWrapper(serializedError);
    Throwable throwable = JSONUtil.deserializeAppException(serializedError);
    return new ErrorResult(wrapper.type, wrapper.message, serializedError, throwable);
  }
}
