package dev.dbos.transact.workflow;

import dev.dbos.transact.json.JSONUtil;

public record ErrorResult(
    String className, String message, String serializedError, Throwable throwable) {

  public static ErrorResult fromThrowable(Throwable error) {
    var serializedError = JSONUtil.serializeAppException(error);
    return deserialize(serializedError);
  }

  public static ErrorResult deserialize(String serializedError) {
    if (serializedError != null) {
      var wrapper = JSONUtil.deserializeAppExceptionWrapper(serializedError);
      Throwable throwable = JSONUtil.deserializeAppException(serializedError);
      return new ErrorResult(wrapper.type, wrapper.message, serializedError, throwable);
    } else {
      return null;
    }
  }
}
