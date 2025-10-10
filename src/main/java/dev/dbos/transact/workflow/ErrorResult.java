package dev.dbos.transact.workflow;

import dev.dbos.transact.json.JSONUtil;

public record ErrorResult(
    String className, String message, String serializedError, Throwable throwable) {

  public static ErrorResult of(Throwable error) {
    String errorString = JSONUtil.serializeAppException(error);
    var wrapper = JSONUtil.deserializeAppExceptionWrapper(errorString);
    Throwable throwable = JSONUtil.deserializeAppException(errorString);
    return new ErrorResult(wrapper.type, wrapper.message, errorString, throwable);
  }
}
