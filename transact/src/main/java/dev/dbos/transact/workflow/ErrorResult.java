package dev.dbos.transact.workflow;

import dev.dbos.transact.json.DBOSSerializer;
import dev.dbos.transact.json.PortableWorkflowException;
import dev.dbos.transact.json.SerializationUtil;

public record ErrorResult(
    String className,
    String message,
    String serializedError,
    String serialization,
    Throwable throwable) {

  public static ErrorResult fromThrowable(
      Throwable error, String serialization, DBOSSerializer serializer) {
    if (error == null) {
      return null;
    }
    var serializedError = SerializationUtil.serializeError(error, serialization, serializer);
    return deserialize(
        serializedError.serializedValue(), serializedError.serialization(), serializer);
  }

  public static ErrorResult deserialize(
      String serializedError, String serialization, DBOSSerializer serializer) {
    if (serializedError == null) return null;

    var desError = SerializationUtil.deserializeError(serializedError, serialization, serializer);
    if (desError instanceof PortableWorkflowException) {
      var e = (PortableWorkflowException) desError;
      return new ErrorResult(
          e.getErrorName(), e.getMessage(), serializedError, serialization, desError);
    }
    return new ErrorResult(
        desError.getClass().getName(),
        desError.getMessage(),
        serializedError,
        serialization,
        desError);
  }
}
