package dev.dbos.transact.json;

/**
 * Exception that can be serialized and deserialized portably across languages. Used when
 * deserializing errors from the portable JSON format.
 */
public class PortableWorkflowException extends RuntimeException {
  private final String errorName;
  private final Object code;
  private final Object data;

  public PortableWorkflowException(String message, String errorName, Object code, Object data) {
    super(message);
    this.errorName = errorName;
    this.code = code;
    this.data = data;
  }

  public PortableWorkflowException(String message, String errorName) {
    this(message, errorName, null, null);
  }

  public String getErrorName() {
    return errorName;
  }

  public Object getCode() {
    return code;
  }

  public Object getData() {
    return data;
  }

  /** Create from portable error data. */
  public static PortableWorkflowException fromErrorData(JsonWorkflowErrorData errorData) {
    return new PortableWorkflowException(
        errorData.message(), errorData.name(), errorData.code(), errorData.data());
  }

  /** Convert to portable error data. */
  public JsonWorkflowErrorData toErrorData() {
    return new JsonWorkflowErrorData(errorName, getMessage(), code, data);
  }
}
