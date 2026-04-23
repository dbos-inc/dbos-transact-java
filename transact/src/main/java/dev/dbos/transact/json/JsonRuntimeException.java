package dev.dbos.transact.json;

import java.io.IOException;

public class JsonRuntimeException extends RuntimeException {
    public JsonRuntimeException(IOException cause) {
      super(cause.getMessage(), cause);
      setStackTrace(cause.getStackTrace());
      for (var suppressed : cause.getSuppressed()) {
        addSuppressed(suppressed);
      }
    }
  }