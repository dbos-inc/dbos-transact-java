package dev.dbos.transact.internal;

import java.time.Duration;

import org.jspecify.annotations.Nullable;

public class Validation {

  public static boolean nullableIsEmpty(@Nullable String value) {
    return value != null && value.isEmpty();
  }

  public static boolean nullableIsNotPositive(@Nullable Duration value) {
    return value != null && (value.isNegative() || value.isZero());
  }
}
