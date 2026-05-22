package dev.dbos.transact.workflow;

import org.jspecify.annotations.Nullable;

/**
 * Three-state field for partial updates: absent (don't touch), present with a value, or present
 * with null (clear the column).
 */
public sealed interface Field<T> permits Field.Absent, Field.Present {

  record Absent<T>() implements Field<T> {}

  record Present<T>(@Nullable T value) implements Field<T> {}

  static <T> Field<T> absent() {
    return new Absent<>();
  }

  static <T> Field<T> of(@Nullable T value) {
    return new Present<>(value);
  }

  default boolean isPresent() {
    return this instanceof Present;
  }

  default @Nullable T get() {
    if (this instanceof Present<T> p) return p.value();
    throw new IllegalStateException("Field.get() called on an absent field");
  }
}
