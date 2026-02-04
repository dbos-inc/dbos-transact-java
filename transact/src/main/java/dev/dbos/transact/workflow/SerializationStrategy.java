package dev.dbos.transact.workflow;

import dev.dbos.transact.json.SerializationUtil;

/**
 * Serialization strategy for workflow arguments and messages.
 *
 * <p>This enum represents the strategic choice of serialization format at the client level. The
 * actual serialization format name used in the database is determined by the strategy:
 *
 * <ul>
 *   <li>{@link #DEFAULT} - Uses the default format for this language (native Java serialization)
 *   <li>{@link #PORTABLE} - Uses portable JSON format for cross-language compatibility
 *   <li>{@link #NATIVE} - Explicitly uses the native format for this language
 * </ul>
 */
public enum SerializationStrategy {
  /**
   * Use the default serialization for this language. For Java, this is the native Java
   * serialization format ({@code java_jackson}).
   */
  DEFAULT(null),

  /**
   * Use portable JSON serialization ({@code portable_json}). This format is compatible across
   * languages and should be used when workflows may be initiated or consumed by applications
   * written in different languages (e.g., TypeScript, Python).
   */
  PORTABLE(SerializationUtil.PORTABLE),

  /**
   * Explicitly use the native serialization format for this language. For Java, this is {@code
   * java_jackson}. This is equivalent to {@link #DEFAULT} but makes the choice explicit.
   */
  NATIVE(SerializationUtil.NATIVE);

  private final String formatName;

  SerializationStrategy(String formatName) {
    this.formatName = formatName;
  }

  /**
   * Get the serialization format name to use in the database.
   *
   * @return the format name, or null for DEFAULT (which lets the lower layers decide)
   */
  public String formatName() {
    return formatName;
  }
}
