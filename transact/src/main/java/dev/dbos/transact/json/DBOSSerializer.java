package dev.dbos.transact.json;

/**
 * Generic serializer interface for DBOS. Implementations must be able to serialize any value to a
 * string and deserialize it back.
 */
public interface DBOSSerializer {
  /**
   * Return a name for the serialization format. This name is stored in the database to identify how
   * data was serialized.
   */
  String name();

  /**
   * Serialize a value to a string.
   *
   * @param value The value to serialize
   * @param noHistoricalWrapper The value is not expected to have a wrapper enclosing array
   * @return The serialized string representation
   */
  String stringify(Object value, boolean noHistoricalWrapper);

  /**
   * Deserialize a string back to a value.
   *
   * @param text A serialized string (potentially null)
   * @param noHistoricalWrapper The value is not expected to have a wrapper enclosing array
   * @return The deserialized value, or null if the input was null
   */
  Object parse(String text, boolean noHistoricalWrapper);
}
