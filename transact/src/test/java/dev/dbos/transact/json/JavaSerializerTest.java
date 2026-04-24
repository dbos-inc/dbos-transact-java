package dev.dbos.transact.json;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class JavaSerializerTest {

  @Test
  public void testFloat() throws Exception {
    float value = 3.3f;
    var serialized = DBOSJavaSerializer.INSTANCE.stringify(value);
    var deserialized = DBOSJavaSerializer.INSTANCE.parse(serialized);
    assertEquals(Float.class, deserialized.getClass());
    assertEquals(value, deserialized);
  }

  @Test
  public void testPrimitiveObjectArray() throws Exception {
    Object[] values = {42, 1234567890123L, 3.3f, 3.14159, true, false};
    var serialized = DBOSJavaSerializer.INSTANCE.stringify(values);
    var deserialized = DBOSJavaSerializer.INSTANCE.parse(serialized);
    // Should deserialize to Object[]
    assertEquals(Object[].class, deserialized.getClass());
    Object[] arr = (Object[]) deserialized;
    assertEquals(values.length, arr.length);
    assertEquals(Integer.class, arr[0].getClass());
    assertEquals(Long.class, arr[1].getClass());
    assertEquals(Float.class, arr[2].getClass());
    assertEquals(Double.class, arr[3].getClass());
    assertEquals(Boolean.class, arr[4].getClass());
    assertEquals(Boolean.class, arr[5].getClass());
    for (int i = 0; i < values.length; i++) {
      assertEquals(values[i], arr[i]);
    }
  }

  @Test
  public void testInt() throws Exception {
    int value = 42;
    var serialized = DBOSJavaSerializer.INSTANCE.stringify(value);
    var deserialized = DBOSJavaSerializer.INSTANCE.parse(serialized);
    assertEquals(Integer.class, deserialized.getClass());
    assertEquals(value, deserialized);
  }

  @Test
  public void testLong() throws Exception {
    long value = 1234567890123L;
    var serialized = DBOSJavaSerializer.INSTANCE.stringify(value);
    var deserialized = DBOSJavaSerializer.INSTANCE.parse(serialized);
    assertEquals(Long.class, deserialized.getClass());
    assertEquals(value, deserialized);
  }

  @Test
  public void testDouble() throws Exception {
    double value = 3.14159;
    var serialized = DBOSJavaSerializer.INSTANCE.stringify(value);
    var deserialized = DBOSJavaSerializer.INSTANCE.parse(serialized);
    assertEquals(Double.class, deserialized.getClass());
    assertEquals(value, deserialized);
  }

  @Test
  public void testBooleanTrue() throws Exception {
    boolean value = true;
    var serialized = DBOSJavaSerializer.INSTANCE.stringify(value);
    var deserialized = DBOSJavaSerializer.INSTANCE.parse(serialized);
    assertEquals(Boolean.class, deserialized.getClass());
    assertEquals(value, deserialized);
  }

  @Test
  public void testBooleanFalse() throws Exception {
    boolean value = false;
    var serialized = DBOSJavaSerializer.INSTANCE.stringify(value);
    var deserialized = DBOSJavaSerializer.INSTANCE.parse(serialized);
    assertEquals(Boolean.class, deserialized.getClass());
    assertEquals(value, deserialized);
  }
}
