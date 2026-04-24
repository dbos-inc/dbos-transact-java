package dev.dbos.transact.json;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

public class JavaSerializerTest {

  record Address(String city, String zip) {}

  record Person(String name, int age, Address address) {}

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

  @Test
  public void testNull() throws Exception {
    var serialized = DBOSJavaSerializer.INSTANCE.stringify(null);
    var deserialized = DBOSJavaSerializer.INSTANCE.parse(serialized);
    assertNull(deserialized);
  }

  @Test
  public void testString() throws Exception {
    String value = "hello world";
    var serialized = DBOSJavaSerializer.INSTANCE.stringify(value);
    var deserialized = DBOSJavaSerializer.INSTANCE.parse(serialized);
    assertEquals(String.class, deserialized.getClass());
    assertEquals(value, deserialized);
  }

  @Test
  public void testList() throws Exception {
    List<String> value = Arrays.asList("apple", "banana", "cherry");
    var serialized = DBOSJavaSerializer.INSTANCE.stringify(value);
    var deserialized = DBOSJavaSerializer.INSTANCE.parse(serialized);
    assertInstanceOf(List.class, deserialized);
    assertEquals(value, deserialized);
  }

  @Test
  public void testMap() throws Exception {
    Map<String, Integer> value = new HashMap<>();
    value.put("one", 1);
    value.put("two", 2);
    var serialized = DBOSJavaSerializer.INSTANCE.stringify(value);
    var deserialized = DBOSJavaSerializer.INSTANCE.parse(serialized);
    assertInstanceOf(Map.class, deserialized);
    assertEquals(value, deserialized);
  }

  @Test
  public void testNestedPojo() throws Exception {
    Person value = new Person("Alice", 30, new Address("Seattle", "98101"));
    var serialized = DBOSJavaSerializer.INSTANCE.stringify(value);
    var deserialized = DBOSJavaSerializer.INSTANCE.parse(serialized);
    assertInstanceOf(Person.class, deserialized);
    assertEquals(value, deserialized);
  }

  @Test
  public void testIntArray() throws Exception {
    int[] value = {1, 2, 3, 4};
    var serialized = DBOSJavaSerializer.INSTANCE.stringify(value);
    var deserialized = DBOSJavaSerializer.INSTANCE.parse(serialized);
    assertInstanceOf(int[].class, deserialized);
    assertArrayEquals(value, (int[]) deserialized);
  }

  @Test
  public void testInstantInObjectArray() throws Exception {
    Object[] value = {Instant.parse("2024-01-01T00:00:00Z"), Instant.parse("2024-01-02T00:00:00Z")};
    var serialized = DBOSJavaSerializer.INSTANCE.stringify(value);
    var deserialized = (Object[]) DBOSJavaSerializer.INSTANCE.parse(serialized);
    assertEquals(2, deserialized.length);
    assertInstanceOf(Instant.class, deserialized[0]);
    assertInstanceOf(Instant.class, deserialized[1]);
    assertEquals(value[0], deserialized[0]);
    assertEquals(value[1], deserialized[1]);
  }

  @Test
  public void testNestedListOfMaps() throws Exception {
    Map<String, Object> m1 = new HashMap<>();
    m1.put("name", "Alice");
    m1.put("age", 30);
    List<Map<String, Object>> value = List.of(m1);
    var serialized = DBOSJavaSerializer.INSTANCE.stringify(value);
    var deserialized = DBOSJavaSerializer.INSTANCE.parse(serialized);
    assertInstanceOf(List.class, deserialized);
    assertEquals(value, deserialized);
  }

  @Test
  public void testMixedObjectArrayWithList() throws Exception {
    Object[] value = {"abc", 123, true, Arrays.asList("x", "y")};
    var serialized = DBOSJavaSerializer.INSTANCE.stringify(value);
    var deserialized = (Object[]) DBOSJavaSerializer.INSTANCE.parse(serialized);
    assertArrayEquals(value, deserialized);
  }

  @Test
  public void testStringifyParseThrowable() throws Exception {
    var original = new RuntimeException("something went wrong");
    var serialized = DBOSJavaSerializer.INSTANCE.stringifyThrowable(original);
    var deserialized = DBOSJavaSerializer.INSTANCE.parseThrowable(serialized);
    assertInstanceOf(RuntimeException.class, deserialized);
    assertEquals(original.getMessage(), deserialized.getMessage());
  }

  @Test
  public void testStringifyParseThrowableWithCause() throws Exception {
    var cause = new IllegalArgumentException("bad input");
    var original = new RuntimeException("wrapper", cause);
    var serialized = DBOSJavaSerializer.INSTANCE.stringifyThrowable(original);
    var deserialized = DBOSJavaSerializer.INSTANCE.parseThrowable(serialized);
    assertInstanceOf(RuntimeException.class, deserialized);
    assertEquals(original.getMessage(), deserialized.getMessage());
    assertInstanceOf(IllegalArgumentException.class, deserialized.getCause());
    assertEquals(cause.getMessage(), deserialized.getCause().getMessage());
  }

  @Test
  public void testParseThrowableNull() throws Exception {
    assertNull(DBOSJavaSerializer.INSTANCE.parseThrowable(null));
  }

  // DBOSPortableSerializer throwable tests

  @Test
  public void testPortableStringifyParseThrowable() throws Exception {
    var original = new RuntimeException("something went wrong");
    var serialized = DBOSPortableSerializer.INSTANCE.stringifyThrowable(original);
    var deserialized = DBOSPortableSerializer.INSTANCE.parseThrowable(serialized);
    assertInstanceOf(PortableWorkflowException.class, deserialized);
    var pwe = (PortableWorkflowException) deserialized;
    assertEquals("RuntimeException", pwe.getErrorName());
    assertEquals(original.getMessage(), pwe.getMessage());
  }

  @Test
  public void testPortableStringifyParsePortableWorkflowException() throws Exception {
    var original =
        new PortableWorkflowException("not found", "WorkflowError", 404, Map.of("id", "abc"));
    var serialized = DBOSPortableSerializer.INSTANCE.stringifyThrowable(original);
    var deserialized = DBOSPortableSerializer.INSTANCE.parseThrowable(serialized);
    assertInstanceOf(PortableWorkflowException.class, deserialized);
    var pwe = (PortableWorkflowException) deserialized;
    assertEquals("WorkflowError", pwe.getErrorName());
    assertEquals("not found", pwe.getMessage());
    assertEquals(404, pwe.getCode());
    assertNotNull(pwe.getData());
  }

  @Test
  public void testPortableParseThrowableNull() throws Exception {
    assertNull(DBOSPortableSerializer.INSTANCE.parseThrowable(null));
  }

  // KryoSerializer tests

  @Test
  public void testKryoNull() throws Exception {
    assertNull(KryoSerializer.INSTANCE.parse(null));
  }

  @Test
  public void testKryoString() throws Exception {
    String value = "hello kryo";
    var serialized = KryoSerializer.INSTANCE.stringify(value);
    assertEquals(value, KryoSerializer.INSTANCE.parse(serialized));
  }

  @Test
  public void testKryoPrimitives() throws Exception {
    Object[] values = {42, 1234567890123L, 3.3f, 3.14159, true};
    var serialized = KryoSerializer.INSTANCE.stringify(values);
    var deserialized = (Object[]) KryoSerializer.INSTANCE.parse(serialized);
    assertEquals(values.length, deserialized.length);
    for (int i = 0; i < values.length; i++) {
      assertEquals(values[i], deserialized[i]);
    }
  }

  @Test
  public void testKryoNestedPojo() throws Exception {
    Person value = new Person("Alice", 30, new Address("Seattle", "98101"));
    var serialized = KryoSerializer.INSTANCE.stringify(value);
    assertEquals(value, KryoSerializer.INSTANCE.parse(serialized));
  }

  @Test
  public void testKryoThrowable() throws Exception {
    var original = new RuntimeException("kryo error");
    var serialized = KryoSerializer.INSTANCE.stringifyThrowable(original);
    var deserialized = KryoSerializer.INSTANCE.parseThrowable(serialized);
    assertInstanceOf(RuntimeException.class, deserialized);
    assertEquals(original.getMessage(), deserialized.getMessage());
  }

  @Test
  public void testKryoParseThrowableNull() throws Exception {
    assertNull(KryoSerializer.INSTANCE.parseThrowable(null));
  }
}
