package dev.dbos.transact.json;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

@org.junit.jupiter.api.Timeout(value = 2, unit = TimeUnit.MINUTES)
class JSONUtilTest {

  @Test
  void testPrimitives() {

    DiffPrimitiveTypes d = new DiffPrimitiveTypes(23, 2467L, 73.63f, 39.31234, true);
    String dString = JSONUtil.serialize(d);
    System.out.println(dString);
    Object[] dser = JSONUtil.deserializeToArray(dString);
    DiffPrimitiveTypes dFromString = (DiffPrimitiveTypes) dser[0];
    assertEquals(23, dFromString.intValue);
    assertEquals(2467L, dFromString.longValue);
    assertEquals(73.63f, dFromString.floatValue);
    assertEquals(39.31234, dFromString.doubleValue);
    assertEquals(true, dFromString.boolValue);
  }

  @Test
  void testPrimitiveObjects() {

    PrimitiveObjects d = new PrimitiveObjects(23, 2467L, 73.63f, 39.31234, true);
    String dString = JSONUtil.serialize(d);
    System.out.println(dString);
    Object[] dser = JSONUtil.deserializeToArray(dString);
    PrimitiveObjects dFromString = (PrimitiveObjects) dser[0];
    assertEquals(23, dFromString.intValue);
    assertEquals(2467L, dFromString.longValue);
    assertEquals(73.63f, dFromString.floatValue);
    assertEquals(39.31234, dFromString.doubleValue);
    assertEquals(true, dFromString.boolValue);
  }

  @Test
  public void testNestedClassSerialization() {
    Person p = new Person("Alice", 30, new Person.Address("Seattle", "98101"));
    String json = JSONUtil.serialize(p);
    System.out.println(json);
    Object[] deserialized = JSONUtil.deserializeToArray(json);

    assertTrue(deserialized[0] instanceof Person);
    assertEquals(p, deserialized[0]);
  }

  @Test
  public void testArraySerialization() {
    int[] nums = {1, 2, 3, 4};
    String json = JSONUtil.serialize(nums);
    System.out.println(json);
    Object[] dser = JSONUtil.deserializeToArray(json);
    int[] deserialized = (int[]) dser[0];
    // int[] deserialized = JSONUtil.deserialize(json, int[].class); // <-- change
    // here
    assertArrayEquals(nums, deserialized);
  }

  @Test
  public void testListSerialization() {
    List<String> list = Arrays.asList("apple", "banana", "cherry");
    String json = JSONUtil.serialize(list);
    System.out.println(json);
    Object[] deserialized = JSONUtil.deserializeToArray(json);

    assertTrue(deserialized[0] instanceof List);
    assertEquals(list, deserialized[0]);
  }

  @Test
  public void testMapSerialization() {
    Map<String, Integer> map = new HashMap<>();
    map.put("one", 1);
    map.put("two", 2);

    String json = JSONUtil.serialize(map);
    Object[] deserialized = JSONUtil.deserializeToArray(json);

    assertTrue(deserialized[0] instanceof Map);
    assertEquals(map, deserialized[0]);
  }

  @Test
  public void testNullSerialization() {
    String json = JSONUtil.serialize(null);
    Object[] deserialized = JSONUtil.deserializeToArray(json);
    assertNull(deserialized[0]);
  }

  @Test
  public void testNestedListOfMaps() {
    List<Map<String, Object>> list = new ArrayList<>();
    Map<String, Object> m1 = new HashMap<>();
    m1.put("name", "Alice");
    m1.put("age", 30);
    list.add(m1);

    String json = JSONUtil.serialize(list);
    Object[] deserialized = JSONUtil.deserializeToArray(json);

    assertTrue(deserialized[0] instanceof List);
    assertEquals(list, deserialized[0]);
  }

  @Test
  public void testMixedArrayTypes() {
    Object[] mixed = {"abc", 123, true, Arrays.asList("x", "y")};
    String json = JSONUtil.serializeArray(mixed);
    Object[] deserialized = JSONUtil.deserializeToArray(json);

    assertTrue(deserialized instanceof Object[]);
    assertArrayEquals(mixed, deserialized);
  }

  @Test
  public void testInstantArraySerialization() {
    Object[] args =
        new Object[] {Instant.parse("2024-01-01T00:00:00Z"), Instant.parse("2024-01-02T00:00:00Z")};
    String json = JSONUtil.serializeArray(args);
    Object[] restored = JSONUtil.deserializeToArray(json);

    assertEquals(2, restored.length);
    assertTrue(restored[0] instanceof Instant);
    assertTrue(restored[1] instanceof Instant);
    assertEquals(args[0], restored[0]);
    assertEquals(args[1], restored[1]);
  }
}
