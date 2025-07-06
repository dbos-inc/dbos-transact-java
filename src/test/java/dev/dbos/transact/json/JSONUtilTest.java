package dev.dbos.transact.json;

import com.fasterxml.jackson.core.type.TypeReference;
import dev.dbos.transact.exceptions.DBOSAppException;
import dev.dbos.transact.exceptions.SerializableException;
import org.junit.jupiter.api.Test;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class JSONUtilTest {

    @Test
    void testPrimitives() {

        DiffPrimitiveTypes d = new DiffPrimitiveTypes(23, 2467L, 73.63f, 39.31234, true) ;
        String dString = JSONUtil.serialize(d);
        System.out.println(dString);
        DiffPrimitiveTypes dFromString = (DiffPrimitiveTypes) JSONUtil.deserialize(dString);
        assertEquals(23, dFromString.intValue);
        assertEquals(2467L, dFromString.longValue);
        assertEquals(73.63f, dFromString.floatValue);
        assertEquals(39.31234, dFromString.doubleValue);
        assertEquals(true, dFromString.boolValue);

    }

    @Test
    void testPrimitiveObjects() {

        PrimitiveObjects d = new PrimitiveObjects(23, 2467L, 73.63f, 39.31234, true) ;
        String dString = JSONUtil.serialize(d);
        System.out.println(dString);
        PrimitiveObjects dFromString = (PrimitiveObjects) JSONUtil.deserialize(dString);
        assertEquals(23, dFromString.intValue);
        assertEquals(2467L, dFromString.longValue);
        assertEquals(73.63f, dFromString.floatValue);
        assertEquals(39.31234, dFromString.doubleValue);
        assertEquals(true, dFromString.boolValue);

    }

    @Test
    void testException() {
        TestException e = new TestException("A significant error");
        SerializableException dto = new SerializableException(e);
        String errStr = JSONUtil.serialize(dto);
        SerializableException fromStr = (SerializableException) JSONUtil.deserialize(errStr) ;
        assertEquals("A significant error", fromStr.message) ;

        try {
            String msg = "Remote Exception of type: " + fromStr.className;
            throw new DBOSAppException(msg, fromStr);
        } catch (Throwable t) {
            assertEquals("Remote Exception of type: dev.dbos.transact.json.TestException", t.getMessage());
        }

    }


    @Test
    public void testNestedClassSerialization() {
        Person p = new Person("Alice", 30, new Person.Address("Seattle", "98101"));
        String json = JSONUtil.serialize(p);
        Object deserialized = JSONUtil.deserialize(json);

        assertTrue(deserialized instanceof Person);
        assertEquals(p, deserialized);
    }

    @Test
    public void testArraySerialization() {
        int[] nums = {1, 2, 3, 4};
        String json = JSONUtil.serialize(nums);
        int[] deserialized = JSONUtil.deserialize(json, int[].class); // <-- change here
        assertArrayEquals(nums, deserialized);
    }

    @Test
    public void testListSerialization() {
        List<String> list = Arrays.asList("apple", "banana", "cherry");
        String json = JSONUtil.serialize(list);
        Object deserialized = JSONUtil.deserialize(json);

        assertTrue(deserialized instanceof List);
        assertEquals(list, deserialized);
    }

    @Test
    public void testMapSerialization() {
        Map<String, Integer> map = new HashMap<>();
        map.put("one", 1);
        map.put("two", 2);

        String json = JSONUtil.serialize(map);
        Object deserialized = JSONUtil.deserialize(json);

        assertTrue(deserialized instanceof Map);
        assertEquals(map, deserialized);
    }

    @Test
    public void testNullSerialization() {
        String json = JSONUtil.serialize(null);
        Object deserialized = JSONUtil.deserialize(json);
        assertNull(deserialized);
    }

    @Test
    public void testDeserializeWithTypeReference() {
        Map<String, List<Integer>> original = new HashMap<>();
        original.put("numbers", Arrays.asList(1, 2, 3, 4, 5));

        String json = JSONUtil.serialize(original);
        
        Map<String, List<Integer>> deserialized = JSONUtil.deserialize(
                json,
                new TypeReference<Map<String, List<Integer>>>() {}
        );

        // Assertions
        assertNotNull(deserialized);
        assertEquals(original.size(), deserialized.size());
        assertEquals(original.get("numbers"), deserialized.get("numbers"));
    }
}