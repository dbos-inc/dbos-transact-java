package dev.dbos.transact.json;

import dev.dbos.transact.exceptions.DBOSAppException;
import dev.dbos.transact.exceptions.SerializableException;
import org.junit.jupiter.api.Test;

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
}