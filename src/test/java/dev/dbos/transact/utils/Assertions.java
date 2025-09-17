package dev.dbos.transact.utils;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

public class Assertions {

  public static <K, V> void assertKeyIsNull(Map<K, V> map, K key) {
    assertTrue(map.containsKey(key));
    assertNull(map.get(key));
  }
}
