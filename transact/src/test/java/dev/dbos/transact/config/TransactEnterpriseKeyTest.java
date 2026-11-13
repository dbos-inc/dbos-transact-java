package dev.dbos.transact.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class TransactEnterpriseKeyTest {

  @Test
  public void theKeyIsUnsetByDefault() {
    assertNull(DBOSConfig.defaults("app").transactEnterpriseKey());
    assertNull(DBOSConfig.defaultsFromEnv("app").transactEnterpriseKey());
  }

  @Test
  public void theKeyIsKeptThroughCopiesAndOtherSettings() {
    var config = DBOSConfig.defaults("app").withTransactEnterpriseKey("enterprise-key");
    assertEquals("enterprise-key", config.transactEnterpriseKey());
    assertEquals("enterprise-key", new DBOSConfig(config).transactEnterpriseKey());
    var changed =
        config
            .withConductorKey("conductor-key")
            .withConductorMetadataOnlyMode(true)
            .withAppName("x");
    assertEquals("enterprise-key", changed.transactEnterpriseKey());
    assertNull(changed.withTransactEnterpriseKey(null).transactEnterpriseKey());
  }

  @Test
  public void aBlankKeyIsUnset() {
    // So an empty placeholder, such as an unset environment variable, falls back to the Conductor
    // key instead of failing.
    for (var blank : new String[] {"", " ", "\n"}) {
      assertNull(
          DBOSConfig.defaults("app").withTransactEnterpriseKey(blank).transactEnterpriseKey());
    }
  }

  @Test
  public void theKeyIsMaskedInToString() {
    var text = DBOSConfig.defaults("app").withTransactEnterpriseKey("enterprise-key").toString();
    assertFalse(text.contains("enterprise-key"), text);
    assertTrue(text.contains("transactEnterpriseKey=***"), text);
    assertTrue(
        DBOSConfig.defaults("app").toString().contains("transactEnterpriseKey=null"),
        DBOSConfig.defaults("app").toString());
  }
}
