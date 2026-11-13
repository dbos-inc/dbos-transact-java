package dev.dbos.transact.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** The Conductor key and domain and the Transact Enterprise key, which DBOSConfig handles alike. */
public class DBOSConfigKeysTest {

  @Test
  public void theKeysAreUnsetByDefault() {
    for (var config :
        new DBOSConfig[] {DBOSConfig.defaults("app"), DBOSConfig.defaultsFromEnv("app")}) {
      assertNull(config.conductorKey());
      assertNull(config.transactEnterpriseKey());
    }
  }

  @Test
  public void theKeysAreKeptThroughCopiesAndOtherSettings() {
    var config =
        DBOSConfig.defaults("app")
            .withConductorKey("conductor-key")
            .withTransactEnterpriseKey("enterprise-key");
    var copy = new DBOSConfig(config);
    assertEquals("conductor-key", copy.conductorKey());
    assertEquals("enterprise-key", copy.transactEnterpriseKey());
    var changed = config.withConductorMetadataOnlyMode(true).withAppName("x");
    assertEquals("conductor-key", changed.conductorKey());
    assertEquals("enterprise-key", changed.transactEnterpriseKey());
    assertNull(changed.withTransactEnterpriseKey(null).transactEnterpriseKey());
    assertNull(changed.withConductorKey(null).conductorKey());
  }

  @Test
  public void aBlankKeyIsUnset() {
    // So an empty placeholder, such as an unset environment variable, means no key instead of
    // failing: no Conductor connection, and the Conductor key standing in for the enterprise key.
    for (var blank : new String[] {"", " ", "\n"}) {
      var config =
          DBOSConfig.defaults("app")
              .withConductorKey(blank)
              .withConductorDomain(blank)
              .withTransactEnterpriseKey(blank);
      assertNull(config.conductorKey(), "conductorKey " + blank);
      assertNull(config.conductorDomain(), "conductorDomain " + blank);
      assertNull(config.transactEnterpriseKey(), "transactEnterpriseKey " + blank);
    }
  }

  @Test
  public void surroundingWhitespaceIsRemoved() {
    // As read from a secret file that ends in a newline: the entitlement check and the Conductor
    // client must both see the value without it.
    var config =
        DBOSConfig.defaults("app")
            .withConductorKey(" conductor-key\n")
            .withConductorDomain(" ws://localhost:8090\r\n")
            .withTransactEnterpriseKey("enterprise-key\n");
    assertEquals("conductor-key", config.conductorKey());
    assertEquals("ws://localhost:8090", config.conductorDomain());
    assertEquals("enterprise-key", config.transactEnterpriseKey());
  }

  @Test
  public void credentialsInTheConductorDomainAreMaskedInToString() {
    var text =
        DBOSConfig.defaults("app")
            .withConductorDomain("wss://user:secret@conductor.example.com/conductor/v1alpha1")
            .toString();
    assertFalse(text.contains("secret"), text);
    assertFalse(text.contains("user"), text);
    assertTrue(
        text.contains("conductorDomain=wss://***@conductor.example.com/conductor/v1alpha1"), text);
    var plain = DBOSConfig.defaults("app").withConductorDomain("ws://localhost:8090").toString();
    assertTrue(plain.contains("conductorDomain=ws://localhost:8090,"), plain);
  }

  @Test
  public void theKeysAreMaskedInToString() {
    var text =
        DBOSConfig.defaults("app")
            .withConductorKey("conductor-key")
            .withTransactEnterpriseKey("enterprise-key")
            .toString();
    assertFalse(text.contains("conductor-key"), text);
    assertFalse(text.contains("enterprise-key"), text);
    assertTrue(text.contains("conductorKey=***"), text);
    assertTrue(text.contains("transactEnterpriseKey=***"), text);
    var unset = DBOSConfig.defaults("app").toString();
    assertTrue(unset.contains("conductorKey=null"), unset);
    assertTrue(unset.contains("transactEnterpriseKey=null"), unset);
  }
}
