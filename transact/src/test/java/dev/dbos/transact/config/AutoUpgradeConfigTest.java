package dev.dbos.transact.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Test;

/** The auto-upgrade settings are checked where the configuration is built. */
public class AutoUpgradeConfigTest {

  @Test
  public void autoUpgradeIsOffByDefault() {
    var config = DBOSConfig.defaults("app");

    assertFalse(config.autoUpgrade());
    assertTrue(config.withAutoUpgrade(true).autoUpgrade());
    assertFalse(config.withAutoUpgrade(true).withAutoUpgrade(false).autoUpgrade());
    // The defaults are left to the loop and the pass.
    assertNull(config.autoUpgradeInterval());
    assertNull(config.autoUpgradeGrace());
    assertEquals(Duration.ofSeconds(30), DBOSConfig.DEFAULT_AUTO_UPGRADE_INTERVAL);
    assertEquals(Duration.ZERO, DBOSConfig.DEFAULT_AUTO_UPGRADE_GRACE);
  }

  @Test
  public void anIntervalMustBePositive() {
    var interval = Duration.ofMillis(500);
    assertEquals(
        interval,
        DBOSConfig.defaults("app").withAutoUpgradeInterval(interval).autoUpgradeInterval());
    assertNull(DBOSConfig.defaults("app").withAutoUpgradeInterval(null).autoUpgradeInterval());

    for (var bad : List.of(Duration.ZERO, Duration.ofSeconds(-1), Duration.ofNanos(-1))) {
      var e =
          assertThrows(
              IllegalArgumentException.class,
              () -> DBOSConfig.defaults("app").withAutoUpgradeInterval(bad),
              bad.toString());
      assertTrue(e.getMessage().contains("autoUpgradeInterval"), e.getMessage());
      assertTrue(e.getMessage().contains("must be positive"), e.getMessage());
      assertTrue(e.getMessage().contains(bad.toString()), e.getMessage());
    }
  }

  @Test
  public void aGraceMustNotBeNegative() {
    // Zero is valid and means no grace.
    for (var grace : List.of(Duration.ofSeconds(60), Duration.ZERO)) {
      assertEquals(
          grace, DBOSConfig.defaults("app").withAutoUpgradeGrace(grace).autoUpgradeGrace());
    }
    assertNull(DBOSConfig.defaults("app").withAutoUpgradeGrace(null).autoUpgradeGrace());

    for (var bad : List.of(Duration.ofSeconds(-1), Duration.ofNanos(-1))) {
      var e =
          assertThrows(
              IllegalArgumentException.class,
              () -> DBOSConfig.defaults("app").withAutoUpgradeGrace(bad),
              bad.toString());
      assertTrue(e.getMessage().contains("autoUpgradeGrace"), e.getMessage());
      assertTrue(e.getMessage().contains("must not be negative"), e.getMessage());
    }
  }

  @Test
  public void theSettingsSurviveTheOtherWithersAndTheCopy() {
    var config =
        DBOSConfig.defaults("app")
            .withAutoUpgrade(true)
            .withAutoUpgradeInterval(Duration.ofSeconds(5))
            .withAutoUpgradeGrace(Duration.ofMinutes(2));

    for (var copy :
        List.of(
            config.withAppName("renamed"),
            config.withSuspendMode(true),
            config.withTransactEnterpriseKey("key"),
            new DBOSConfig(config))) {
      assertTrue(copy.autoUpgrade());
      assertEquals(Duration.ofSeconds(5), copy.autoUpgradeInterval());
      assertEquals(Duration.ofMinutes(2), copy.autoUpgradeGrace());
    }
    var printed = config.toString();
    assertTrue(printed.contains("autoUpgrade=true"), printed);
    assertTrue(printed.contains("autoUpgradeInterval=PT5S"), printed);
    assertTrue(printed.contains("autoUpgradeGrace=PT2M"), printed);
  }
}
