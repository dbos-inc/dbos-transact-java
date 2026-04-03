package dev.dbos.transact.spring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import dev.dbos.transact.DBOS;

import org.junit.jupiter.api.Test;
import org.springframework.context.SmartLifecycle;

class DBOSLifecycleTest {

  @Test
  void notRunningBeforeStart() {
    var lifecycle = new DBOSAutoConfiguration.DBOSLifecycle(mock(DBOS.class));
    assertFalse(lifecycle.isRunning());
  }

  @Test
  void startCallsLaunchAndSetsRunning() {
    var mockDbos = mock(DBOS.class);
    var lifecycle = new DBOSAutoConfiguration.DBOSLifecycle(mockDbos);

    lifecycle.start();

    verify(mockDbos).launch();
    assertTrue(lifecycle.isRunning());
  }

  @Test
  void stopCallsShutdownAndClearsRunning() {
    var mockDbos = mock(DBOS.class);
    var lifecycle = new DBOSAutoConfiguration.DBOSLifecycle(mockDbos);
    lifecycle.start();

    lifecycle.stop();

    verify(mockDbos).shutdown();
    assertFalse(lifecycle.isRunning());
  }

  @Test
  void phaseIsDefaultPhase() {
    var lifecycle = new DBOSAutoConfiguration.DBOSLifecycle(mock(DBOS.class));
    assertEquals(SmartLifecycle.DEFAULT_PHASE, lifecycle.getPhase());
  }
}
