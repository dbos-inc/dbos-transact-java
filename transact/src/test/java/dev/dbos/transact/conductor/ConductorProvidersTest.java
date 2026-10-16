package dev.dbos.transact.conductor;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;

import java.util.List;

import org.junit.jupiter.api.Test;

public class ConductorProvidersTest {

  @Test
  public void loadsTheBundledProvider() {
    assertInstanceOf(BundledConductorProvider.class, ConductorProviders.load());
  }

  @Test
  public void selectsTheOnlyProvider() {
    var provider = new BundledConductorProvider();
    assertSame(provider, ConductorProviders.select(List.of(provider)));
  }

  @Test
  public void noProviderSaysWhatToAdd() {
    var e = assertThrows(IllegalStateException.class, () -> ConductorProviders.select(List.of()));
    assertTrue(
        e.getMessage().contains("dev.dbos:transact-enterprise:" + DBOS.version()), e.getMessage());
  }

  @Test
  public void moreThanOneProviderIsRejected() {
    var providers =
        List.<ConductorProvider>of(new BundledConductorProvider(), new BundledConductorProvider());
    assertThrows(IllegalStateException.class, () -> ConductorProviders.select(providers));
  }
}
