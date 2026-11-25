package dev.dbos.transact.spi;

import java.util.function.BooleanSupplier;

/** One auto-upgrade pass, which DBOS runs on a timer. */
public interface AutoUpgradePass {
  /** Runs a pass until done or {@code stopRequested}; returns the workflows moved. */
  int run(BooleanSupplier stopRequested);

  /** Called once, when DBOS stops running passes; {@link #run} may still be in flight. */
  void close();
}
