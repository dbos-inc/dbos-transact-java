package dev.dbos.transact.database.signal;

import dev.dbos.transact.database.signal.SignalKey.WakeReason;

import java.util.concurrent.CompletableFuture;

public class Subscription extends CompletableFuture<WakeReason> implements AutoCloseable {
  private final Runnable onClose;

  public Subscription(Runnable onClose) {
    this.onClose = onClose;
  }

  @Override
  public void close() {
    onClose.run();
  }
}
