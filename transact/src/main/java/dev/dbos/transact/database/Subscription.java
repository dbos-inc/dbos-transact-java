package dev.dbos.transact.database;

import dev.dbos.transact.database.SignalKey.WakeReason;

import java.util.concurrent.CompletableFuture;

class Subscription extends CompletableFuture<WakeReason> implements AutoCloseable {
  private final Runnable onClose;

  Subscription(Runnable onClose) {
    this.onClose = onClose;
  }

  @Override
  public void close() {
    onClose.run();
  }
}
