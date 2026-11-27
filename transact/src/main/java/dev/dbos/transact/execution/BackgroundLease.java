package dev.dbos.transact.execution;

// Whether this process holds the background-jobs lock, until its loop fails to retake it: kept
// past a stalled hold's idle timeout, as two holders only duplicate work.
final class BackgroundLease {

  private volatile boolean held;

  void renew() {
    held = true;
  }

  void lose() {
    held = false;
  }

  boolean held() {
    return held;
  }
}
