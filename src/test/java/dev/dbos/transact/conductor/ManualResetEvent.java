package dev.dbos.transact.conductor;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ManualResetEvent {
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();
    private volatile boolean signaled;

    public ManualResetEvent(boolean initialState) {
        this.signaled = initialState;
    }

    // Wait until signaled
    public void waitOne() throws InterruptedException {
        lock.lock();
        try {
            while (!signaled) {
                condition.await();
            }
        } finally {
            lock.unlock();
        }
    }

    // Wait until signaled or timeout
    public boolean waitOne(long millis) throws InterruptedException {
        lock.lock();
        try {
            long nanos = millis * 1_000_000L;
            while (!signaled) {
                if (nanos <= 0L) {
                    return false;
                }
                nanos = condition.awaitNanos(nanos);
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    // Set the event (unblocks all waiters)
    public void set() {
        lock.lock();
        try {
            signaled = true;
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    // Reset the event (future waits will block)
    public void reset() {
        lock.lock();
        try {
            signaled = false;
        } finally {
            lock.unlock();
        }
    }
}
