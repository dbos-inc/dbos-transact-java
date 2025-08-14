package dev.dbos.transact.utils;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ManualResetEvent {
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();
    private volatile boolean signaled;

    public ManualResetEvent(boolean initialState) {
        this.signaled = initialState;
    }

    public void set() {
        lock.lock();
        try {
            signaled = true;
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public void reset() {
        lock.lock();
        try {
            signaled = false;
        } finally {
            lock.unlock();
        }
    }

    // Note, it's a little suspect to wrap a checked exception in an unchecked exception in the two waitOne overloads. 
    // However, this is test code so a thrown exception should just fail the test.

    public void waitOne() {
        lock.lock();
        try {
            while (!signaled) {
                try {
                    condition.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public boolean waitOne(long millis) {
        lock.lock();
        try {
            long nanos = millis * 1_000_000L;
            while (!signaled) {
                if (nanos <= 0L) {
                    return false;
                }
                try {
                    nanos = condition.awaitNanos(nanos);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
            return true;
        } finally {
            lock.unlock();
        }
    }
}
