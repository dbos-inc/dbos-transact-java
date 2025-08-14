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

    public void waitOne() {
        lock.lock();
        try {
            while (!signaled) {
                try {
                    condition.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
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
                    return false;
                }
            }
            return true;
        } finally {
            lock.unlock();
        }
    }
}
