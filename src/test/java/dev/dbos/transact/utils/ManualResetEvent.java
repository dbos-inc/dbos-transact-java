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
}
