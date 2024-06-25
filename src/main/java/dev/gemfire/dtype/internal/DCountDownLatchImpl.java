package dev.gemfire.dtype.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import dev.gemfire.dtype.DCountDownLatch;
import dev.gemfire.dtype.DTypeException;

public class DCountDownLatchImpl extends AbstractDType implements DCountDownLatch {

  private volatile long count;
  private boolean isDestroyed = false;
  private volatile int waiters = 0;

  public DCountDownLatchImpl() {
    // For serialization
  }

  public DCountDownLatchImpl(String name, int count) {
    super(name);
    this.count = count;
  }

  @Override
  public void await() {
    DTypeCollectionsFunction fn = x -> {
      DCountDownLatchImpl latch = (DCountDownLatchImpl) x;
      while (latch.count > 0) {
        try {
          latch.waiters++;
          latch.wait();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        } finally {
          latch.waiters--;
        }
      }
      return null;
    };
    noDeltaUpdate(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
    DTypeCollectionsFunction fn = x -> {
      DCountDownLatchImpl latch = (DCountDownLatchImpl) x;
      if (latch.count == 0) {
        return true;
      }

      long overallTimeoutMs = unit.toMillis(timeout);
      long waitTimeoutMs = overallTimeoutMs;
      long start = System.currentTimeMillis();

      while (latch.count > 0 && System.currentTimeMillis() - start < overallTimeoutMs) {
        ensureUsable();
        try {
          latch.waiters++;
          long waitStart = System.currentTimeMillis();
          latch.wait(waitTimeoutMs);
          overallTimeoutMs -= waitStart;
          if (latch.count == 0) {
            return true;
          }
        } catch (InterruptedException e) {
          throw new MarkerException(e);
        } finally {
          latch.waiters--;
        }
      }
      return false;
    };
    return noDeltaUpdateInterruptibly(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public void countDown() {
    DTypeCollectionsFunction fn = x -> {
      DCountDownLatchImpl latch = (DCountDownLatchImpl) x;
      ensureUsable();
      if (latch.count > 0) {
        latch.count -= 1;
        if (latch.count == 0) {
          latch.notifyAll();
        }
      }
      return null;
    };
    noDeltaUpdate(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public long getCount() {
    DTypeCollectionsFunction fn = x -> ((DCountDownLatchImpl) x).count;
    return query(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public boolean setCount(long newCount) {
    DTypeCollectionsFunction fn = x -> {
      ensureUsable();
      DCountDownLatchImpl latch = (DCountDownLatchImpl) x;
      if (latch.count > 0) {
        return false;
      }
      latch.count = newCount;
      return true;
    };
    return noDeltaUpdate(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public void destroy() {
    DTypeCollectionsFunction fn = x -> {
      ((DCountDownLatchImpl) x).isDestroyed = true;
      x.notifyAll();
      return null;
    };
    noDeltaUpdate(fn, CollectionsBackendFunction.ID);
    super.destroy();
  }

  public int getWaiters() {
    DTypeCollectionsFunction fn = x -> ((DCountDownLatchImpl) x).waiters;
    return query(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public String toString() {
    return super.toString();
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeLong(count);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    count = in.readLong();
  }

  private void ensureUsable() {
    if (isDestroyed) {
      throw new DTypeException("countdown latch is destroyed");
    }
  }

}
