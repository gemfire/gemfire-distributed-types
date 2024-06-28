package dev.gemfire.dtype.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import dev.gemfire.dtype.DCountDownLatch;
import dev.gemfire.dtype.DTypeException;

public class DCountDownLatchImpl extends AbstractDType implements DCountDownLatch {

  private long count;
  private boolean isDestroyed = false;
  private int waiters = 0;

  private final DTypeCollectionsFunction AWAIT_FN = x -> {
    DCountDownLatchImpl latch = (DCountDownLatchImpl) x;
    latch.ensureUsable();
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

  private final DTypeCollectionsFunction COUNTDOWN_FN = x -> {
    DCountDownLatchImpl latch = (DCountDownLatchImpl) x;
    latch.ensureUsable();
    if (latch.count > 0) {
      latch.count -= 1;
      if (latch.count == 0) {
        latch.notifyAll();
      }
    }
    return null;
  };
  private static final DTypeCollectionsFunction GET_COUNT_FN = x -> ((DCountDownLatchImpl) x).count;
  private static final DTypeCollectionsFunction DESTROY_FN = x -> {
    ((DCountDownLatchImpl) x).isDestroyed = true;
    x.notifyAll();
    return null;
  };

  public DCountDownLatchImpl() {
    // For serialization
  }

  public DCountDownLatchImpl(String name, int count) {
    super(name);
    this.count = count;
  }

  @Override
  public void await() {
    noDeltaUpdate(AWAIT_FN, CollectionsBackendFunction.ID);
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
        latch.ensureUsable();
        try {
          latch.waiters++;
          long waitStart = System.currentTimeMillis();
          latch.wait(waitTimeoutMs);
          overallTimeoutMs -= waitStart;
          if (latch.count == 0) {
            return true;
          }
        } catch (InterruptedException e) {
          throw new UncheckInterruptedException(e);
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
    noDeltaUpdate(COUNTDOWN_FN, CollectionsBackendFunction.ID);
  }

  @Override
  public long getCount() {
    return query(GET_COUNT_FN, CollectionsBackendFunction.ID);
  }

  @Override
  public boolean setCount(long newCount) {
    DTypeCollectionsFunction fn = x -> {
      DCountDownLatchImpl latch = (DCountDownLatchImpl) x;
      latch.ensureUsable();
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
    noDeltaUpdate(DESTROY_FN, CollectionsBackendFunction.ID);
    super.destroy();
  }

  DTypeCollectionsFunction GET_WAITERS_FN = x -> ((DCountDownLatchImpl) x).waiters;

  public int getWaiters() {
    return query(GET_WAITERS_FN, CollectionsBackendFunction.ID);
  }

  @Override
  public String toString() {
    return String.format("DCountDownLatchImpl{name=%s, count=%d}", getName(), count);
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
