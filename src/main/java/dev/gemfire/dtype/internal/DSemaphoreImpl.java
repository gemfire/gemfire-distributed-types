/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype.internal;

import dev.gemfire.dtype.DSemaphore;

/**
 * Concrete implementation of DSemaphore that forwards all calls to the backing GemFire cluster.
 * Instances of this class hold no state on the client but are associated with a corresponding
 * instance of {@link DSemaphoreBackend} that maintains the state in the cluster.
 */
public class DSemaphoreImpl extends AbstractDType implements DSemaphore {

  public DSemaphoreImpl(String name) {
    super(name);
  }

  public boolean setPermits(int permits) {
    validatePermits(permits);
    DTypeContextualFunction fn = (sem, ctx) -> ((DSemaphoreBackend) sem).setPermits(permits);
    return update(fn, SemaphoreBackendFunction.ID);
  }

  @Override
  public void acquire() {
    acquire(1);
  }

  @Override
  public void acquire(int permits) {
    validatePermits(permits);
    DTypeContextualFunction fn = (sem, lock) -> {
      ((DSemaphoreBackend) sem).acquire(lock, permits);
      return null;
    };
    update(fn, SemaphoreBackendFunction.ID);
  }

  @Override
  public void release() {
    release(1);
  }

  @Override
  public void release(int permits) {
    validatePermits(permits);
    DTypeContextualFunction fn = (sem, ctx) -> {
      ((DSemaphoreBackend) sem).release(ctx, permits);
      return null;
    };
    update(fn, SemaphoreBackendFunction.ID);
  }

  @Override
  public boolean tryAcquire() {
    return tryAcquire(1);
  }

  @Override
  public boolean tryAcquire(int permits) {
    validatePermits(permits);
    DTypeContextualFunction fn =
        (sem, ctx) -> ((DSemaphoreBackend) sem).tryAcquire(ctx, permits);
    return update(fn, SemaphoreBackendFunction.ID);
  }

  @Override
  public int availablePermits() {
    DTypeContextualFunction fn = (sem, ctx) -> ((DSemaphoreBackend) sem).availablePermits();
    return query(fn, SemaphoreBackendFunction.ID);
  }

  @Override
  public int getQueueLength() {
    DTypeContextualFunction fn = (sem, ctx) -> ((DSemaphoreBackend) sem).getQueueLength();
    return query(fn, SemaphoreBackendFunction.ID);
  }

  @Override
  public int drainPermits() {
    DTypeContextualFunction fn = (sem, ctx) -> ((DSemaphoreBackend) sem).drainPermits(ctx);
    return update(fn, SemaphoreBackendFunction.ID);
  }

  @Override
  public void destroy() {
    DTypeContextualFunction fn = (sem, ctx) -> {
      ((DSemaphoreBackend) sem).destroy(ctx);
      return null;
    };
    query(fn, SemaphoreBackendFunction.ID);
    super.destroy();
  }

  private void validatePermits(int permits) {
    if (permits < 0) {
      throw new IllegalArgumentException("permits must be a positive integer");
    }
  }
}
