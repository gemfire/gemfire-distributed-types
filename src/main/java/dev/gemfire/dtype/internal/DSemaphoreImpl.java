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

  private static final DTypeContextualFunction AVAILABLE_PERMITS_FN =
      (sem, ctx) -> ((DSemaphoreBackend) sem).availablePermits();
  private static final DTypeContextualFunction GET_QUEUE_LENGTH_FN =
      (sem, ctx) -> ((DSemaphoreBackend) sem).getQueueLength();
  private static final DTypeContextualFunction DRAIN_PERMITS_FN =
      (sem, ctx) -> ((DSemaphoreBackend) sem).drainPermits(ctx);
  private static final DTypeContextualFunction DESTROY_FN = (sem, ctx) -> {
    ((DSemaphoreBackend) sem).destroy(ctx);
    return null;
  };

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
    return query(AVAILABLE_PERMITS_FN, SemaphoreBackendFunction.ID);
  }

  @Override
  public int getQueueLength() {
    return query(GET_QUEUE_LENGTH_FN, SemaphoreBackendFunction.ID);
  }

  @Override
  public int drainPermits() {
    return update(DRAIN_PERMITS_FN, SemaphoreBackendFunction.ID);
  }

  @Override
  public void destroy() {
    query(DESTROY_FN, SemaphoreBackendFunction.ID);
    super.destroy();
  }

  private void validatePermits(int permits) {
    if (permits < 0) {
      throw new IllegalArgumentException("permits must be a positive integer");
    }
  }
}
