/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import dev.gemfire.dtype.DTypeException;
import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * This class is the backing class that corresponds to {@link DSemaphoreImpl}. All state is held
 * here.
 */
public class DSemaphoreBackend extends AbstractDType {

  private static final Logger logger = LogService.getLogger();

  private int permitsAvailable;

  // Map of client member names and corresponding permits held for this semaphore
  private Map<String, Integer> permitHolders = new HashMap<>();
  private boolean isInitialized = false;
  private int queueLength = 0;
  // This is set when serialization has transferred state to another member and requires tracking
  // to be re-established.
  private boolean requiresRecovery;
  private boolean isDestroyed = false;

  public DSemaphoreBackend() {
    // For serialization
    requiresRecovery = true;
  }

  public DSemaphoreBackend(String name) {
    super(name);
    requiresRecovery = false;
  }

  /**
   * Set the initial number of permits for this instance. Will only be effective on the first call.
   *
   * @param permits the number of permits to set
   * @return true if the permits were set, false if this instance has already been initialized with
   *         a number of permits
   */
  public synchronized boolean setPermits(int permits) {
    if (isInitialized) {
      return false;
    }
    permitsAvailable = permits;
    isInitialized = true;

    return true;
  }

  public int getQueueLength() {
    return queueLength;
  }

  public void acquire(DTypeFunctionContext context, int permits) {
    while (!_acquire(context, permits)) {
      try {
        queueLength++;
        if (logger.isDebugEnabled()) {
          logger.debug("Waiting to acquire semaphore '{}' for member {}", getName(),
              ((DSemaphoreFunctionContext) context).getMemberTag());
        }
        this.wait();
      } catch (InterruptedException e) {
        throw new UncheckedInterruptedException(e);
      } finally {
        queueLength--;
      }
    }
  }

  private synchronized boolean _acquire(DTypeFunctionContext context, int permits) {
    ensureUsable();
    DSemaphoreFunctionContext semContext = (DSemaphoreFunctionContext) context;
    if (permitsAvailable >= permits) {
      permitsAvailable -= permits;
      permitHolders.compute(semContext.getMemberTag(), (k, v) -> v == null ? 1 : v + permits);
      semContext.getTracker().add(semContext.getMemberTag(), this);
      if (logger.isDebugEnabled()) {
        logger.debug("Acquired semaphore '{}' for member {}", getName(),
            semContext.getMemberTag());
      }
      return true;
    }
    return false;
  }

  public boolean tryAcquire(DTypeFunctionContext DSemaphoreFunctionContext, int permits) {
    return _acquire(DSemaphoreFunctionContext, permits);
  }

  public synchronized void release(DTypeFunctionContext context, int permits) {
    ensureUsable();
    DSemaphoreFunctionContext semContext = (DSemaphoreFunctionContext) context;

    if (permitHolders.get(semContext.getMemberTag()) == permits) {
      permitHolders.remove(semContext.getMemberTag());
      semContext.getTracker().remove(semContext.getMemberTag(), this);
    } else {
      permitHolders.compute(semContext.getMemberTag(), (k, v) -> v - permits);
    }
    permitsAvailable += permits;

    notifyAll();
  }

  public int availablePermits() {
    return permitsAvailable;
  }

  public synchronized int drainPermits(DTypeFunctionContext context) {
    ensureUsable();
    DSemaphoreFunctionContext semContext = (DSemaphoreFunctionContext) context;
    if (permitsAvailable <= 0) {
      return 0;
    }

    int permitsReturned = permitsAvailable;
    permitsAvailable = 0;
    permitHolders.compute(semContext.getMemberTag(), (k, v) -> v == null ? 1 : v + permitsReturned);
    semContext.getTracker().add(semContext.getMemberTag(), this);

    return permitsReturned;
  }

  public synchronized void destroy(DTypeFunctionContext context) {
    ensureUsable();
    DSemaphoreFunctionContext semContext = (DSemaphoreFunctionContext) context;
    semContext.getTracker().remove(semContext.getMemberTag(), this);

    isDestroyed = true;

    notifyAll();
  }

  synchronized void releaseAll(String memberTag) {
    Integer permits = permitHolders.remove(memberTag);
    if (permits != null) {
      permitsAvailable += permits;
      notifyAll();
    }
  }

  synchronized void recoverTrackingIfNeeded(DSemaphoreTracker tracker) {
    if (!requiresRecovery) {
      return;
    }

    for (String clientMemberId : permitHolders.keySet()) {
      tracker.add(clientMemberId, this);
    }
    requiresRecovery = false;
  }

  private void ensureUsable() {
    if (!isInitialized) {
      throw new DTypeException("semaphore is not initialized");
    }

    if (isDestroyed) {
      throw new DTypeException("semaphore is destroyed");
    }
  }

  @Override
  public synchronized void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writePrimitiveBoolean(isInitialized, out);
    DataSerializer.writePrimitiveInt(permitsAvailable, out);
    DataSerializer.writePrimitiveInt(permitHolders.size(), out);
    for (Map.Entry<String, Integer> entry : permitHolders.entrySet()) {
      DataSerializer.writeString(entry.getKey(), out);
      DataSerializer.writeInteger(entry.getValue(), out);
    }
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    isInitialized = DataSerializer.readPrimitiveBoolean(in);
    permitsAvailable = DataSerializer.readPrimitiveInt(in);
    int size = DataSerializer.readPrimitiveInt(in);
    permitHolders = new HashMap<>(size);
    for (int i = 0; i < size; i++) {
      String member = DataSerializer.readString(in);
      Integer permitsHeld = DataSerializer.readInteger(in);
      permitHolders.put(member, permitsHeld);
    }
  }

}
