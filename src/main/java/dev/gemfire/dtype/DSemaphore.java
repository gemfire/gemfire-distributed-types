/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype;

import java.util.concurrent.Semaphore;

/**
 * DSemaphore is a highly-available, distributed version of the JDK's {@link Semaphore}. As such,
 * the subset of methods attempts to provide the same semantics as the regular semaphore's does.
 * DSemaphores are intended to be used from GemFire clients.
 * <p>
 * Each DSemaphore is initialized with a number of permits that clients can {@link #acquire()} or
 * {@link #release()}. If no permit is available the calling thread is blocked until one becomes
 * available. Since DSemaphores are distributed, a given semaphore can be accessed across different
 * JVM instances. As with regular semaphores, DSemaphores simply maintain a count of the permits
 * available and no actual 'permit' objects are used. It is up to the calling application to ensure
 * correct usage of the semaphore by ensuring that the acquired permits are also correctly released.
 * <p>
 * DSemaphores are backed by objects stored in the GemFire cluster. In the event of a server
 * shutdown or crash, the state of a given semaphore is maintained. Clients that are blocked,
 * attempting to acquire a permit, will be disconnected and will receive an exception. Permits that
 * are held by a client that crashes or closes its cache without releasing the permits, are
 * automatically released.
 * <p>
 * Example use:
 *
 * <pre>
 * ClientCache client = new ClientCacheFactory()
 *     .addPoolLocator("localhost", locatorPort)
 *     .create();
 *
 * factory = new DTypeFactory(client);
 * DSemaphore semaphore = factory.createSemaphore("semi", 1);
 *
 * semaphore.initialize(1);
 * semaphore.acquire();
 * </pre>
 *
 * Note that a DSemaphore will only be initialized once. If a semaphore has already been created,
 * subsequent calls to {@code createSemaphore}, even from different JVMs, will not adjust the
 * number of permits.
 */
public interface DSemaphore {

  /**
   * Acquire a single permit. The caller will block until a permit becomes available. The order of
   * acquisition, for block callers is non-deterministic. Unlike regular semaphores, there is no
   * optional strategy for determining which waiting caller will acquire a permit.
   */
  void acquire();

  /**
   * Acquire a number of permits. The caller will block until the total number of permits are
   * available. This does not mean that the caller will accumulate and 'hold' permits until the
   * required number is reached.
   *
   * @param permits the number of permits to acquire
   */
  void acquire(int permits);

  /**
   * Release a single permit. Callers waiting to acquire a permit will be notified and
   */
  void release();

  /**
   * Release a number of permits. Note that a given caller can release permits without necessarily
   * having acquired those permits.
   *
   * @param permits the number of permits to acquire
   */
  void release(int permits);

  /**
   * Non-blocking version of {@code acquire} which does not block. Will return immediately with a
   * boolean indicating success or failure.
   *
   * @return true if the permit was acquired, false otherwise
   */
  boolean tryAcquire();

  /**
   * Non-blocking version of {@code acquire(int)} which does not block. Will return immediately with
   * a
   * boolean indicating success or failure.
   *
   * @param permits the number of permits to acquire
   * @return true if the permits were acquired, false otherwise
   */
  boolean tryAcquire(int permits);

  /**
   * Return the number of permits available
   *
   * @return the number of permits available
   */
  int availablePermits();

  /**
   * Return the number of callers waiting to acquire permits
   *
   * @return the number of callers waiting to acquire permits
   */
  int getQueueLength();

  /**
   * Acquire all remaining permits if any are available.
   *
   * @return the number of permits acquired
   */
  int drainPermits();

  /**
   * Destroy the semaphore. The method will release any backend structures. Callers waiting to
   * acquire permits will receive a {@link DTypeException} indicating that the semaphore has been
   * destroyed.
   */
  void destroy();
}
