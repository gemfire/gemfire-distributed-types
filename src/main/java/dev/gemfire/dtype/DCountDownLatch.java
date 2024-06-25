package dev.gemfire.dtype;

import java.util.concurrent.TimeUnit;

public interface DCountDownLatch {
  /**
   * Causes the current thread to wait until the latch has counted down to zero, unless the thread
   * is interrupted.
   */
  void await();

  /**
   * Causes the current thread to wait until the latch has counted down to zero, unless the thread
   * is interrupted, or the specified waiting time elapses.
   *
   * @param timeout the maximum time to wait
   * @param unit the time unit of the timeout argument
   * @return true if the count reached zero and false if the waiting time elapsed before the count
   *         reached zero
   * @throws InterruptedException if the current thread is interrupted while waiting
   */
  boolean await(long timeout, TimeUnit unit) throws InterruptedException;

  /**
   * Decrements the count of the latch, releasing all waiting threads if the count reaches zero.
   */
  void countDown();

  /**
   * Returns the current count.
   *
   * @return the current count
   */
  long getCount();

  /**
   * Set the count to a new value only if count is already zero.
   *
   * @param count the new value to set
   * @return true if count was set. False if the existing count was not zero.
   */
  boolean setCount(long count);

  /**
   * Destroys the countdown latch. This will release any threads that may have been waiting on the
   * latch.
   */
  void destroy();

  /**
   * Return the number of threads waiting on this countdown latch.
   *
   * @return the number of threads waiting
   */
  int getWaiters();

  /**
   * Returns a string identifying this latch, as well as its state.
   *
   * @return atring representation of the latch
   */
  String toString();
}
