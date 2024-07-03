package dev.gemfire.dtype;

/**
 * A distributed type, similar to a DAtomicLong, but with higher throughput and less potential
 * contention than DAtomicLong.
 */
public interface DCounter extends DType {

  /**
   * Get the current value.
   *
   * @return the current value
   */
  long get();

  /**
   * Update the value. The returned value reflects the current, local value. Any updates, performed
   * by other clients, will not be reflected in the returned value. Use {@link #get()} to retrieve
   * the most up-to-date value.
   *
   * @param delta the amount to update by - can be either positive or negative
   * @return the current, local value
   */
  long increment(long delta);

}
