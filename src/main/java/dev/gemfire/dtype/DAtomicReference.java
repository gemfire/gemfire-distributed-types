package dev.gemfire.dtype;

import java.util.function.BinaryOperator;
import java.util.function.UnaryOperator;

/**
 * A distributed and highly-available implementation of {@code AtomicReference}s.
 */
public interface DAtomicReference<V> extends DType {

  /**
   * Atomically updates the current value with the results of applying the given function to the
   * current and given values, returning the updated value.
   *
   * @param x the update value
   * @param accumulatorFunction a side-effect-free function of two arguments
   * @return the updated value
   */
  V accumulateAndGet(V x, BinaryOperator<V> accumulatorFunction);

  /**
   * Atomically sets the value to the given updated value if the current value == the expected
   * value.
   *
   * @param expect the expected value
   * @param update the new value
   * @return true if successful
   */
  boolean compareAndSet(V expect, V update);

  /**
   * Gets the current value.
   *
   * @return the current value
   */
  V get();

  /**
   * Atomically updates the current value with the results of applying the given function to the
   * current and given values, returning the previous value.
   *
   * @param x the update value
   * @param accumulatorFunction a side-effect-free function of two arguments
   * @return the previous value
   */
  V getAndAccumulate(V x, BinaryOperator<V> accumulatorFunction);

  /**
   * Atomically sets to the given value and returns the old value.
   *
   * @param updateFunction a side-effect-free function
   * @return the previous value
   */
  V getAndUpdate(UnaryOperator<V> updateFunction);

  /**
   * Atomically updates the current value with the results of applying the given function,
   * returning the previous value.
   *
   * @param newValue the value to set
   * @return the previous value
   */
  V getAndSet(V newValue);

  /**
   * Sets to the given value.
   *
   * @param newValue the value to set
   */
  void set(V newValue);

  /**
   * Atomically updates the current value with the results of applying the given function,
   * returning the updated value.
   *
   * @param updateFunction a side-effect-free function
   * @return the updated value
   */
  V updateAndGet(UnaryOperator<V> updateFunction);
}
