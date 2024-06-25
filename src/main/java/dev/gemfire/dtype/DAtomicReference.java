package dev.gemfire.dtype;

import java.util.function.BinaryOperator;
import java.util.function.UnaryOperator;

/**
 * A distributed and highly-available implementation of {@code AtomicReference}s.
 */
public interface DAtomicReference<V> {

  /**
   * Atomically updates the current value with the results of applying the given function to the
   * current and given values, returning the updated value.
   */
  V accumulateAndGet(V x, BinaryOperator<V> accumulatorFunction);

  /**
   * Atomically sets the value to the given updated value if the current value == the expected
   * value.
   */
  boolean compareAndSet(V expect, V update);

  /**
   * Gets the current value.
   */
  V get();

  /**
   * Atomically updates the current value with the results of applying the given function to the
   * current and given values, returning the previous value.
   */
  V getAndAccumulate(V x, BinaryOperator<V> accumulatorFunction);

  /**
   * Atomically sets to the given value and returns the old value.
   */
  V getAndUpdate(UnaryOperator<V> updateFunction);

  /**
   * Atomically updates the current value with the results of applying the given function,
   * returning the previous value.
   */
  V getAndSet(V newValue);

  /**
   * Sets to the given value.
   */
  void set(V newValue);

  /**
   * Atomically updates the current value with the results of applying the given function,
   * returning the updated value.
   */
  V updateAndGet(UnaryOperator<V> updateFunction);
}