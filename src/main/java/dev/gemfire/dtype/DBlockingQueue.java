/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype;

import java.util.concurrent.BlockingDeque;

/**
 * An implementation of a {@link BlockingDeque} that is distributed and highly available.
 *
 * @param <E> the type of elements in this queue
 */
public interface DBlockingQueue<E> extends BlockingDeque<E> {

  /**
   * Removes all the elements of this collection that satisfy the given predicate.
   *
   * @param filter a predicate which returns {@code true} for elements to be
   *        removed. The predicate must be able to be serialized.
   * @return {@code true} if any elements were removed
   */
  boolean removeIf(SerializablePredicate<? super E> filter);

}
