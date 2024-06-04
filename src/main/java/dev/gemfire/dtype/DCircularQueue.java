/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype;

import java.util.Queue;

/**
 * An implementation of a {@link Queue} that provides a first-in first-out queue with a
 * fixed size that replaces its oldest element if full.
 * <p>
 *
 * @implSpec
 *           This queue does not allow null elements to be added.
 *
 * @param <E> the type of elements in this queue
 */
public interface DCircularQueue<E> extends Queue<E> {
}
