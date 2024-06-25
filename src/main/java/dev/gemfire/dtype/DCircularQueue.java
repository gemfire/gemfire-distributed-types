/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype;

import java.io.Serializable;
import java.util.Queue;

import org.apache.geode.DataSerializable;

/**
 * An implementation of a {@link Queue} that provides a first-in first-out queue with a
 * fixed size that replaces its oldest element if full. Most operations are forwarded to the
 * backend cluster where the actual data is maintained. Objects added to a queue need to be
 * serializable either as Java {@link Serializable} or one of GemFire's serializable types such as
 * {@link DataSerializable}.
 * <p>
 * Note that iteration methods will serialize the whole structure to the client and perform the
 * iteration locally. Iterators do not support {@code remove()} and will throw an
 * UnsupportedOperationException.
 * <p>
 *
 * @implNote
 *           The backing queue does not allow null elements to be added.
 *
 * @param <E> the type of elements in this queue
 */
public interface DCircularQueue<E> extends Queue<E>, DType {
}
