/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype;

import java.io.Serializable;
import java.util.concurrent.BlockingDeque;

import org.apache.geode.DataSerializable;

/**
 * An implementation of a {@link BlockingDeque} that is distributed and highly available.
 * Most operations are forwarded to the backend cluster where the actual data is maintained.
 * Objects added to a queue need to be serializable either as Java {@link Serializable} or one
 * of GemFire's serializable types such as {@link DataSerializable}.
 * <p>
 * Note that iteration methods will serialize the whole structure to the client and perform the
 * iteration locally. Iterators do not support {@code remove()} and will throw an
 * UnsupportedOperationException.
 * <p>
 * Note that methods that are interruptible can only be interrupted locally. There is no interrupt
 * 'signal' that is passed to the server performing the actual operation.
 *
 * @param <E> the type of elements in this queue
 */
public interface DBlockingQueue<E> extends BlockingDeque<E>, DType {

}
