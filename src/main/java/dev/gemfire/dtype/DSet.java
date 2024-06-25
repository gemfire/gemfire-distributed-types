/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype;

import java.io.Serializable;
import java.util.Set;

import org.apache.geode.DataSerializable;

/**
 * A DSet is a distributed and highly available {@link Set} that is backed by a GemFire cluster.
 * Most operations are forwarded to the backend cluster where the actual data is maintained.
 * Objects added to a set need to be serializable either as Java {@link Serializable} or one
 * of GemFire's serializable types such as {@link DataSerializable}.
 * <p>
 * Note that iteration methods will serialize the whole structure to the client and perform the
 * iteration locally.
 *
 * @param <E> the type of elements held in this set
 */
public interface DSet<E> extends Set<E>, DType {
}
