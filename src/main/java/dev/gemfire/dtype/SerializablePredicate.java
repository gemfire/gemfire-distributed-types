/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype;

import java.io.Serializable;
import java.util.function.Predicate;

/**
 * An interface for {@link Serializable} {@link Predicate}s. This is used by the {@code removeIf}
 * method found in various collection types.
 *
 * @param <T> the type of element to be acted on
 */
@FunctionalInterface
public interface SerializablePredicate<T> extends Predicate<T>, Serializable {
}
