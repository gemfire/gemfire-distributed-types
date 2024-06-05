/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype.internal;

import java.util.Iterator;

class DelegatingQueueIterator<E> implements Iterator<E> {
  private Iterator<E> outer;

  DelegatingQueueIterator(Iterator<E> outer) {
    this.outer = outer;
  }

  @Override
  public boolean hasNext() {
    return outer.hasNext();
  }

  @Override
  public E next() {
    return outer.next();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}
