/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import dev.gemfire.dtype.DBlockingDeque;

import org.apache.geode.DataSerializer;

public class DBlockingDequeImpl<E> extends AbstractDType implements DBlockingDeque<E> {

  private LinkedBlockingDeque<E> deque;
  private int capacity;
  private transient ExecutorService executor;

  public DBlockingDequeImpl() {}

  public DBlockingDequeImpl(String name) {
    this(name, Integer.MAX_VALUE);
  }

  public DBlockingDequeImpl(String name, int capacity) {
    super(name);
    deque = new LinkedBlockingDeque<>(capacity);
    this.capacity = capacity;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void addFirst(E e) {
    byte[] arg = serialize(e);
    DTypeCollectionsFunction fn = x -> {
      ((DBlockingDequeImpl<E>) x).deque.addFirst(deserialize(arg));
      return null;
    };
    update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void addLast(E e) {
    byte[] arg = serialize(e);
    DTypeCollectionsFunction fn = x -> {
      ((DBlockingDequeImpl<E>) x).deque.addLast(deserialize(arg));
      return null;
    };
    update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean offerFirst(E e) {
    byte[] arg = serialize(e);
    DTypeCollectionsFunction fn =
        x -> ((DBlockingDequeImpl<E>) x).deque.offerFirst(deserialize(arg));
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean offerLast(E e) {
    byte[] arg = serialize(e);
    DTypeCollectionsFunction fn =
        x -> ((DBlockingDequeImpl<E>) x).deque.offerLast(deserialize(arg));
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public E removeFirst() {
    DTypeCollectionsFunction fn = x -> ((DBlockingDequeImpl<?>) x).deque.removeFirst();
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public E removeLast() {
    DTypeCollectionsFunction fn = x -> ((DBlockingDequeImpl<?>) x).deque.removeLast();
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public E pollFirst() {
    DTypeCollectionsFunction fn = x -> ((DBlockingDequeImpl<?>) x).deque.pollFirst();
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public E pollLast() {
    DTypeCollectionsFunction fn = x -> ((DBlockingDequeImpl<?>) x).deque.pollLast();
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public E getFirst() {
    DTypeCollectionsFunction fn = x -> ((DBlockingDequeImpl<?>) x).deque.getFirst();
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public E getLast() {
    DTypeCollectionsFunction fn = x -> ((DBlockingDequeImpl<?>) x).deque.getLast();
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public E peekFirst() {
    DTypeCollectionsFunction fn = x -> ((DBlockingDequeImpl<?>) x).deque.peekFirst();
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public E peekLast() {
    DTypeCollectionsFunction fn = x -> ((DBlockingDequeImpl<?>) x).deque.peekLast();
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void putFirst(E e) throws InterruptedException {
    DTypeCollectionsFunction fn = x -> {
      if (!((DBlockingDequeImpl<E>) x).deque.offerFirst(e)) {
        throw new RetryableException(100);
      }
      return null;
    };
    updateInterruptibly(fn, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void putLast(E e) throws InterruptedException {
    DTypeCollectionsFunction fn = x -> {
      if (!((DBlockingDequeImpl<E>) x).deque.offerLast(e)) {
        throw new RetryableException(100);
      }
      return null;
    };
    updateInterruptibly(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public boolean offerFirst(E e, long timeout, TimeUnit unit) throws InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean offerLast(E e, long timeout, TimeUnit unit) throws InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  @SuppressWarnings("unchecked")
  public E takeFirst() throws InterruptedException {
    DTypeCollectionsFunction fn = x -> {
      E result = ((DBlockingDequeImpl<E>) x).deque.pollFirst();
      if (result == null) {
        throw new RetryableException(100);
      }
      return result;
    };
    return updateInterruptibly(fn, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public E takeLast() throws InterruptedException {
    DTypeCollectionsFunction fn = x -> {
      E result = ((DBlockingDequeImpl<E>) x).deque.pollLast();
      if (result == null) {
        throw new RetryableException(100);
      }
      return result;
    };
    return updateInterruptibly(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public E pollFirst(long timeout, TimeUnit unit) throws InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public E pollLast(long timeout, TimeUnit unit) throws InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean removeFirstOccurrence(Object o) {
    byte[] arg = serialize(o);
    DTypeCollectionsFunction fn =
        x -> ((DBlockingDequeImpl<E>) x).deque.removeFirstOccurrence(deserialize(arg));
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean removeLastOccurrence(Object o) {
    byte[] arg = serialize(o);
    DTypeCollectionsFunction fn =
        x -> ((DBlockingDequeImpl<E>) x).deque.removeLastOccurrence(deserialize(arg));
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean add(E e) {
    byte[] arg = serialize(e);
    DTypeCollectionsFunction fn = x -> ((DBlockingDequeImpl<E>) x).deque.add(deserialize(arg));
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean offer(E e) {
    byte[] arg = serialize(e);
    DTypeCollectionsFunction fn = x -> ((DBlockingDequeImpl<E>) x).deque.offer(deserialize(arg));
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void put(E e) throws InterruptedException {
    byte[] arg = serialize(e);
    DTypeCollectionsFunction fn = x -> {
      if (!((DBlockingDequeImpl<E>) x).deque.offerLast(deserialize(arg))) {
        throw new RetryableException(100);
      }
      return null;
    };
    updateInterruptibly(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  @SuppressWarnings("unchecked")
  public E remove() {
    DTypeCollectionsFunction fn = x -> ((DBlockingDequeImpl<E>) x).deque.remove();
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public E poll() {
    return pollFirst();
  }

  @Override
  public E take() throws InterruptedException {
    return takeFirst();
  }

  @Override
  public E poll(long timeout, TimeUnit unit) throws InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int remainingCapacity() {
    DTypeCollectionsFunction fn = x -> ((DBlockingDequeImpl<?>) x).deque.remainingCapacity();
    return query(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public E element() {
    DTypeCollectionsFunction fn = x -> ((DBlockingDequeImpl<?>) x).deque.element();
    return query(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public E peek() {
    DTypeCollectionsFunction fn = x -> ((DBlockingDequeImpl<?>) x).deque.peek();
    return query(fn, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean remove(Object o) {
    byte[] arg = serialize(o);
    DTypeCollectionsFunction fn = x -> ((DBlockingDequeImpl<E>) x).deque.remove(deserialize(arg));
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    byte[] arg = serialize(c);
    DTypeCollectionsFunction fn =
        x -> ((DBlockingDequeImpl<?>) x).deque.containsAll(deserialize(arg));
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean addAll(Collection<? extends E> c) {
    byte[] arg = serialize(c);
    DTypeCollectionsFunction fn =
        x -> ((DBlockingDequeImpl<E>) x).deque.addAll(deserialize(arg));
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    byte[] arg = serialize(c);
    DTypeCollectionsFunction fn =
        x -> ((DBlockingDequeImpl<?>) x).deque.removeAll(deserialize(arg));
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    byte[] arg = serialize(c);
    DTypeCollectionsFunction fn =
        x -> ((DBlockingDequeImpl<?>) x).deque.retainAll(deserialize(arg));
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public void clear() {
    DTypeCollectionsFunction fn = x -> {
      ((DBlockingDequeImpl<?>) x).deque.clear();
      return null;
    };
    update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public boolean contains(Object o) {
    byte[] arg = serialize(o);
    DTypeCollectionsFunction fn =
        x -> ((DBlockingDequeImpl<?>) x).deque.contains(deserialize(arg));
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public int drainTo(Collection<? super E> c) {
    DTypeCollectionsFunction fn = x -> {
      Collection<? super E> result = new ArrayList<>();
      ((DBlockingDequeImpl<E>) x).deque.drainTo(result);
      return result;
    };
    Collection<E> r = update(fn, CollectionsBackendFunction.ID);
    c.addAll(r);
    return r.size();
  }

  @Override
  @SuppressWarnings("unchecked")
  public int drainTo(Collection<? super E> c, int maxElements) {
    DTypeCollectionsFunction fn = x -> {
      Collection<? super E> result = new ArrayList<>();
      ((DBlockingDequeImpl<E>) x).deque.drainTo(result, maxElements);
      return result;
    };
    Collection<E> r = update(fn, CollectionsBackendFunction.ID);
    c.addAll(r);
    return r.size();
  }

  @Override
  public int size() {
    DTypeCollectionsFunction fn = x -> ((DBlockingDequeImpl<?>) x).deque.size();
    return query(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public boolean isEmpty() {
    DTypeCollectionsFunction fn = x -> ((DBlockingDequeImpl<?>) x).deque.isEmpty();
    return query(fn, CollectionsBackendFunction.ID);
  }

  private class DelegatingQueueIterator implements Iterator<E> {
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

  /**
   * {@inheritDoc}
   * <p>
   * Note that iteration occurs on the client. As such, the entire structure will be serialized to
   * the client and then discarded once iteration is complete.
   * <p>
   * {@code remove} operations are NOT supported for iteration over this structure and will throw an
   * {@code UnsupportedOperationException}.
   */
  @Override
  public Iterator<E> iterator() {
    return null;
  }

  @Override
  public Object[] toArray() {
    DBlockingDequeImpl<E> entry = getEntry();
    return entry.deque.toArray();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    DBlockingDequeImpl<E> entry = getEntry();
    return entry.deque.toArray(a);
  }

  @Override
  public Iterator<E> descendingIterator() {
    return null;
  }

  @Override
  public void push(E e) {
    addFirst(e);
  }

  @Override
  public E pop() {
    return removeFirst();
  }

  @Override
  public synchronized void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writePrimitiveInt(capacity, out);
    DataSerializer.writePrimitiveInt(deque.size(), out);
    for (E element : deque) {
      DataSerializer.writeObject(element, out);
    }
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    capacity = DataSerializer.readPrimitiveInt(in);
    deque = new LinkedBlockingDeque<>(capacity);
    int size = DataSerializer.readPrimitiveInt(in);
    for (int i = 0; i < size; ++i) {
      E element = DataSerializer.readObject(in);
      deque.add(element);
    }
  }

  private <R> R updateInterruptibly(DTypeCollectionsFunction fn, String functionId)
      throws InterruptedException {
    Future<R> future = getExecutor().submit(() -> update(fn, functionId));
    try {
      return future.get();
    } catch (ExecutionException ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Return our executor, creating one lazily since not everyone will need one.
   *
   * @return an Excutor used to process any APIs that are interruptible
   */
  private synchronized ExecutorService getExecutor() {
    if (executor == null) {
      executor = Executors.newSingleThreadExecutor();
    }
    return executor;
  }
}
