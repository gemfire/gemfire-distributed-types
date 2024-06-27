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
import java.util.Spliterator;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;

import dev.gemfire.dtype.DBlockingQueue;

import org.apache.geode.DataSerializer;

public class DBlockingQueueImpl<E> extends AbstractDType implements DBlockingQueue<E> {

  private transient LinkedBlockingDeque<E> deque;
  private int capacity;

  // A few lambdas that can be static since they don't need to capture any values.
  private static final DTypeCollectionsFunction REMOVE_FIRST_FN =
      x -> ((DBlockingQueueImpl<?>) x).deque.removeFirst();
  private static final DTypeCollectionsFunction REMOVE_LAST_FN =
      x -> ((DBlockingQueueImpl<?>) x).deque.removeLast();
  private static final DTypeCollectionsFunction POLL_FIRST_FN =
      x -> ((DBlockingQueueImpl<?>) x).deque.pollFirst();
  private static final DTypeCollectionsFunction POLL_LAST_FN =
      x -> ((DBlockingQueueImpl<?>) x).deque.pollLast();
  private static final DTypeCollectionsFunction GET_FIRST_FN =
      x -> ((DBlockingQueueImpl<?>) x).deque.getFirst();
  private static final DTypeCollectionsFunction GET_LAST_FN =
      x -> ((DBlockingQueueImpl<?>) x).deque.getLast();
  private static final DTypeCollectionsFunction PEEK_FIRST_FN =
      x -> ((DBlockingQueueImpl<?>) x).deque.peekFirst();
  private static final DTypeCollectionsFunction PEEK_LAST_FN =
      x -> ((DBlockingQueueImpl<?>) x).deque.peekLast();
  private final DTypeCollectionsFunction TAKE_FIRST_FN = x -> {
    E result = ((DBlockingQueueImpl<E>) x).deque.pollFirst();
    if (result == null) {
      throw new RetryableException(100);
    }
    return result;
  };
  private final DTypeCollectionsFunction TAKE_LAST_FN = x -> {
    E result = ((DBlockingQueueImpl<E>) x).deque.pollLast();
    if (result == null) {
      throw new RetryableException(100);
    }
    return result;
  };
  private static final DTypeCollectionsFunction REMOVE_FN =
      x -> ((DBlockingQueueImpl<?>) x).deque.remove();

  private static final DTypeCollectionsFunction REMAINING_CAPACITY_FN =
      x -> ((DBlockingQueueImpl<?>) x).deque.remainingCapacity();
  private static final DTypeCollectionsFunction ELEMENT_FN =
      x -> ((DBlockingQueueImpl<?>) x).deque.element();
  private static final DTypeCollectionsFunction PEEK_FN =
      x -> ((DBlockingQueueImpl<?>) x).deque.peek();
  private static final DTypeCollectionsFunction CLEAR_FN = x -> {
    ((DBlockingQueueImpl<?>) x).deque.clear();
    return null;
  };
  private static final DTypeCollectionsFunction SIZE_FN =
      x -> ((DBlockingQueueImpl<?>) x).deque.size();
  private static final DTypeCollectionsFunction IS_EMPTY_FN =
      x -> ((DBlockingQueueImpl<?>) x).deque.isEmpty();

  public DBlockingQueueImpl() {}

  public DBlockingQueueImpl(String name) {
    this(name, Integer.MAX_VALUE);
  }

  public DBlockingQueueImpl(String name, int capacity) {
    super(name);
    deque = new LinkedBlockingDeque<>(capacity);
    this.capacity = capacity;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void addFirst(E e) {
    byte[] arg = serialize(e);
    DTypeCollectionsFunction fn = x -> {
      ((DBlockingQueueImpl<E>) x).deque.addFirst(deserialize(arg));
      return null;
    };
    update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void addLast(E e) {
    byte[] arg = serialize(e);
    DTypeCollectionsFunction fn = x -> {
      ((DBlockingQueueImpl<E>) x).deque.addLast(deserialize(arg));
      return null;
    };
    update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean offerFirst(E e) {
    byte[] arg = serialize(e);
    DTypeCollectionsFunction fn =
        x -> ((DBlockingQueueImpl<E>) x).deque.offerFirst(deserialize(arg));
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean offerLast(E e) {
    byte[] arg = serialize(e);
    DTypeCollectionsFunction fn =
        x -> ((DBlockingQueueImpl<E>) x).deque.offerLast(deserialize(arg));
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public E removeFirst() {
    return update(REMOVE_FIRST_FN, CollectionsBackendFunction.ID);
  }

  @Override
  public E removeLast() {
    return update(REMOVE_LAST_FN, CollectionsBackendFunction.ID);
  }

  @Override
  public E pollFirst() {
    return update(POLL_FIRST_FN, CollectionsBackendFunction.ID);
  }

  @Override
  public E pollLast() {
    return update(POLL_LAST_FN, CollectionsBackendFunction.ID);
  }

  @Override
  public E getFirst() {
    return update(GET_FIRST_FN, CollectionsBackendFunction.ID);
  }

  @Override
  public E getLast() {
    return update(GET_LAST_FN, CollectionsBackendFunction.ID);
  }

  @Override
  public E peekFirst() {
    return update(PEEK_FIRST_FN, CollectionsBackendFunction.ID);
  }

  @Override
  public E peekLast() {
    return update(PEEK_LAST_FN, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void putFirst(E e) throws InterruptedException {
    DTypeCollectionsFunction fn = x -> {
      if (!((DBlockingQueueImpl<E>) x).deque.offerFirst(e)) {
        throw new RetryableException(100);
      }
      return null;
    };
    updateInterruptibly(fn, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void putLast(E e) throws InterruptedException {
    byte[] arg = serialize(e);
    DTypeCollectionsFunction fn = x -> {
      if (!((DBlockingQueueImpl<E>) x).deque.offerLast(deserialize(arg))) {
        throw new RetryableException(100);
      }
      return null;
    };
    updateInterruptibly(fn, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean offerFirst(E e, long timeout, TimeUnit unit) throws InterruptedException {
    byte[] arg = serialize(e);
    DTypeCollectionsFunction fn = x -> {
      if (((DBlockingQueueImpl<E>) x).deque.offerFirst(deserialize(arg))) {
        return true;
      }
      throw new RetryableException(100, timeout, unit, () -> false);
    };
    return updateInterruptibly(fn, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean offerLast(E e, long timeout, TimeUnit unit) throws InterruptedException {
    byte[] arg = serialize(e);
    DTypeCollectionsFunction fn = x -> {
      if (((DBlockingQueueImpl<E>) x).deque.offerLast(deserialize(arg))) {
        return true;
      }
      throw new RetryableException(100, timeout, unit, () -> false);
    };
    return updateInterruptibly(fn, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public E takeFirst() throws InterruptedException {
    return updateInterruptibly(TAKE_FIRST_FN, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public E takeLast() throws InterruptedException {
    return updateInterruptibly(TAKE_LAST_FN, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public E pollFirst(long timeout, TimeUnit unit) throws InterruptedException {
    DTypeCollectionsFunction fn = x -> {
      E result = ((DBlockingQueueImpl<E>) x).deque.pollFirst();
      if (result == null) {
        throw new RetryableException(100, timeout, unit);
      }
      return result;
    };
    return updateInterruptibly(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public E pollLast(long timeout, TimeUnit unit) throws InterruptedException {
    DTypeCollectionsFunction fn = x -> {
      E result = ((DBlockingQueueImpl<E>) x).deque.pollLast();
      if (result == null) {
        throw new RetryableException(100, timeout, unit);
      }
      return result;
    };
    return updateInterruptibly(fn, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean removeFirstOccurrence(Object o) {
    byte[] arg = serialize(o);
    DTypeCollectionsFunction fn =
        x -> ((DBlockingQueueImpl<E>) x).deque.removeFirstOccurrence(deserialize(arg));
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean removeLastOccurrence(Object o) {
    byte[] arg = serialize(o);
    DTypeCollectionsFunction fn =
        x -> ((DBlockingQueueImpl<E>) x).deque.removeLastOccurrence(deserialize(arg));
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean add(E e) {
    byte[] arg = serialize(e);
    DTypeCollectionsFunction fn = x -> ((DBlockingQueueImpl<E>) x).deque.add(deserialize(arg));
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean offer(E e) {
    byte[] arg = serialize(e);
    DTypeCollectionsFunction fn = x -> ((DBlockingQueueImpl<E>) x).deque.offer(deserialize(arg));
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void put(E e) throws InterruptedException {
    byte[] arg = serialize(e);
    DTypeCollectionsFunction fn = x -> {
      if (!((DBlockingQueueImpl<E>) x).deque.offerLast(deserialize(arg))) {
        throw new RetryableException(100);
      }
      return null;
    };
    updateInterruptibly(fn, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
    byte[] arg = serialize(e);
    DTypeCollectionsFunction fn = x -> {
      if (((DBlockingQueueImpl<E>) x).deque.offer(deserialize(arg))) {
        return true;
      }
      throw new RetryableException(100, timeout, unit, () -> false);
    };
    return updateInterruptibly(fn, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public E remove() {
    return update(REMOVE_FN, CollectionsBackendFunction.ID);
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
  @SuppressWarnings("unchecked")
  public E poll(long timeout, TimeUnit unit) throws InterruptedException {
    DTypeCollectionsFunction fn = x -> {
      E result = ((DBlockingQueueImpl<E>) x).deque.poll();
      if (result == null) {
        throw new RetryableException(100, timeout, unit);
      }
      return result;
    };
    return updateInterruptibly(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public int remainingCapacity() {
    return query(REMAINING_CAPACITY_FN, CollectionsBackendFunction.ID);
  }

  @Override
  public E element() {
    return query(ELEMENT_FN, CollectionsBackendFunction.ID);
  }

  @Override
  public E peek() {
    return query(PEEK_FN, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean remove(Object o) {
    byte[] arg = serialize(o);
    DTypeCollectionsFunction fn = x -> ((DBlockingQueueImpl<E>) x).deque.remove(deserialize(arg));
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    byte[] arg = serialize(c);
    DTypeCollectionsFunction fn =
        x -> ((DBlockingQueueImpl<?>) x).deque.containsAll(deserialize(arg));
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean addAll(Collection<? extends E> c) {
    byte[] arg = serialize(c);
    DTypeCollectionsFunction fn =
        x -> ((DBlockingQueueImpl<E>) x).deque.addAll(deserialize(arg));
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    byte[] arg = serialize(c);
    DTypeCollectionsFunction fn =
        x -> ((DBlockingQueueImpl<?>) x).deque.removeAll(deserialize(arg));
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    byte[] arg = serialize(c);
    DTypeCollectionsFunction fn =
        x -> ((DBlockingQueueImpl<?>) x).deque.retainAll(deserialize(arg));
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public void clear() {
    update(CLEAR_FN, CollectionsBackendFunction.ID);
  }

  @Override
  public boolean contains(Object o) {
    byte[] arg = serialize(o);
    DTypeCollectionsFunction fn =
        x -> ((DBlockingQueueImpl<?>) x).deque.contains(deserialize(arg));
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public int drainTo(Collection<? super E> c) {
    DTypeCollectionsFunction fn = x -> {
      Collection<? super E> result = new ArrayList<>();
      ((DBlockingQueueImpl<E>) x).deque.drainTo(result);
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
      ((DBlockingQueueImpl<E>) x).deque.drainTo(result, maxElements);
      return result;
    };
    Collection<E> r = update(fn, CollectionsBackendFunction.ID);
    c.addAll(r);
    return r.size();
  }

  @Override
  public int size() {
    return query(SIZE_FN, CollectionsBackendFunction.ID);
  }

  @Override
  public boolean isEmpty() {
    return query(IS_EMPTY_FN, CollectionsBackendFunction.ID);
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
  @SuppressWarnings("unchecked")
  public Iterator<E> iterator() {
    return new DelegatingQueueIterator<>(((DBlockingQueueImpl<E>) getEntry()).deque.iterator());
  }

  @Override
  public Object[] toArray() {
    DBlockingQueueImpl<E> entry = getEntry();
    return entry.deque.toArray();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    DBlockingQueueImpl<E> entry = getEntry();
    return entry.deque.toArray(a);
  }

  @Override
  public Iterator<E> descendingIterator() {
    return null;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void forEach(Consumer<? super E> action) {
    ((DBlockingQueueImpl<E>) getEntry()).deque.forEach(action);
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
  @SuppressWarnings("unchecked")
  public boolean removeIf(Predicate<? super E> filter) {
    DTypeCollectionsFunction fn = x -> ((DBlockingQueueImpl<E>) x).deque.removeIf(filter);
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Spliterator<E> spliterator() {
    return ((DBlockingQueueImpl<E>) getEntry()).deque.spliterator();
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

}
