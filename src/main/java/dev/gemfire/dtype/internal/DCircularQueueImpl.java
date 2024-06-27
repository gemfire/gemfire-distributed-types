/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import dev.gemfire.dtype.DCircularQueue;
import org.apache.commons.collections4.queue.CircularFifoQueue;

import org.apache.geode.DataSerializer;

public class DCircularQueueImpl<E> extends AbstractDType implements DCircularQueue<E> {

  private transient CircularFifoQueue<E> queue;
  private int capacity;

  // A few lambdas that can be static since they don't need to capture any values.
  private static final DTypeCollectionsFunction REMOVE_FN =
      x -> ((DCircularQueueImpl<?>) x).queue.remove();
  private static final DTypeCollectionsFunction POLL_FN =
      x -> ((DCircularQueueImpl<?>) x).queue.poll();
  private static final DTypeCollectionsFunction ELEMENT_FN =
      x -> ((DCircularQueueImpl<?>) x).queue.element();
  private static final DTypeCollectionsFunction PEEK_FN =
      x -> ((DCircularQueueImpl<?>) x).queue.peek();
  private static final DTypeCollectionsFunction SIZE_FN =
      x -> ((DCircularQueueImpl<?>) x).queue.size();
  private static final DTypeCollectionsFunction IS_EMPTY_FN =
      x -> ((DCircularQueueImpl<?>) x).queue.isEmpty();
  private static final DTypeCollectionsFunction CLEAR_FN = x -> {
    ((DCircularQueueImpl<?>) x).queue.clear();
    return null;
  };

  public DCircularQueueImpl() {}

  public DCircularQueueImpl(String name, int capacity) {
    super(name);
    queue = new CircularFifoQueue<>(capacity);
    this.capacity = capacity;
  }

  /**
   * Adds the given element to this queue. If the queue is full, the least recently added
   * element is discarded so that a new element can be inserted.
   *
   * @param e the element to add
   * @return true, always
   * @throws NullPointerException if the given element is null
   */
  @Override
  @SuppressWarnings("unchecked")
  public boolean add(E e) {
    byte[] arg = serialize(e);
    DTypeCollectionsFunction fn = x -> ((DCircularQueueImpl<E>) x).queue.add(deserialize(arg));
    return update(fn, CollectionsBackendFunction.ID);
  }

  /**
   * Adds the given element to this queue. If the queue is full, the least recently added
   * element is discarded so that a new element can be inserted.
   *
   * @param e the element to add
   * @return true, always
   * @throws NullPointerException if the given element is null
   */
  @Override
  @SuppressWarnings("unchecked")
  public boolean offer(E e) {
    byte[] arg = serialize(e);
    DTypeCollectionsFunction fn = x -> ((DCircularQueueImpl<E>) x).queue.offer(deserialize(arg));
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public E remove() {
    return update(REMOVE_FN, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public E poll() {
    return update(POLL_FN, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public E element() {
    return query(ELEMENT_FN, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public E peek() {
    return query(PEEK_FN, CollectionsBackendFunction.ID);
  }

  /**
   * Returns the number of elements stored in the queue.
   *
   * @return this queue's size
   */
  @Override
  public int size() {
    return query(SIZE_FN, CollectionsBackendFunction.ID);
  }

  /**
   * Returns true if this queue is empty; false otherwise.
   *
   * @return true if this queue is empty
   */
  @Override
  public boolean isEmpty() {
    return query(IS_EMPTY_FN, CollectionsBackendFunction.ID);
  }

  @Override
  public boolean contains(Object o) {
    byte[] arg = serialize(o);
    DTypeCollectionsFunction fn = x -> ((DCircularQueueImpl<?>) x).queue.contains(deserialize(arg));
    return query(fn, CollectionsBackendFunction.ID);
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
    return new DelegatingQueueIterator<>(((DCircularQueueImpl<E>) getEntry()).queue.iterator());
  }

  @Override
  public Object[] toArray() {
    DCircularQueueImpl<E> entry = getEntry();
    return entry.queue.toArray();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    return null;
  }

  @Override
  public boolean remove(Object o) {
    byte[] arg = serialize(o);
    DTypeCollectionsFunction fn = x -> ((DCircularQueueImpl<?>) x).queue.remove(deserialize(arg));
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    byte[] arg = serialize(c);
    DTypeCollectionsFunction fn =
        x -> ((DCircularQueueImpl<?>) x).queue.containsAll(deserialize(arg));
    return query(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public boolean addAll(Collection<? extends E> c) {
    byte[] arg = serialize(c);
    DTypeCollectionsFunction fn = x -> ((DCircularQueueImpl<?>) x).queue.addAll(deserialize(arg));
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    byte[] arg = serialize(c);
    DTypeCollectionsFunction fn =
        x -> ((DCircularQueueImpl<?>) x).queue.removeAll(deserialize(arg));
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    byte[] arg = serialize(c);
    DTypeCollectionsFunction fn =
        x -> ((DCircularQueueImpl<?>) x).queue.retainAll(deserialize(arg));
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public void clear() {
    update(CLEAR_FN, CollectionsBackendFunction.ID);
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writePrimitiveInt(capacity, out);
    DataSerializer.writePrimitiveInt(queue.size(), out);
    for (E element : queue) {
      DataSerializer.writeObject(element, out);
    }
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    capacity = DataSerializer.readPrimitiveInt(in);
    queue = new CircularFifoQueue<>(capacity);
    int size = DataSerializer.readPrimitiveInt(in);
    for (int i = 0; i < size; ++i) {
      E element = DataSerializer.readObject(in);
      queue.add(element);
    }
  }
}
