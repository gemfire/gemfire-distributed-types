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

  public DCircularQueueImpl() {}

  public DCircularQueueImpl(String name, int capacity) {
    super(name);
    queue = new CircularFifoQueue(capacity);
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
    DTypeCollectionsFunction fn = x -> ((DCircularQueueImpl<E>) x).queue.remove();
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public E poll() {
    DTypeCollectionsFunction fn = x -> ((DCircularQueueImpl<E>) x).queue.poll();
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public E element() {
    DTypeCollectionsFunction fn = x -> ((DCircularQueueImpl<E>) x).queue.element();
    return query(fn, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public E peek() {
    DTypeCollectionsFunction fn = x -> ((DCircularQueueImpl<E>) x).queue.peek();
    return query(fn, CollectionsBackendFunction.ID);
  }

  /**
   * Returns the number of elements stored in the queue.
   *
   * @return this queue's size
   */
  @Override
  public int size() {
    DTypeCollectionsFunction fn = x -> ((DCircularQueueImpl<?>) x).queue.size();
    return query(fn, CollectionsBackendFunction.ID);
  }

  /**
   * Returns true if this queue is empty; false otherwise.
   *
   * @return true if this queue is empty
   */
  @Override
  public boolean isEmpty() {
    DTypeCollectionsFunction fn = x -> ((DCircularQueueImpl<?>) x).queue.isEmpty();
    return query(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public boolean contains(Object o) {
    byte[] arg = serialize(o);
    DTypeCollectionsFunction fn = x -> ((DCircularQueueImpl<?>) x).queue.contains(deserialize(arg));
    return query(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public Iterator<E> iterator() {
    return null;
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
  @SuppressWarnings("unchecked")
  public boolean containsAll(Collection<?> c) {
    byte[] arg = serialize(c);
    DTypeCollectionsFunction fn =
        x -> ((DCircularQueueImpl<?>) x).queue.containsAll(deserialize(arg));
    return query(fn, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean addAll(Collection<? extends E> c) {
    byte[] arg = serialize(c);
    DTypeCollectionsFunction fn = x -> ((DCircularQueueImpl<?>) x).queue.addAll(deserialize(arg));
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean removeAll(Collection<?> c) {
    byte[] arg = serialize(c);
    DTypeCollectionsFunction fn =
        x -> ((DCircularQueueImpl<?>) x).queue.removeAll(deserialize(arg));
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean retainAll(Collection<?> c) {
    byte[] arg = serialize(c);
    DTypeCollectionsFunction fn =
        x -> ((DCircularQueueImpl<?>) x).queue.retainAll(deserialize(arg));
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public void clear() {
    DTypeCollectionsFunction fn = x -> {
      ((DCircularQueueImpl<?>) x).queue.clear();
      return null;
    };
    update(fn, CollectionsBackendFunction.ID);
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
