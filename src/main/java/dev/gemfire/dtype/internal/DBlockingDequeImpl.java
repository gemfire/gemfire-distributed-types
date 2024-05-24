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
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import dev.gemfire.dtype.DBlockingDeque;

import org.apache.geode.DataSerializer;

public class DBlockingDequeImpl<E> extends AbstractDType implements DBlockingDeque<E> {

  private LinkedBlockingDeque<E> deque;
  private int capacity;

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
    // DTypeCollectionsFunction fn = x -> {
    // ((DBlockingDequeImpl<E>) x).deque.putFirst(e);
    // return null;
    // };
    // update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public void putLast(E e) throws InterruptedException {

  }

  @Override
  public boolean offerFirst(E e, long timeout, TimeUnit unit) throws InterruptedException {
    return false;
  }

  @Override
  public boolean offerLast(E e, long timeout, TimeUnit unit) throws InterruptedException {
    return false;
  }

  @Override
  public E takeFirst() throws InterruptedException {
    return null;
  }

  @Override
  public E takeLast() throws InterruptedException {
    return null;
  }

  @Override
  public E pollFirst(long timeout, TimeUnit unit) throws InterruptedException {
    return null;
  }

  @Override
  public E pollLast(long timeout, TimeUnit unit) throws InterruptedException {
    return null;
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
    // DTypeCollectionsFunction fn = x -> {
    // ((DBlockingDequeImpl<E>) x).deque.put(e);
    // return null;
    // };
    // update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
    return false;
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
    DTypeCollectionsFunction fn = x -> ((DBlockingDequeImpl<E>) x).deque.poll();
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public E take() throws InterruptedException {
    return null;
  }

  @Override
  public E poll(long timeout, TimeUnit unit) throws InterruptedException {
    return null;
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
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public boolean isEmpty() {
    return false;
  }

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
}
