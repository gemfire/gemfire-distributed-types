/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

import dev.gemfire.dtype.DList;

import org.apache.geode.DataSerializer;

public class DListImpl<E> extends AbstractDType implements DList<E> {

  private LinkedList<E> list;

  public DListImpl() {}

  public DListImpl(String name) {
    super(name);
    list = new LinkedList<>();
  }

  @Override
  public int size() {
    DTypeCollectionsFunction fn = x -> ((DListImpl<?>) x).list.size();
    return query(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public boolean isEmpty() {
    DTypeCollectionsFunction fn = x -> ((DListImpl<?>) x).list.isEmpty();
    return query(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public boolean contains(Object o) {
    byte[] arg = serialize(o);
    DTypeCollectionsFunction fn = x -> ((DListImpl<?>) x).list.contains(deserialize(arg));
    return query(fn, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean add(E e) {
    byte[] arg = serialize(e);
    DTypeCollectionsFunction fn = x -> ((DListImpl<E>) x).list.add(deserialize(arg));
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void add(int index, E e) {
    byte[] arg = serialize(e);
    DTypeCollectionsFunction fn = x -> {
      ((DListImpl<E>) x).list.add(index, deserialize(arg));
      return null;
    };
    update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public boolean remove(Object o) {
    byte[] arg = serialize(o);
    DTypeCollectionsFunction fn = x -> ((DListImpl<?>) x).list.remove(deserialize(arg));
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public E get(int index) {
    DTypeCollectionsFunction fn = x -> ((DListImpl<?>) x).list.get(index);
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public E set(int index, E e) {
    byte[] arg = serialize(e);
    DTypeCollectionsFunction fn = x -> ((DListImpl<E>) x).list.set(index, deserialize(arg));
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public E remove(int index) {
    DTypeCollectionsFunction fn = x -> ((DListImpl<?>) x).list.remove(index);
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public void clear() {
    DTypeCollectionsFunction fn = x -> {
      ((DListImpl<?>) x).list.clear();
      return null;
    };
    update(fn, CollectionsBackendFunction.ID);
  }

  private class DelegatingListIterator implements Iterator<E> {
    private Iterator<E> outer;
    private int index = 0;

    DelegatingListIterator(Iterator<E> outer) {
      this.outer = outer;
    }

    @Override
    public boolean hasNext() {
      return outer.hasNext();
    }

    @Override
    public E next() {
      index++;
      return outer.next();
    }

    @Override
    public void remove() {
      index--;
      DListImpl.this.remove(index);
    }
  }

  /**
   * {@inheritDoc}
   * <p>
   * Note that iteration occurs on the client. As such, the entire structure will be serialized to
   * the client and then discarded once iteration is complete. Any {@code remove} calls will be
   * submitted to the structure maintained on the server.
   */
  @Override
  public Iterator<E> iterator() {
    DListImpl<E> entry = getEntry();
    return new DelegatingListIterator(entry.list.iterator());
  }

  @Override
  public Object[] toArray() {
    DListImpl<E> entry = getEntry();
    return entry.list.toArray();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    DListImpl<E> entry = getEntry();
    return entry.list.toArray(a);
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    byte[] arg = serialize(c);
    DTypeCollectionsFunction fn = x -> ((DListImpl<?>) x).list.containsAll(deserialize(arg));
    return query(fn, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean addAll(Collection<? extends E> c) {
    byte[] arg = serialize(c);
    DTypeCollectionsFunction fn = x -> ((DListImpl<E>) x).list.addAll(deserialize(arg));
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean addAll(int index, Collection<? extends E> c) {
    byte[] arg = serialize(c);
    DTypeCollectionsFunction fn = x -> ((DListImpl<E>) x).list.addAll(index, deserialize(arg));
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean removeAll(Collection<?> c) {
    byte[] arg = serialize(c);
    DTypeCollectionsFunction fn = x -> ((DListImpl<E>) x).list.removeAll(deserialize(arg));
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean retainAll(Collection<?> c) {
    byte[] arg = serialize(c);
    DTypeCollectionsFunction fn = x -> ((DListImpl<E>) x).list.retainAll(deserialize(arg));
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public int indexOf(Object o) {
    byte[] arg = serialize(o);
    DTypeCollectionsFunction fn = x -> ((DListImpl<?>) x).list.indexOf(deserialize(arg));
    return query(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public int lastIndexOf(Object o) {
    byte[] arg = serialize(o);
    DTypeCollectionsFunction fn = x -> ((DListImpl<?>) x).list.lastIndexOf(deserialize(arg));
    return query(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public ListIterator<E> listIterator() {
    return null;
  }

  @Override
  public ListIterator<E> listIterator(int index) {
    return null;
  }

  @Override
  public List<E> subList(int fromIndex, int toIndex) {
    return Collections.emptyList();
  }

  @Override
  public synchronized void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writePrimitiveInt(list.size(), out);
    for (E element : list) {
      DataSerializer.writeObject(element, out);
    }
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    list = new LinkedList<>();
    int size = DataSerializer.readPrimitiveInt(in);
    for (int i = 0; i < size; ++i) {
      E element = DataSerializer.readObject(in);
      list.addLast(element);
    }
  }

}
