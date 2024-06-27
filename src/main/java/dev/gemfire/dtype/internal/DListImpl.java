/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import dev.gemfire.dtype.DList;

import org.apache.geode.DataSerializer;

public class DListImpl<E> extends AbstractDType implements DList<E> {

  private LinkedList<E> list;

  // A few lambdas that can be static since they don't need to capture any values.
  private static final DTypeCollectionsFunction SIZE_FN = x -> ((DListImpl<?>) x).list.size();
  private static final DTypeCollectionsFunction IS_EMPTY_FN =
      x -> ((DListImpl<?>) x).list.isEmpty();
  private static final DTypeCollectionsFunction CLEAR_FN = x -> {
    ((DListImpl<?>) x).list.clear();
    return null;
  };

  public DListImpl() {}

  public DListImpl(String name) {
    super(name);
    list = new LinkedList<>();
  }

  @Override
  public int size() {
    return query(SIZE_FN, CollectionsBackendFunction.ID);
  }

  @Override
  public boolean isEmpty() {
    return query(IS_EMPTY_FN, CollectionsBackendFunction.ID);
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
    update(CLEAR_FN, CollectionsBackendFunction.ID);
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
  @SuppressWarnings("unchecked")
  public ListIterator<E> listIterator() {
    return ((DListImpl<E>) getEntry()).list.listIterator();
  }

  @Override
  @SuppressWarnings("unchecked")
  public ListIterator<E> listIterator(int index) {
    return ((DListImpl<E>) getEntry()).list.listIterator(index);
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<E> subList(int fromIndex, int toIndex) {
    DTypeCollectionsFunction fn =
        x -> new ArrayList<>(((DListImpl<E>) x).list.subList(fromIndex, toIndex));
    return query(fn, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void replaceAll(UnaryOperator<E> operator) {
    DTypeCollectionsFunction fn = x -> {
      ((DListImpl<E>) x).list.replaceAll(operator);
      return null;
    };
    update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void sort(Comparator<? super E> comparator) {
    DTypeCollectionsFunction fn = x -> {
      ((DListImpl<E>) x).list.sort(comparator);
      return null;
    };
    update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Spliterator<E> spliterator() {
    return ((DListImpl<E>) getEntry()).list.spliterator();
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean removeIf(Predicate<? super E> filter) {
    DTypeCollectionsFunction fn = x -> ((DListImpl<E>) x).list.removeIf(filter);
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void forEach(Consumer<? super E> action) {
    ((DListImpl<E>) getEntry()).list.forEach(action);
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
