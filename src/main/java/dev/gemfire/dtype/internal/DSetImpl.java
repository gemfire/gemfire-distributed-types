/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

import dev.gemfire.dtype.DSet;

import org.apache.geode.DataSerializer;

public class DSetImpl<E> extends AbstractDType implements DSet<E> {

  private HashSet<E> set;

  public DSetImpl() {}

  public DSetImpl(String name) {
    super(name);
    set = new HashSet<>();
  }

  @Override
  public int size() {
    DTypeCollectionsFunction fn = x -> ((DSetImpl<?>) x).set.size();
    return query(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public boolean isEmpty() {
    DTypeCollectionsFunction fn = x -> ((DSetImpl<?>) x).set.isEmpty();
    return query(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public boolean contains(Object o) {
    byte[] arg = serialize(o);
    DTypeCollectionsFunction fn = x -> ((DSetImpl<?>) x).set.contains(deserialize(arg));
    return query(fn, CollectionsBackendFunction.ID);
  }

  private class DelegatingSetIterator implements Iterator<E> {
    private Iterator<E> outer;
    private E lastEntry;

    DelegatingSetIterator(Iterator<E> outer) {
      this.outer = outer;
    }

    @Override
    public boolean hasNext() {
      return outer.hasNext();
    }

    @Override
    public E next() {
      lastEntry = outer.next();
      return lastEntry;
    }

    @Override
    public void remove() {
      DSetImpl.this.remove(lastEntry);
    }
  }

  @Override
  public Iterator<E> iterator() {
    DSetImpl<E> entry = getEntry();
    return new DelegatingSetIterator(entry.set.iterator());
  }

  @Override
  public Object[] toArray() {
    DSetImpl<E> entry = getEntry();
    return entry.set.toArray();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    DSetImpl<E> entry = getEntry();
    return entry.set.toArray(a);
  }

  @Override
  public boolean add(E e) {
    byte[] arg = serialize(e);
    DTypeCollectionsFunction fn = x -> ((DSetImpl<?>) x).set.add(deserialize(arg));
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public boolean remove(Object o) {
    byte[] arg = serialize(o);
    DTypeCollectionsFunction fn = x -> ((DSetImpl<?>) x).set.remove(deserialize(arg));
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    byte[] arg = serialize(c);
    DTypeCollectionsFunction fn = x -> ((DSetImpl<?>) x).set.containsAll(deserialize(arg));
    return query(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public boolean addAll(Collection<? extends E> c) {
    byte[] arg = serialize(c);
    DTypeCollectionsFunction fn = x -> ((DSetImpl<?>) x).set.addAll(deserialize(arg));
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    byte[] arg = serialize(c);
    DTypeCollectionsFunction fn = x -> ((DSetImpl<?>) x).set.retainAll(deserialize(arg));
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    byte[] arg = serialize(c);
    DTypeCollectionsFunction fn = x -> ((DSetImpl<?>) x).set.removeAll(deserialize(arg));
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public void clear() {
    DTypeCollectionsFunction fn = x -> {
      ((DSetImpl<?>) x).set.clear();
      return null;
    };
    update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public synchronized void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writePrimitiveInt(set.size(), out);
    for (E element : set) {
      DataSerializer.writeObject(element, out);
    }
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    set = new HashSet<>();
    int size = DataSerializer.readPrimitiveInt(in);
    for (int i = 0; i < size; ++i) {
      E element = DataSerializer.readObject(in);
      set.add(element);
    }
  }

}
