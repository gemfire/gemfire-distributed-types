/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import dev.gemfire.dtype.DAtomicLong;

import org.apache.geode.DataSerializer;

public class DAtomicLongImpl extends AbstractDType implements DAtomicLong {

  private AtomicLong value;

  public DAtomicLongImpl() {
    // For serialization
  }

  public DAtomicLongImpl(String name) {
    super(name);
    value = new AtomicLong(0);
  }

  @Override
  public long get() {
    DAtomicLongImpl entry = getEntry();
    return entry.value.get();
  }

  @Override
  public void set(long value) {
    DTypeCollectionsFunction fn = x -> {
      ((DAtomicLongImpl) x).value.set(value);
      return null;
    };
    update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public long getAndAdd(long delta) {
    DTypeCollectionsFunction fn = x -> ((DAtomicLongImpl) x).value.getAndAdd(delta);
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public boolean compareAndSet(long expect, long update) {
    DTypeCollectionsFunction fn = x -> ((DAtomicLongImpl) x).value.compareAndSet(expect, update);
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writePrimitiveLong(value.get(), out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    value = new AtomicLong(DataSerializer.readPrimitiveLong(in));
  }

}
