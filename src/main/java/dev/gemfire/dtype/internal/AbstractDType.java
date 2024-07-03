/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype.internal;

import static dev.gemfire.dtype.internal.OperationType.NO_DELTA_UPDATE;
import static dev.gemfire.dtype.internal.OperationType.QUERY;
import static dev.gemfire.dtype.internal.OperationType.UPDATE;
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;

import dev.gemfire.dtype.DType;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.Delta;
import org.apache.geode.InvalidDeltaException;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.serialization.ByteArrayDataInput;

public abstract class AbstractDType implements Delta, DataSerializable, DType {

  private String name;
  private transient Region<String, Object> region;
  private transient DTypeCollectionsFunction deltaOperation = null;
  private transient OperationPerformer operationPerformer;

  public AbstractDType() {}

  public AbstractDType(String name) {
    this.name = name;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public void destroy() {
    region.remove(name);
  }

  public void initialize(Region<String, Object> region, OperationPerformer operationPerformer) {
    this.region = region;
    this.operationPerformer = operationPerformer;
  }

  protected <T> T getEntry() {
    return uncheckedCast(region.get(name));
  }

  public void updateEntry() {
    region.put(name, this);
  }

  protected void setDelta(DTypeCollectionsFunction fn) {
    deltaOperation = fn;
  }

  protected <T> T query(DTypeFunction fn, String gemfireFunctionId) {
    return operationPerformer.performOperation(this, fn, QUERY, gemfireFunctionId);
  }

  protected <T> T update(DTypeFunction fn, String gemfireFunctionId) {
    return operationPerformer.performOperation(this, fn, UPDATE, gemfireFunctionId);
  }

  protected <T> T updateInterruptibly(DTypeCollectionsFunction fn, String functionId)
      throws InterruptedException {
    try {
      return update(fn, functionId);
    } catch (Exception ex) {
      // This needs to be set correctly by GemFire's fn execution code - i.e. when a blocked
      // fn call is actually interrupted.
      if (Thread.currentThread().isInterrupted()) {
        throw new InterruptedException();
      }
      if (ex instanceof UncheckedInterruptedException) {
        throw (InterruptedException) ex.getCause();
      }
      throw ex;
    }
  }

  protected <T> T noDeltaUpdate(DTypeFunction fn, String gemfireFunctionId) {
    return operationPerformer.performOperation(this, fn, NO_DELTA_UPDATE, gemfireFunctionId);
  }

  protected <T> T noDeltaUpdateInterruptibly(DTypeCollectionsFunction fn, String functionId)
      throws InterruptedException {
    try {
      return noDeltaUpdate(fn, functionId);
    } catch (Exception ex) {
      if (Thread.currentThread().isInterrupted()) {
        throw new InterruptedException();
      }
      if (ex instanceof UncheckedInterruptedException) {
        throw (InterruptedException) ex.getCause();
      }
      throw ex;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AbstractDType that = (AbstractDType) o;
    return Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(name);
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(name, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    name = DataSerializer.readString(in);
  }

  @Override
  public boolean hasDelta() {
    return deltaOperation != null;
  }

  @Override
  public void toDelta(DataOutput out) throws IOException {
    DataSerializer.writeObject(deltaOperation, out);
  }

  @Override
  public void fromDelta(DataInput in) throws IOException, InvalidDeltaException {
    try {
      DTypeCollectionsFunction fn = DataSerializer.readObject(in);
      fn.apply(this);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  protected static byte[] serialize(Object o) {
    HeapDataOutputStream heap = new HeapDataOutputStream(0);
    try {
      DataSerializer.writeObject(o, heap);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return heap.toByteArray();
  }

  protected static <R> R deserialize(byte[] data) {
    R result;
    try {
      result = DataSerializer.readObject(new ByteArrayDataInput(data));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

    return result;
  }
}
