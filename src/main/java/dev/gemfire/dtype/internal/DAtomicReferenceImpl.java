package dev.gemfire.dtype.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.function.BinaryOperator;
import java.util.function.UnaryOperator;

import dev.gemfire.dtype.DAtomicReference;

import org.apache.geode.DataSerializer;

public class DAtomicReferenceImpl<V> extends AbstractDType implements DAtomicReference<V> {

  private transient V value;

  public DAtomicReferenceImpl() {
    // For serialization
  }

  public DAtomicReferenceImpl(String name, V value) {
    super(name);
    this.value = value;
  }

  @Override
  @SuppressWarnings("unchecked")
  public V accumulateAndGet(V value, BinaryOperator<V> accumulatorFunction) {
    byte[] arg = serialize(value);
    DTypeCollectionsFunction fn = x -> {
      DAtomicReferenceImpl<V> atomicRef = (DAtomicReferenceImpl<V>) x;
      atomicRef.value = accumulatorFunction.apply(atomicRef.value, deserialize(arg));
      return atomicRef.value;
    };
    return update(fn, CollectionsBackendFunction.ID);
  }

  /**
   * Atomically sets the value to the given updated value if the current value {@code equals()}
   * the expected value. Note that these semantics are different to Java's AtomicReference
   * compareAndSet which compares <i>identity</i>.
   *
   * @param expect the expected value
   * @param update the new value
   * @return true if successful. False return indicates that the actual value was not equal to the
   *         expected value.
   */
  @Override
  @SuppressWarnings("unchecked")
  public boolean compareAndSet(V expect, V update) {
    byte[] argExpect  = serialize(expect);
    byte[] argUpdate  = serialize(update);
    DTypeCollectionsFunction fn = x -> {
      DAtomicReferenceImpl<V> atomicRef = (DAtomicReferenceImpl<V>) x;
      V realExpected = deserialize(argExpect);
      if (atomicRef.value.equals(realExpected)) {
        atomicRef.value = deserialize(argUpdate);
        return true;
      }
      return false;
    };
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public V get() {
    DAtomicReferenceImpl<V> entry = getEntry();
    return entry.value;
  }

  @Override
  @SuppressWarnings("unchecked")
  public V getAndAccumulate(V value, BinaryOperator<V> accumulatorFunction) {
    byte[] arg = serialize(value);
    DTypeCollectionsFunction fn = x -> {
      DAtomicReferenceImpl<V> atomicRef = (DAtomicReferenceImpl<V>) x;
      V previous = atomicRef.value;
      atomicRef.value = accumulatorFunction.apply(atomicRef.value, deserialize(arg));
      return previous;
    };
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public V getAndUpdate(UnaryOperator<V> updateFunction) {
    DTypeCollectionsFunction fn = x -> {
      DAtomicReferenceImpl<V> atomicRef = (DAtomicReferenceImpl<V>) x;
      V previous = atomicRef.value;
      atomicRef.value = updateFunction.apply(atomicRef.value);
      return previous;
    };
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public V getAndSet(V newValue) {
    byte[] arg = serialize(newValue);
    DTypeCollectionsFunction fn = x -> {
      DAtomicReferenceImpl<V> atomicRef = (DAtomicReferenceImpl<V>) x;
      V previous = atomicRef.value;
      atomicRef.value = deserialize(arg);
      return previous;
    };
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void set(V newValue) {
    byte[] arg = serialize(newValue);
    DTypeCollectionsFunction fn = x -> ((DAtomicReferenceImpl<V>) x).value = deserialize(arg);
    update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public V updateAndGet(UnaryOperator<V> updateFunction) {
    DTypeCollectionsFunction fn = x -> {
      DAtomicReferenceImpl<V> atomicRef = (DAtomicReferenceImpl<V>) x;
      atomicRef.value = updateFunction.apply(atomicRef.value);
      return atomicRef.value;
    };
    return update(fn, CollectionsBackendFunction.ID);
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeObject(value, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    value = DataSerializer.readObject(in);
  }

}
