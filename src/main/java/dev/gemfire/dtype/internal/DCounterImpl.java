package dev.gemfire.dtype.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import dev.gemfire.dtype.DCounter;

import org.apache.geode.DataSerializer;
import org.apache.geode.InvalidDeltaException;

/**
 * This implementation of DCounter sends a delta value to make CRDT-like updates to the counter.
 * Unlike other types in the package, this approach does not require co-ordinating (locking)
 * updates in a central location (primary bucket).
 * <p>
 * Heavily based on <a href="https://github.com/charliemblack/gemfire-delta-counter">charliemblack/gemfire-delta-counter</a>
 */
public class DCounterImpl extends AbstractDType implements DCounter {

  private final AtomicLong counter = new AtomicLong(0);
  private final AtomicLong accumulator = new AtomicLong(0);

  public DCounterImpl() {}

  public DCounterImpl(String name, int value) {
    super(name);
    counter.set(value);
  }

  @Override
  public long get() {
    DCounterImpl entry = getEntry();
    long value = entry.counter.get();
    counter.set(value);
    return value;
  }

  @Override
  public synchronized long increment(long delta) {
    accumulator.addAndGet(delta);
    long result = counter.addAndGet(delta);
    updateEntry();

    return result;
  }

  @Override
  public void toData(DataOutput dataOutput) throws IOException {
    super.toData(dataOutput);
    DataSerializer.writePrimitiveLong(counter.get(), dataOutput);
  }

  @Override
  public void fromData(DataInput dataInput) throws IOException, ClassNotFoundException {
    super.fromData(dataInput);
    counter.set(dataInput.readLong());
  }

  @Override
  public boolean hasDelta() {
    return accumulator.get() != 0;
  }

  @Override
  public void toDelta(DataOutput dataOutput) throws IOException {
    long value;
    value = accumulator.getAndSet(0);
    DataSerializer.writePrimitiveLong(value, dataOutput);
  }

  @Override
  public void fromDelta(DataInput dataInput) throws IOException, InvalidDeltaException {
    counter.addAndGet(DataSerializer.readPrimitiveLong(dataInput));
  }

  @Override
  public String toString() {
    return "DeltaCounter{" +
        "value=" + counter +
        ", accumulator=" + accumulator +
        '}';
  }

}
