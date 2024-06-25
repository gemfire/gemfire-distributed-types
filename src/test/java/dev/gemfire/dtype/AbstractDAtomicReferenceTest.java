package dev.gemfire.dtype;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.function.BinaryOperator;
import java.util.function.UnaryOperator;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public abstract class AbstractDAtomicReferenceTest {

  @Rule
  public TestName testName = new TestName();

  abstract DTypeFactory getFactory();

  @Test
  public void testAccumulateAndGet() {
    DAtomicReference<Long> ref = getFactory().createDAtomicReference(testName.getMethodName());
    ref.set(0L);

    assertThat(ref.accumulateAndGet(1L, (Serializable & BinaryOperator<Long>) Long::sum))
        .isEqualTo(1L);
    assertThat(ref.get()).isEqualTo(1L);
  }

  @Test
  public void testCompareAndSet() {
    Movie avatar = new Movie("Avatar");
    Movie aliens = new Movie("Aliens");

    DAtomicReference<Movie> ref = getFactory().createDAtomicReference(testName.getMethodName());
    ref.set(aliens);

    assertThat(ref.compareAndSet(aliens, avatar)).isTrue();

    assertThat(ref.get()).isEqualTo(avatar);
  }

  @Test
  public void testGetAndAccumulate() {
    DAtomicReference<Long> ref = getFactory().createDAtomicReference(testName.getMethodName());
    ref.set(0L);

    assertThat(ref.getAndAccumulate(1L, (Serializable & BinaryOperator<Long>) Long::sum))
        .isEqualTo(0L);
    assertThat(ref.get()).isEqualTo(1L);
  }

  @Test
  public void testGetAndUpdate() {
    DAtomicReference<Long> ref = getFactory().createDAtomicReference(testName.getMethodName());
    ref.set(3L);

    assertThat(ref.getAndUpdate((Serializable & UnaryOperator<Long>) x -> x * 7))
        .isEqualTo(3L);
    assertThat(ref.get()).isEqualTo(21L);
  }

  @Test
  public void testGetSet() {
    Movie avatar = new Movie("Avatar");
    Movie aliens = new Movie("Aliens");

    DAtomicReference<Movie> ref = getFactory().createDAtomicReference(testName.getMethodName(), avatar);
    assertThat(ref.getAndSet(aliens)).isEqualTo(avatar);

    assertThat(ref.get()).isEqualTo(aliens);
  }

  @Test
  public void testSet() {
    Movie avatar = new Movie("Avatar");
    Movie aliens = new Movie("Aliens");

    DAtomicReference<Movie> ref = getFactory().createDAtomicReference(testName.getMethodName(), avatar);
    ref.set(aliens);

    DAtomicReference<Movie> ref2 = getFactory().createDAtomicReference(testName.getMethodName());
    assertThat(ref2.get()).isEqualTo(aliens);
  }

  @Test
  public void testUpdateAndGet() {
    DAtomicReference<Long> ref = getFactory().createDAtomicReference(testName.getMethodName());
    ref.set(3L);

    assertThat(ref.updateAndGet((Serializable & UnaryOperator<Long>) x -> x * 7))
        .isEqualTo(21L);
    assertThat(ref.get()).isEqualTo(21L);
  }

}
