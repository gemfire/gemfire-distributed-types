/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public abstract class AbstractDCircularQueueTest {

  @ClassRule
  public static ExecutorServiceRule executor = new ExecutorServiceRule();

  @Rule
  public TestName testName = new TestName();

  abstract DTypeFactory getFactory();

  @Test
  public void testAdd() {
    DCircularQueue<String> queue = getFactory().createDCircularQueue(testName.getMethodName(), 3);

    queue.add("A");
    queue.add("B");
    queue.add("C");

    assertThat(queue.toArray()).containsExactly("A", "B", "C");
  }

  @Test
  public void testOffer() {
    DCircularQueue<String> queue = getFactory().createDCircularQueue(testName.getMethodName(), 3);

    queue.add("A");
    queue.add("B");
    assertThat(queue.offer("C")).isTrue();
    assertThat(queue.offer("D")).isTrue();

    assertThat(queue.toArray()).containsExactly("B", "C", "D");
  }

  @Test
  public void testRemove() {
    DCircularQueue<String> queue = getFactory().createDCircularQueue(testName.getMethodName(), 3);

    queue.add("A");
    queue.add("B");
    assertThat(queue.remove()).isEqualTo("A");
    assertThat(queue.remove()).isEqualTo("B");

    assertThatThrownBy(queue::remove).isInstanceOf(NoSuchElementException.class);
  }

  @Test
  public void testPoll() {
    DCircularQueue<String> queue = getFactory().createDCircularQueue(testName.getMethodName(), 3);

    queue.add("A");
    queue.add("B");
    assertThat(queue.poll()).isEqualTo("A");
    assertThat(queue.poll()).isEqualTo("B");
    assertThat(queue.poll()).isNull();
  }

  @Test
  public void testElement() {
    DCircularQueue<String> queue = getFactory().createDCircularQueue(testName.getMethodName(), 3);

    assertThatThrownBy(queue::element).isInstanceOf(NoSuchElementException.class);

    queue.add("A");
    queue.add("B");
    assertThat(queue.element()).isEqualTo("A");
    assertThat(queue.toArray()).containsExactly("A", "B");
  }

  @Test
  public void testPeek() {
    DCircularQueue<String> queue = getFactory().createDCircularQueue(testName.getMethodName(), 3);

    assertThat(queue.peek()).isNull();

    queue.add("A");
    queue.add("B");
    assertThat(queue.peek()).isEqualTo("A");
    assertThat(queue.toArray()).containsExactly("A", "B");
  }

  @Test
  public void testSize() {
    DCircularQueue<String> queue = getFactory().createDCircularQueue(testName.getMethodName(), 3);

    assertThat(queue.size()).isEqualTo(0);
    queue.add("A");
    assertThat(queue.size()).isEqualTo(1);
    queue.add("B");
    assertThat(queue.size()).isEqualTo(2);
    queue.remove();
    assertThat(queue.size()).isEqualTo(1);
    queue.remove();
    assertThat(queue.size()).isEqualTo(0);
  }

  @Test
  public void testEmpty() {
    DCircularQueue<String> queue = getFactory().createDCircularQueue(testName.getMethodName(), 3);

    assertThat(queue.isEmpty()).isTrue();
    queue.add("A");
    assertThat(queue.isEmpty()).isFalse();
    queue.remove();
    assertThat(queue.isEmpty()).isTrue();
  }

  @Test
  public void testContains() {
    DCircularQueue<String> queue = getFactory().createDCircularQueue(testName.getMethodName(), 3);

    assertThat(queue.contains("A")).isFalse();
    queue.add("A");
    assertThat(queue.contains("A")).isTrue();
    queue.remove();
    assertThat(queue.contains("A")).isFalse();
  }

  @Test
  public void testRemoveObject() {
    DCircularQueue<String> queue = getFactory().createDCircularQueue(testName.getMethodName(), 3);

    queue.add("A");
    queue.add("B");
    queue.add("C");

    assertThat(queue.remove("X")).isFalse();
    assertThat(queue.remove("B")).isTrue();
    assertThat(queue.toArray()).containsExactly("A", "C");

    assertThat(queue.remove("A")).isTrue();
    assertThat(queue.remove("C")).isTrue();
    assertThat(queue.isEmpty()).isTrue();
  }

  @Test
  public void testContainsAll() {
    DCircularQueue<String> queue = getFactory().createDCircularQueue(testName.getMethodName(), 3);

    queue.add("A");
    queue.add("B");
    queue.add("C");

    assertThat(queue.containsAll(Arrays.asList("C", "B", "A"))).isTrue();
    assertThat(queue.containsAll(Arrays.asList("C", "B", "A", "X"))).isFalse();
  }

  @Test
  public void testAddAll() {
    DCircularQueue<String> queue = getFactory().createDCircularQueue(testName.getMethodName(), 3);

    queue.addAll(Arrays.asList("A", "B", "C"));
    assertThat(queue.toArray()).containsExactly("A", "B", "C");

    queue.addAll(Arrays.asList("X", "Y"));
    assertThat(queue.toArray()).containsExactly("C", "X", "Y");
  }

  @Test
  public void testRemoveAll() {
    DCircularQueue<String> queue = getFactory().createDCircularQueue(testName.getMethodName(), 4);

    queue.add("A");
    queue.add("B");
    queue.add("C");
    queue.add("D");

    queue.removeAll(Arrays.asList("A", "C"));
    assertThat(queue.toArray()).containsExactly("B", "D");

    queue.removeAll(Arrays.asList("X", "B", "Y", "D"));
    assertThat(queue.isEmpty()).isTrue();
  }

  @Test
  public void testRetainAll() {
    DCircularQueue<String> queue = getFactory().createDCircularQueue(testName.getMethodName(), 4);

    queue.add("A");
    queue.add("B");
    queue.add("C");
    queue.add("D");

    queue.retainAll(Arrays.asList("A", "C"));
    assertThat(queue.toArray()).containsExactly("A", "C");
  }

  @Test
  public void testClear() {
    DCircularQueue<String> queue = getFactory().createDCircularQueue(testName.getMethodName(), 3);

    queue.add("A");
    queue.add("B");
    queue.add("C");

    queue.clear();
    assertThat(queue.isEmpty()).isTrue();
  }

  @Test
  public void testForeach() {
    DCircularQueue<String> queue = getFactory().createDCircularQueue(testName.getMethodName(), 3);

    queue.add("A");
    queue.add("B");
    queue.add("C");

    List<String> result = new ArrayList<>();
    queue.forEach(x -> result.add(x));
    assertThat(queue.toArray()).containsExactlyElementsOf(result);
  }

}
