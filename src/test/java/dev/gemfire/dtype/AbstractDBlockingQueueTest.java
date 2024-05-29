/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.Future;

import org.assertj.core.util.Lists;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public abstract class AbstractDBlockingQueueTest {

  @ClassRule
  public static ExecutorServiceRule executor = new ExecutorServiceRule();

  @Rule
  public TestName testName = new TestName();

  abstract DTypeFactory getFactory();

  @Test
  public void testAddFirst() {
    DBlockingQueue<String> queue = getFactory().createDQueue(testName.getMethodName());
    queue.addFirst("A");
    queue.addFirst("B");
    queue.addFirst("C");
    assertThat(queue.size()).isEqualTo(3);

    assertThat(queue.toArray()).containsExactly("C", "B", "A");
  }

  @Test
  public void testAddLast() {
    DBlockingQueue<String> queue = getFactory().createDQueue(testName.getMethodName());
    queue.addLast("A");
    queue.addLast("B");
    queue.addLast("C");

    assertThat(queue.size()).isEqualTo(3);
    assertThat(queue.toArray()).containsExactly("A", "B", "C");
  }

  @Test
  public void testOfferFirst() {
    DBlockingQueue<String> queue = getFactory().createDQueue(testName.getMethodName(), 2);
    queue.offerFirst("A");
    queue.offerFirst("B");

    assertThat(queue.offerFirst("C")).isFalse();
    assertThat(queue.toArray()).containsExactly("B", "A");
  }

  @Test
  public void testOfferLast() {
    DBlockingQueue<String> queue = getFactory().createDQueue(testName.getMethodName(), 2);
    queue.offerLast("A");
    queue.offerLast("B");

    assertThat(queue.offerLast("C")).isFalse();
    assertThat(queue.toArray()).containsExactly("A", "B");
  }

  @Test
  public void testRemoveFirst() {
    DBlockingQueue<String> queue = getFactory().createDQueue(testName.getMethodName());
    queue.add("A");
    queue.add("B");

    assertThat(queue.removeFirst()).isEqualTo("A");
    assertThat(queue.toArray()).containsExactly("B");
  }

  @Test
  public void testRemoveLast() {
    DBlockingQueue<String> queue = getFactory().createDQueue(testName.getMethodName());
    queue.add("A");
    queue.add("B");

    assertThat(queue.removeLast()).isEqualTo("B");
    assertThat(queue.toArray()).containsExactly("A");
  }

  @Test
  public void testPollFirst() {
    DBlockingQueue<String> queue = getFactory().createDQueue(testName.getMethodName());
    queue.add("A");
    queue.add("B");

    assertThat(queue.pollFirst()).isEqualTo("A");
    assertThat(queue.pollFirst()).isEqualTo("B");
    assertThat(queue.pollFirst()).isNull();
  }

  @Test
  public void testPollLast() {
    DBlockingQueue<String> queue = getFactory().createDQueue(testName.getMethodName());
    queue.add("A");
    queue.add("B");

    assertThat(queue.pollLast()).contains("B");
    assertThat(queue.pollLast()).contains("A");
    assertThat(queue.pollLast()).isNull();
  }

  @Test
  public void testGetFirst() {
    DBlockingQueue<String> queue = getFactory().createDQueue(testName.getMethodName());
    queue.add("A");
    queue.add("B");

    assertThat(queue.getFirst()).contains("A");
    queue.clear();
    assertThatThrownBy(() -> queue.getFirst()).isInstanceOf(NoSuchElementException.class);
  }

  @Test
  public void testGetLast() {
    DBlockingQueue<String> queue = getFactory().createDQueue(testName.getMethodName());
    queue.add("A");
    queue.add("B");

    assertThat(queue.getLast()).contains("B");
    queue.clear();
    assertThatThrownBy(() -> queue.getLast()).isInstanceOf(NoSuchElementException.class);
  }

  @Test
  public void testPeekFirst() {
    DBlockingQueue<String> queue = getFactory().createDQueue(testName.getMethodName());
    queue.add("A");
    queue.add("B");

    assertThat(queue.peekFirst()).contains("A");
    queue.clear();
    assertThat(queue.peekFirst()).isNull();
  }

  @Test
  public void testPeekLast() {
    DBlockingQueue<String> queue = getFactory().createDQueue(testName.getMethodName());
    queue.add("A");
    queue.add("B");

    assertThat(queue.peekLast()).contains("B");
    queue.clear();
    assertThat(queue.peekLast()).isNull();
  }

  @Test
  public void testPutFirst() throws Exception {
    DBlockingQueue<String> queue = getFactory().createDQueue(testName.getMethodName(), 1);
    queue.add("A");

    Future<?> future = executor.submit(() -> queue.putFirst("B"));
    queue.remove();

    future.get();
    assertThat(queue.remainingCapacity()).isEqualTo(0);
  }

  @Test
  public void testPutLast() throws Exception {
    DBlockingQueue<String> queue = getFactory().createDQueue(testName.getMethodName(), 1);
    queue.add("A");

    Future<?> future = executor.submit(() -> queue.putLast("B"));
    queue.remove();

    future.get();
    assertThat(queue.remainingCapacity()).isEqualTo(0);
  }

  @Ignore
  @Test
  public void testOfferFirstWithTimeout() throws Exception {
    // Not implemented yet
  }

  @Ignore
  @Test
  public void testOfferLastWithTimeout() {

  }

  @Test
  public void testTakeFirst() throws Exception {
    DBlockingQueue<String> queue = getFactory().createDQueue(testName.getMethodName());
    queue.add("A");
    queue.add("B");

    assertThat(queue.takeFirst()).isEqualTo("A");

    queue.remove();
    Future<String> future = executor.submit(() -> queue.takeFirst());

    queue.add("C");
    assertThat(future.get()).isEqualTo("C");
  }

  @Test
  public void testTakeLast() throws Exception {
    DBlockingQueue<String> queue = getFactory().createDQueue(testName.getMethodName());
    queue.add("A");
    queue.add("B");

    assertThat(queue.takeLast()).isEqualTo("B");

    queue.remove();
    Future<String> future = executor.submit(() -> queue.takeLast());

    queue.add("C");
    assertThat(future.get()).isEqualTo("C");
  }

  @Ignore
  @Test
  public void testPollFirstWithTimeout() {
    // Not implemented yet
  }

  @Ignore
  @Test
  public void testPollLastWithTimeout() {
    // Not implemented yet
  }

  @Test
  public void testRemoveFirstOccurrence() {
    DBlockingQueue<String> queue = getFactory().createDQueue(testName.getMethodName());
    queue.add("A");
    queue.add("B");
    queue.add("A");

    assertThat(queue.removeFirstOccurrence("A")).isTrue();
    assertThat(queue.removeFirstOccurrence("C")).isFalse();
    assertThat(queue.toArray()).contains("B", "A");
  }

  @Test
  public void testRemoveLastOccurrence() {
    DBlockingQueue<String> queue = getFactory().createDQueue(testName.getMethodName());
    queue.add("A");
    queue.add("B");
    queue.add("A");

    assertThat(queue.removeLastOccurrence("A")).isTrue();
    assertThat(queue.removeLastOccurrence("C")).isFalse();
    assertThat(queue.toArray()).contains("A", "B");
  }

  @Test
  public void testOffer() {
    DBlockingQueue<String> queue = getFactory().createDQueue(testName.getMethodName(), 1);
    assertThat(queue.offer("A")).isTrue();
    assertThat(queue.offer("B")).isFalse();
  }

  @Test
  public void testPut() throws Exception {
    DBlockingQueue<String> queue = getFactory().createDQueue(testName.getMethodName(), 1);
    queue.add("A");

    Future<Void> future = executor.submit(() -> queue.put("B"));

    queue.remove();
    future.get();

    assertThat(queue.remainingCapacity()).isEqualTo(0);
  }

  @Ignore
  @Test
  public void testOfferWithTimeout() {
    // Not implemented yet
  }

  @Test
  public void testRemove() {
    DBlockingQueue<String> queue = getFactory().createDQueue(testName.getMethodName());
    queue.add("A");
    queue.add("B");

    assertThat(queue.remove()).isEqualTo("A");
    assertThat(queue.remove()).isEqualTo("B");
    assertThatThrownBy(() -> queue.remove()).isInstanceOf(NoSuchElementException.class);
  }

  @Test
  public void testPoll() {
    DBlockingQueue<String> queue = getFactory().createDQueue(testName.getMethodName());
    queue.add("A");
    queue.add("B");

    assertThat(queue.poll()).isEqualTo("A");
    assertThat(queue.poll()).isEqualTo("B");
    assertThat(queue.poll()).isNull();
  }

  @Ignore
  @Test
  public void testTake() throws Exception {
    DBlockingQueue<String> queue = getFactory().createDQueue(testName.getMethodName());
    queue.add("A");
    queue.add("B");

    assertThat(queue.take()).isEqualTo("A");

    queue.remove();
    Future<String> future = executor.submit(() -> queue.take());

    queue.add("C");
    assertThat(future.get()).isEqualTo("C");
  }

  @Ignore
  @Test
  public void testPollWithTimeout() {
    // Not implemented yet
  }

  @Test
  public void testRemainingCapacity() {
    DBlockingQueue<String> queue = getFactory().createDQueue(testName.getMethodName(), 2);
    assertThat(queue.remainingCapacity()).isEqualTo(2);

    queue.add("A");
    assertThat(queue.remainingCapacity()).isEqualTo(1);

    queue.add("B");
    assertThat(queue.remainingCapacity()).isEqualTo(0);
  }

  @Test
  public void testElement() {
    DBlockingQueue<String> queue = getFactory().createDQueue(testName.getMethodName());
    queue.add("A");
    queue.add("B");

    assertThat(queue.element()).isEqualTo("A");
    queue.clear();
    assertThatThrownBy(() -> queue.element()).isInstanceOf(NoSuchElementException.class);
  }

  @Test
  public void testPeek() {
    DBlockingQueue<String> queue = getFactory().createDQueue(testName.getMethodName());
    queue.add("A");
    queue.add("B");

    assertThat(queue.peek()).isEqualTo("A");
    queue.clear();
    assertThat(queue.peek()).isNull();
  }

  @Test
  public void testRemoveObject() {
    DBlockingQueue<String> queue = getFactory().createDQueue(testName.getMethodName());
    queue.add("A");
    queue.add("B");
    queue.add("C");

    assertThat(queue.remove("B")).isTrue();
    assertThat(queue.remove("B")).isFalse();
    assertThat(queue.toArray()).contains("A", "C");
  }

  @Test
  public void testContainsAll() {
    DBlockingQueue<String> queue = getFactory().createDQueue(testName.getMethodName());
    queue.add("A");
    queue.add("B");
    queue.add("C");

    assertThat(queue.containsAll(Lists.list("A", "B", "C"))).isTrue();
    assertThat(queue.containsAll(Lists.list("C", "B", "A"))).isTrue();
    assertThat(queue.containsAll(Lists.list("A", "B", "D"))).isFalse();
  }

  @Test
  public void testAddAll() {
    DBlockingQueue<String> queue = getFactory().createDQueue(testName.getMethodName(), 4);

    assertThat(queue.addAll(Lists.list("A", "B", "C"))).isTrue();
    assertThat(queue.containsAll(Lists.list("C", "B", "A"))).isTrue();

    assertThatThrownBy(() -> queue.addAll(Lists.list("X", "Y", "Z")))
        .isInstanceOf(IllegalStateException.class);
    assertThat(queue.toArray()).contains("A");
    assertThat(queue.containsAll(Lists.list("A", "B", "D"))).isFalse();
  }

  @Test
  public void testRemoveAll() {
    DBlockingQueue<String> queue = getFactory().createDQueue(testName.getMethodName());
    queue.add("A");
    queue.add("B");
    queue.add("C");
    queue.add("D");

    assertThat(queue.removeAll(Lists.list("A", "C"))).isTrue();
    assertThat(queue.removeAll(Lists.list("D", "E"))).isTrue();
    assertThat(queue.removeAll(Lists.list("E"))).isFalse();
    assertThat(queue.toArray()).contains("B");
  }

  @Test
  public void testRetainAll() {
    DBlockingQueue<String> queue = getFactory().createDQueue(testName.getMethodName());
    queue.add("A");
    queue.add("B");
    queue.add("C");
    queue.add("D");

    assertThat(queue.retainAll(Lists.list("A", "C"))).isTrue();
    assertThat(queue.retainAll(Lists.list("C", "E"))).isTrue();
    assertThat(queue.toArray()).contains("C");

    assertThat(queue.retainAll(Lists.list("C"))).isFalse();
    assertThat(queue.toArray()).contains("C");

    assertThat(queue.retainAll(Lists.list("E"))).isTrue();
    assertThat(queue.toArray()).isEmpty();
  }

  @Test
  public void testContains() {
    DBlockingQueue<String> queue = getFactory().createDQueue(testName.getMethodName());
    queue.add("A");
    queue.add("B");

    assertThat(queue.contains("A")).isTrue();
    assertThat(queue.contains("Z")).isFalse();
  }

  @Test
  public void testDrainTo() {
    DBlockingQueue<String> queue = getFactory().createDQueue(testName.getMethodName());
    queue.add("A");
    queue.add("B");
    queue.add("C");

    List<String> expected = new ArrayList<>();
    queue.drainTo(expected);

    assertThat(expected).containsExactly("A", "B", "C");
    assertThat(queue.toArray()).isEmpty();
  }

  @Test
  public void testDrainToWithMaxCapacity() {
    DBlockingQueue<String> queue = getFactory().createDQueue(testName.getMethodName());
    queue.add("A");
    queue.add("B");
    queue.add("C");

    List<String> expected = new ArrayList<>();
    queue.drainTo(expected, 2);

    assertThat(expected).containsExactly("A", "B");
    assertThat(queue.toArray()).containsExactly("C");
  }

}
