/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import org.assertj.core.util.Lists;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

public abstract class AbstractDBlockingQueueTest {

  private static final String QUEUE = "queue";

  @After
  public void cleanup() {
    getFactory().destroy(QUEUE);
  }

  abstract DTypeFactory getFactory();

  @Test
  public void testAddFirst() {
    DBlockingDeque<String> queue = getFactory().createDQueue(QUEUE);
    queue.addFirst("A");
    queue.addFirst("B");
    queue.addFirst("C");
    assertThat(queue.size()).isEqualTo(3);

    assertThat(queue.toArray()).containsExactly("C", "B", "A");
  }

  @Test
  public void testAddLast() {
    DBlockingDeque<String> queue = getFactory().createDQueue(QUEUE);
    queue.addLast("A");
    queue.addLast("B");
    queue.addLast("C");

    assertThat(queue.size()).isEqualTo(3);
    assertThat(queue.toArray()).containsExactly("A", "B", "C");
  }

  @Test
  public void testOfferFirst() {
    DBlockingDeque<String> queue = getFactory().createDQueue(QUEUE, 2);
    queue.offerFirst("A");
    queue.offerFirst("B");

    assertThat(queue.offerFirst("C")).isFalse();
    assertThat(queue.toArray()).containsExactly("B", "A");
  }

  @Test
  public void testOfferLast() {
    DBlockingDeque<String> queue = getFactory().createDQueue(QUEUE, 2);
    queue.offerLast("A");
    queue.offerLast("B");

    assertThat(queue.offerLast("C")).isFalse();
    assertThat(queue.toArray()).containsExactly("A", "B");
  }

  @Test
  public void testRemoveFirst() {
    DBlockingDeque<String> queue = getFactory().createDQueue(QUEUE);
    queue.add("A");
    queue.add("B");

    assertThat(queue.removeFirst()).isEqualTo("A");
    assertThat(queue.toArray()).containsExactly("B");
  }

  @Test
  public void testRemoveLast() {
    DBlockingDeque<String> queue = getFactory().createDQueue(QUEUE);
    queue.add("A");
    queue.add("B");

    assertThat(queue.removeLast()).isEqualTo("B");
    assertThat(queue.toArray()).containsExactly("A");
  }

  @Test
  public void testPollFirst() {
    DBlockingDeque<String> queue = getFactory().createDQueue(QUEUE);
    queue.add("A");
    queue.add("B");

    assertThat(queue.pollFirst()).isEqualTo("A");
    assertThat(queue.pollFirst()).isEqualTo("B");
    assertThat(queue.pollFirst()).isNull();
  }

  @Test
  public void testPollLast() {
    DBlockingDeque<String> queue = getFactory().createDQueue(QUEUE);
    queue.add("A");
    queue.add("B");

    assertThat(queue.pollLast()).contains("B");
    assertThat(queue.pollLast()).contains("A");
    assertThat(queue.pollLast()).isNull();
  }

  @Test
  public void testGetFirst() {
    DBlockingDeque<String> queue = getFactory().createDQueue(QUEUE);
    queue.add("A");
    queue.add("B");

    assertThat(queue.getFirst()).contains("A");
    queue.clear();
    assertThatThrownBy(() -> queue.getFirst()).isInstanceOf(NoSuchElementException.class);
  }

  @Test
  public void testGetLast() {
    DBlockingDeque<String> queue = getFactory().createDQueue(QUEUE);
    queue.add("A");
    queue.add("B");

    assertThat(queue.getLast()).contains("B");
    queue.clear();
    assertThatThrownBy(() -> queue.getLast()).isInstanceOf(NoSuchElementException.class);
  }

  @Test
  public void testPeekFirst() {
    DBlockingDeque<String> queue = getFactory().createDQueue(QUEUE);
    queue.add("A");
    queue.add("B");

    assertThat(queue.peekFirst()).contains("A");
    queue.clear();
    assertThat(queue.peekFirst()).isNull();
  }

  @Test
  public void testPeekLast() {
    DBlockingDeque<String> queue = getFactory().createDQueue(QUEUE);
    queue.add("A");
    queue.add("B");

    assertThat(queue.peekLast()).contains("B");
    queue.clear();
    assertThat(queue.peekLast()).isNull();
  }

  @Ignore
  @Test
  public void testPutFirst() {

  }

  @Ignore
  @Test
  public void testPutLast() {

  }

  @Ignore
  @Test
  public void testOfferFirstWithTimeout() {

  }

  @Ignore
  @Test
  public void testOfferLastWithTimeout() {

  }

  @Ignore
  @Test
  public void testTakeFirst() {

  }

  @Ignore
  @Test
  public void testTakeLast() {

  }

  @Ignore
  @Test
  public void testPollFirstWithTimeout() {

  }

  @Ignore
  @Test
  public void testPollLastWithTimeout() {

  }

  @Test
  public void testRemoveFirstOccurrence() {
    DBlockingDeque<String> queue = getFactory().createDQueue(QUEUE);
    queue.add("A");
    queue.add("B");
    queue.add("A");

    assertThat(queue.removeFirstOccurrence("A")).isTrue();
    assertThat(queue.removeFirstOccurrence("C")).isFalse();
    assertThat(queue.toArray()).contains("B", "A");
  }

  @Test
  public void testRemoveLastOccurrence() {
    DBlockingDeque<String> queue = getFactory().createDQueue(QUEUE);
    queue.add("A");
    queue.add("B");
    queue.add("A");

    assertThat(queue.removeLastOccurrence("A")).isTrue();
    assertThat(queue.removeLastOccurrence("C")).isFalse();
    assertThat(queue.toArray()).contains("A", "B");
  }

  @Test
  public void testOffer() {
    DBlockingDeque<String> queue = getFactory().createDQueue(QUEUE, 1);
    assertThat(queue.offer("A")).isTrue();
    assertThat(queue.offer("B")).isFalse();
  }

  @Ignore
  @Test
  public void testPut() {

  }

  @Ignore
  @Test
  public void testOfferWithTimeout() {

  }

  @Test
  public void testRemove() {
    DBlockingDeque<String> queue = getFactory().createDQueue(QUEUE);
    queue.add("A");
    queue.add("B");

    assertThat(queue.remove()).isEqualTo("A");
    assertThat(queue.remove()).isEqualTo("B");
    assertThatThrownBy(() -> queue.remove()).isInstanceOf(NoSuchElementException.class);
  }

  @Test
  public void testPoll() {
    DBlockingDeque<String> queue = getFactory().createDQueue(QUEUE);
    queue.add("A");
    queue.add("B");

    assertThat(queue.poll()).isEqualTo("A");
    assertThat(queue.poll()).isEqualTo("B");
    assertThat(queue.poll()).isNull();
  }

  @Ignore
  @Test
  public void testTake() {

  }

  @Ignore
  @Test
  public void testPollWithTimeout() {

  }

  @Test
  public void testRemainingCapacity() {
    DBlockingDeque<String> queue = getFactory().createDQueue(QUEUE, 2);
    assertThat(queue.remainingCapacity()).isEqualTo(2);

    queue.add("A");
    assertThat(queue.remainingCapacity()).isEqualTo(1);

    queue.add("B");
    assertThat(queue.remainingCapacity()).isEqualTo(0);
  }

  @Test
  public void testElement() {
    DBlockingDeque<String> queue = getFactory().createDQueue(QUEUE);
    queue.add("A");
    queue.add("B");

    assertThat(queue.element()).isEqualTo("A");
    queue.clear();
    assertThatThrownBy(() -> queue.element()).isInstanceOf(NoSuchElementException.class);
  }

  @Test
  public void testPeek() {
    DBlockingDeque<String> queue = getFactory().createDQueue(QUEUE);
    queue.add("A");
    queue.add("B");

    assertThat(queue.peek()).isEqualTo("A");
    queue.clear();
    assertThat(queue.peek()).isNull();
  }

  @Test
  public void testRemoveObject() {
    DBlockingDeque<String> queue = getFactory().createDQueue(QUEUE);
    queue.add("A");
    queue.add("B");
    queue.add("C");

    assertThat(queue.remove("B")).isTrue();
    assertThat(queue.remove("B")).isFalse();
    assertThat(queue.toArray()).contains("A", "C");
  }

  @Test
  public void testContainsAll() {
    DBlockingDeque<String> queue = getFactory().createDQueue(QUEUE);
    queue.add("A");
    queue.add("B");
    queue.add("C");

    assertThat(queue.containsAll(Lists.list("A", "B", "C"))).isTrue();
    assertThat(queue.containsAll(Lists.list("C", "B", "A"))).isTrue();
    assertThat(queue.containsAll(Lists.list("A", "B", "D"))).isFalse();
  }

  @Test
  public void testAddAll() {
    DBlockingDeque<String> queue = getFactory().createDQueue(QUEUE, 4);

    assertThat(queue.addAll(Lists.list("A", "B", "C"))).isTrue();
    assertThat(queue.containsAll(Lists.list("C", "B", "A"))).isTrue();

    assertThatThrownBy(() -> queue.addAll(Lists.list("X", "Y", "Z")))
        .isInstanceOf(IllegalStateException.class);
    assertThat(queue.toArray()).contains("A");
    assertThat(queue.containsAll(Lists.list("A", "B", "D"))).isFalse();
  }

  @Test
  public void testRemoveAll() {
    DBlockingDeque<String> queue = getFactory().createDQueue(QUEUE);
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
    DBlockingDeque<String> queue = getFactory().createDQueue(QUEUE);
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
    DBlockingDeque<String> queue = getFactory().createDQueue(QUEUE);
    queue.add("A");
    queue.add("B");

    assertThat(queue.contains("A")).isTrue();
    assertThat(queue.contains("Z")).isFalse();
  }

  @Test
  public void testDrainTo() {
    DBlockingDeque<String> queue = getFactory().createDQueue(QUEUE);
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
    DBlockingDeque<String> queue = getFactory().createDQueue(QUEUE);
    queue.add("A");
    queue.add("B");
    queue.add("C");

    List<String> expected = new ArrayList<>();
    queue.drainTo(expected, 2);

    assertThat(expected).containsExactly("A", "B");
    assertThat(queue.toArray()).containsExactly("C");
  }

}
