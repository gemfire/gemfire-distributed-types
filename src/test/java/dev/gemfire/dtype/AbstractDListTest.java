/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;

import org.assertj.core.util.Lists;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public abstract class AbstractDListTest {

  @Rule
  public TestName testName = new TestName();

  abstract DTypeFactory getFactory();

  @Test
  public void testAdd() {
    List<String> list = getFactory().createDList(testName.getMethodName());
    list.add("foo");
    assertThat(list.size()).isEqualTo(1);

    list.add("bar");
    assertThat(list.size()).isEqualTo(2);

    list.add(1, "baz");
    assertThat(list.toArray()).containsExactly("foo", "baz", "bar");
  }

  @Test
  public void testRemove() {
    List<String> list = getFactory().createDList(testName.getMethodName());
    list.add("foo");
    list.add("bar");
    list.add("baz");
    list.add("zap");

    list.remove("foo");

    assertThat(list.contains("foo")).isFalse();
    assertThat(list.toArray()).containsExactly("bar", "baz", "zap");

    list.remove(1);
    assertThat(list.toArray()).containsExactly("bar", "zap");
  }

  @Test
  public void testIteratingRemove() {
    List<String> list = getFactory().createDList(testName.getMethodName());
    list.add("foo");
    list.add("bar");
    list.add("baz");

    Iterator<String> iterator = list.iterator();
    while (iterator.hasNext()) {
      iterator.next();
      iterator.remove();
    }

    assertThat(list.size()).isEqualTo(0);
  }

  @Test
  public void testContainsAll() {
    List<String> list = getFactory().createDList(testName.getMethodName());
    list.add("foo");
    list.add("bar");
    list.add("baz");

    List<String> contains = Lists.list("baz", "foo", "bar");

    assertThat(list.containsAll(contains)).isTrue();
  }

  @Test
  public void testAddAll() {
    List<String> list = getFactory().createDList(testName.getMethodName());
    list.add("foo");

    list.addAll(Lists.list("baz", "foo", "bar"));

    assertThat(list).containsExactly("foo", "baz", "foo", "bar");
  }

  @Test
  public void testAddAllIndex() {
    List<String> list = getFactory().createDList(testName.getMethodName());
    list.add("foo");
    list.add("bar");

    list.addAll(1, Lists.list("A", "B"));

    assertThat(list).containsExactly("foo", "A", "B", "bar");
  }

  @Test
  public void testRemoveAll() {
    List<String> list = getFactory().createDList(testName.getMethodName());
    list.add("foo");
    list.add("bar");
    list.add("baz");
    list.add("zap");

    list.removeAll(Lists.list("bar", "zap"));

    assertThat(list).containsExactly("foo", "baz");
  }

  @Test
  public void testRetainAll() {
    List<String> list = getFactory().createDList(testName.getMethodName());
    list.add("foo");
    list.add("bar");
    list.add("baz");
    list.add("zap");

    list.retainAll(Lists.list("bar", "zap"));

    assertThat(list).containsExactly("bar", "zap");
  }

  @Test
  public void testIndexOf() {
    List<String> list = getFactory().createDList(testName.getMethodName());
    list.add("foo");
    list.add("bar");
    list.add("baz");

    assertThat(list.indexOf("foo")).isEqualTo(0);
    assertThat(list.indexOf("bar")).isEqualTo(1);
    assertThat(list.indexOf("baz")).isEqualTo(2);
  }

  @Test
  public void testLastIndexOf() {
    List<String> list = getFactory().createDList(testName.getMethodName());
    list.add("foo");
    list.add("bar");
    list.add("foo");

    assertThat(list.lastIndexOf("foo")).isEqualTo(2);
    assertThat(list.lastIndexOf("bar")).isEqualTo(1);
  }

  @Test
  public void testSubList() {
    List<String> list = getFactory().createDList(testName.getMethodName());
    list.add("foo");
    list.add("bar");
    list.add("baz");
    list.add("zap");

    assertThat(list.subList(1, 3)).containsExactly("bar", "baz");
  }

  @Test
  public void testSort() {
    List<String> list = getFactory().createDList(testName.getMethodName());
    list.add("foo");
    list.add("bar");
    list.add("baz");
    list.add("zap");

    list.sort(Comparator.naturalOrder());

    assertThat(list.toArray()).containsExactly("bar", "baz", "foo", "zap");
  }

  @Test
  public void testRemoveIf() {
    List<Integer> list = getFactory().createDList(testName.getMethodName());
    for (int i = 0; i < 10; i++) {
      list.add(i);
    }

    list.removeIf((Predicate<Integer> & Serializable) x -> x % 2 == 0);

    assertThat(list.toArray()).containsExactly(1, 3, 5, 7, 9);
  }

}
