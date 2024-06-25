/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype;

import java.io.Serializable;
import java.util.Objects;

class Movie implements Serializable {
  final String title;
  private int views;

  Movie(final String title) {
    this.title = title;
  }

  void incrementViews() {
    views += 1;
  }

  int getViews() {
    return views;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Movie movie = (Movie) o;
    return Objects.equals(title, movie.title);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(title);
  }

  @Override
  public String toString() {
    return "Movie{" +
        "title='" + title + '\'' +
        ", views=" + views +
        '}';
  }

}
