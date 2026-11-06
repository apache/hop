/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.annotation;

import java.util.ArrayList;
import java.util.List;
import java.util.function.ToIntFunction;

/** Places an annotation balloon outside the icon it is anchored to. */
public final class AnnotationGeometry {
  public static final int GAP_PX = 12;
  public static final int PADDING_PX = 6;
  public static final int MAX_TEXT_WIDTH_PX = 180;
  public static final int RADIUS_PX = 8;

  private AnnotationGeometry() {}

  /** A rectangle in the same coordinate space as the icon. */
  public record Bounds(int x, int y, int width, int height) {
    public boolean overlaps(Bounds other) {
      return x < other.x + other.width
          && x + width > other.x
          && y < other.y + other.height
          && y + height > other.y;
    }

    public int centerX() {
      return x + width / 2;
    }

    public int centerY() {
      return y + height / 2;
    }
  }

  /** A straight connector between two points. */
  public record Segment(int x1, int y1, int x2, int y2) {}

  /**
   * Balloon rectangle for the measured text. Padding is added here. The text size is the wrapped
   * block, not including padding.
   *
   * <p>{@code magnification} is not applied. {@code SwtGc.setTransform} and {@code
   * SvgGc.setTransform} already scale everything drawn, and {@code IGc#textExtent} measures in that
   * unscaled space. The graph hit-tests with {@code screen2real}, which divides the canvas point by
   * the same factor and subtracts the scroll offset before comparing it to the click rectangle.
   * That rectangle is this balloon with the offset removed, so a click lands at every zoom while
   * the gap and padding stay {@link #GAP_PX} and {@link #PADDING_PX}.
   *
   * @param icon icon rectangle
   * @param textWidth wrapped text width
   * @param textHeight wrapped text height
   * @param placement where the balloon sits; null is {@link AnnotationPlacement#TOP}
   * @param magnification canvas magnification, accepted and not applied
   * @return the balloon, fully outside the icon when the gap is positive
   */
  public static Bounds balloon(
      Bounds icon,
      int textWidth,
      int textHeight,
      AnnotationPlacement placement,
      float magnification) {
    // The graphics transform already scales these pixels. Multiplying by magnification would
    // scale the callout a second time. The parameter stays so callers and tests can pass the
    // canvas zoom and see that the rectangle does not change.
    int gap = GAP_PX;
    int padding = PADDING_PX;
    int width = Math.max(0, textWidth) + 2 * padding;
    int height = Math.max(0, textHeight) + 2 * padding;
    AnnotationPlacement side = placement == null ? AnnotationPlacement.TOP : placement;
    int x =
        switch (side) {
          case TOP, BOTTOM -> icon.centerX() - width / 2;
          case TOP_RIGHT, RIGHT, BOTTOM_RIGHT -> icon.x + icon.width + gap;
          case TOP_LEFT, LEFT, BOTTOM_LEFT -> icon.x - gap - width;
        };
    int y =
        switch (side) {
          case TOP, TOP_LEFT, TOP_RIGHT -> icon.y - gap - height;
          case LEFT, RIGHT -> icon.centerY() - height / 2;
          case BOTTOM, BOTTOM_LEFT, BOTTOM_RIGHT -> icon.y + icon.height + gap;
        };
    return new Bounds(x, y, width, height);
  }

  /**
   * Dashed connector from the balloon edge to the icon edge. Each end is the point on that
   * rectangle closest to the other rectangle's center.
   *
   * @param balloon the callout rectangle
   * @param icon the transform or action icon
   * @return the segment to draw
   */
  public static Segment connector(Bounds balloon, Bounds icon) {
    int[] from = nearestPoint(balloon, icon.centerX(), icon.centerY());
    int[] to = nearestPoint(icon, balloon.centerX(), balloon.centerY());
    return new Segment(from[0], from[1], to[0], to[1]);
  }

  private static int[] nearestPoint(Bounds rect, int px, int py) {
    int x = Math.min(Math.max(px, rect.x()), rect.x() + rect.width());
    int y = Math.min(Math.max(py, rect.y()), rect.y() + rect.height());
    return new int[] {x, y};
  }

  /**
   * Wraps text to a maximum width. Existing line breaks are kept. A word wider than the limit is
   * split by character.
   *
   * @param text the annotation text
   * @param maxWidth maximum line width in the same units as {@code widthOf}
   * @param widthOf measures one line
   * @return the wrapped lines, or an empty list when the text is blank
   */
  public static List<String> wrap(String text, int maxWidth, ToIntFunction<String> widthOf) {
    if (text == null || text.isBlank()) {
      return List.of();
    }
    int limit = Math.max(1, maxWidth);
    List<String> lines = new ArrayList<>();
    for (String paragraph : text.split("\\R", -1)) {
      if (paragraph.isEmpty() || widthOf.applyAsInt(paragraph) <= limit) {
        lines.add(paragraph);
      } else {
        lines.addAll(wrapParagraph(paragraph, limit, widthOf));
      }
    }
    return lines;
  }

  private static List<String> wrapParagraph(
      String paragraph, int limit, ToIntFunction<String> widthOf) {
    List<String> lines = new ArrayList<>();
    StringBuilder current = new StringBuilder();
    for (String word : paragraph.split(" ", -1)) {
      if (word.isEmpty()) {
        continue;
      }
      if (widthOf.applyAsInt(word) > limit) {
        flush(lines, current);
        lines.addAll(breakWord(word, limit, widthOf));
        continue;
      }
      if (current.isEmpty()) {
        current.append(word);
        continue;
      }
      String candidate = current + " " + word;
      if (widthOf.applyAsInt(candidate) <= limit) {
        current.append(' ').append(word);
      } else {
        flush(lines, current);
        current.append(word);
      }
    }
    flush(lines, current);
    return lines;
  }

  private static List<String> breakWord(String word, int limit, ToIntFunction<String> widthOf) {
    List<String> parts = new ArrayList<>();
    StringBuilder current = new StringBuilder();
    for (int i = 0; i < word.length(); i++) {
      current.append(word.charAt(i));
      if (widthOf.applyAsInt(current.toString()) > limit && current.length() > 1) {
        current.setLength(current.length() - 1);
        parts.add(current.toString());
        current.setLength(0);
        current.append(word.charAt(i));
      }
    }
    if (!current.isEmpty()) {
      parts.add(current.toString());
    }
    return parts;
  }

  private static void flush(List<String> lines, StringBuilder current) {
    if (!current.isEmpty()) {
      lines.add(current.toString());
      current.setLength(0);
    }
  }
}
