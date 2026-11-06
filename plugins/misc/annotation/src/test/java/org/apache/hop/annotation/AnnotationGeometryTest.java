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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.annotation.AnnotationGeometry.Bounds;
import org.apache.hop.annotation.AnnotationGeometry.Segment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

class AnnotationGeometryTest {

  private static final Bounds ICON = new Bounds(100, 80, 32, 32);

  @ParameterizedTest
  @EnumSource(AnnotationPlacement.class)
  void balloonSitsOutsideTheIconOnThatSide(AnnotationPlacement placement) {
    int gap = AnnotationGeometry.GAP_PX;
    Bounds balloon = AnnotationGeometry.balloon(ICON, 40, 16, placement, 1f);

    assertFalse(balloon.overlaps(ICON));
    assertEquals(40 + 2 * AnnotationGeometry.PADDING_PX, balloon.width());
    switch (placement) {
      case TOP -> {
        assertEquals(ICON.y(), balloon.y() + balloon.height() + gap);
        assertEquals(ICON.centerX(), balloon.centerX());
      }
      case BOTTOM -> {
        assertEquals(ICON.y() + ICON.height() + gap, balloon.y());
        assertEquals(ICON.centerX(), balloon.centerX());
      }
      case LEFT -> {
        assertEquals(ICON.x(), balloon.x() + balloon.width() + gap);
        assertEquals(ICON.centerY(), balloon.centerY());
      }
      case RIGHT -> {
        assertEquals(ICON.x() + ICON.width() + gap, balloon.x());
        assertEquals(ICON.centerY(), balloon.centerY());
      }
      case TOP_LEFT -> {
        assertEquals(ICON.x(), balloon.x() + balloon.width() + gap);
        assertEquals(ICON.y(), balloon.y() + balloon.height() + gap);
      }
      case TOP_RIGHT -> {
        assertEquals(ICON.x() + ICON.width() + gap, balloon.x());
        assertEquals(ICON.y(), balloon.y() + balloon.height() + gap);
      }
      case BOTTOM_LEFT -> {
        assertEquals(ICON.x(), balloon.x() + balloon.width() + gap);
        assertEquals(ICON.y() + ICON.height() + gap, balloon.y());
      }
      case BOTTOM_RIGHT -> {
        assertEquals(ICON.x() + ICON.width() + gap, balloon.x());
        assertEquals(ICON.y() + ICON.height() + gap, balloon.y());
      }
      default -> throw new IllegalStateException(placement.name());
    }
  }

  @ParameterizedTest
  @ValueSource(floats = {0.5f, 1f, 2f})
  void balloonSizeDoesNotDependOnMagnification(float magnification) {
    for (AnnotationPlacement placement : AnnotationPlacement.values()) {
      assertEquals(
          AnnotationGeometry.balloon(ICON, 40, 16, placement, 1f),
          AnnotationGeometry.balloon(ICON, 40, 16, placement, magnification),
          placement.name());
    }

    Bounds balloon =
        AnnotationGeometry.balloon(ICON, 40, 16, AnnotationPlacement.TOP, magnification);
    assertEquals(40 + 2 * AnnotationGeometry.PADDING_PX, balloon.width());
    assertEquals(16 + 2 * AnnotationGeometry.PADDING_PX, balloon.height());
    assertEquals(AnnotationGeometry.GAP_PX, ICON.y() - (balloon.y() + balloon.height()));
    assertEquals(ICON.centerX(), balloon.centerX());

    Bounds right =
        AnnotationGeometry.balloon(ICON, 40, 16, AnnotationPlacement.RIGHT, magnification);
    assertEquals(AnnotationGeometry.GAP_PX, right.x() - (ICON.x() + ICON.width()));
    assertEquals(ICON.centerY(), right.centerY());
  }

  @Test
  void connectorJoinsTheFacingEdges() {
    Bounds above = AnnotationGeometry.balloon(ICON, 40, 16, AnnotationPlacement.TOP, 1f);
    Segment vertical = AnnotationGeometry.connector(above, ICON);
    assertEquals(above.centerX(), vertical.x1());
    assertEquals(above.y() + above.height(), vertical.y1());
    assertEquals(ICON.centerX(), vertical.x2());
    assertEquals(ICON.y(), vertical.y2());

    Bounds corner = AnnotationGeometry.balloon(ICON, 40, 16, AnnotationPlacement.TOP_LEFT, 1f);
    Segment diagonal = AnnotationGeometry.connector(corner, ICON);
    assertEquals(corner.x() + corner.width(), diagonal.x1());
    assertEquals(corner.y() + corner.height(), diagonal.y1());
    assertEquals(ICON.x(), diagonal.x2());
    assertEquals(ICON.y(), diagonal.y2());
  }

  @Test
  void nullPlacementIsAboveTheIcon() {
    Bounds balloon = AnnotationGeometry.balloon(ICON, 20, 10, null, 1f);
    assertTrue(balloon.y() + balloon.height() <= ICON.y());
  }

  @Test
  void wrapKeepsLineBreaksAndSplitsLongWords() {
    assertTrue(AnnotationGeometry.wrap("   ", 10, String::length).isEmpty());
    assertEquals(List.of("aa", "", "bb"), AnnotationGeometry.wrap("aa\n\nbb", 10, String::length));
    assertEquals(
        List.of("hello", "world"), AnnotationGeometry.wrap("hello world", 5, String::length));
    assertEquals(List.of("abc", "def"), AnnotationGeometry.wrap("abcdef", 3, String::length));
  }
}
