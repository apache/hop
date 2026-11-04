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

package org.apache.hop.core.gui;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class HopPathTest {

  private static final int ICON_SIZE = 32;
  private static final double DELTA = 0.5;

  @Test
  void straightPathKeepsCenterToCenterLine() {
    HopPath path = HopPath.create(HopLineShape.STRAIGHT, 0, 0, 100, 0, ICON_SIZE, true, true);

    assertFalse(path.isCurved());
    assertEquals(100, path.getLength(), DELTA);
    assertArrayEquals(new double[] {65, 0}, path.pointAt(0.65), DELTA);
    assertEquals(0, path.angleAt(0.5), 1e-9);
  }

  @Test
  void curvedPathLeavesRightAndEntersLeftWhenTargetIsToTheRight() {
    HopPath path = HopPath.create(HopLineShape.CURVED, 0, 0, 200, 60, ICON_SIZE, true, true);

    assertTrue(path.isCurved());
    assertArrayEquals(new double[] {16, 0}, path.pointAt(0), DELTA);
    assertArrayEquals(new double[] {184, 60}, path.pointAt(1), DELTA);
    // Horizontal at both ends.
    assertEquals(0, path.angleAt(0), 0.1);
    assertEquals(0, path.angleAt(1), 0.1);
  }

  @Test
  void curvedPathEntersFromTheTopWhenTargetIsBelow() {
    HopPath path = HopPath.create(HopLineShape.CURVED, 0, 0, 40, 200, ICON_SIZE, true, true);

    assertArrayEquals(new double[] {0, 16}, path.pointAt(0), DELTA);
    assertArrayEquals(new double[] {40, 184}, path.pointAt(1), DELTA);
    assertEquals(Math.PI / 2, path.angleAt(1), 0.1);
  }

  @Test
  void curvedPathEntersFromTheRightWhenTargetIsToTheLeft() {
    HopPath path = HopPath.create(HopLineShape.CURVED, 200, 0, 0, 0, ICON_SIZE, true, true);

    assertArrayEquals(new double[] {184, 0}, path.pointAt(0), DELTA);
    assertArrayEquals(new double[] {16, 0}, path.pointAt(1), DELTA);
  }

  @Test
  void freeEndPointIsNotMovedToAnIconBorder() {
    HopPath path = HopPath.create(HopLineShape.CURVED, 0, 0, 200, 0, ICON_SIZE, true, false);

    assertArrayEquals(new double[] {200, 0}, path.pointAt(1), DELTA);
  }

  @Test
  void distanceToFindsPointsOnTheCurve() {
    HopPath path = HopPath.create(HopLineShape.CURVED, 0, 0, 300, 120, ICON_SIZE, true, true);
    double[] middle = path.pointAt(0.5);

    assertTrue(path.distanceTo(middle[0], middle[1]) < 1);
    assertTrue(path.distanceTo(middle[0], middle[1] - 30) > 10);
  }

  @Test
  void unknownCodeFallsBackToStraight() {
    assertSame(HopLineShape.STRAIGHT, HopLineShape.lookupCode(null));
    assertSame(HopLineShape.STRAIGHT, HopLineShape.lookupCode("nonsense"));
    assertSame(HopLineShape.CURVED, HopLineShape.lookupCode("CURVED"));
  }
}
