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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.junit.jupiter.api.Test;

class PointAndDPointTest {

  @Test
  void pointMultiplyRounds() {
    Point p = new Point(10, 11);
    p.multiply(1.5f);
    assertEquals(15, p.getX());
    assertEquals(17, p.getY());
  }

  @Test
  void pointToDoubleAndDPointToPoint() {
    Point p = new Point(3, 7);
    DPoint d = p.toDouble();
    assertEquals(3.0, d.getX());
    assertEquals(7.0, d.getY());
    Point back = d.toPoint();
    assertEquals(3, back.getX());
    assertEquals(7, back.getY());
  }

  @Test
  void dPointMultiplyAndCopyConstructors() {
    DPoint a = new DPoint(2.0, 4.0);
    DPoint b = new DPoint(a);
    b.multiply(0.5);
    assertEquals(1.0, b.getX());
    assertEquals(2.0, b.getY());
    assertEquals(2.0, a.getX());

    DPoint fromInt = new DPoint(new Point(1, 2));
    assertEquals(1.0, fromInt.getX());
    assertEquals(2.0, fromInt.getY());
  }

  @Test
  void dPointEqualsHashCodeToString() {
    DPoint p1 = new DPoint(1.25, 2.5);
    DPoint p2 = new DPoint(1.25, 2.5);
    DPoint p3 = new DPoint(1.0, 2.0);
    assertEquals(p1, p2);
    assertEquals(p1.hashCode(), p2.hashCode());
    assertNotEquals(p1, p3);
    assertEquals("DPoint(1.25,2.5)", p1.toString());
  }

  @Test
  void pointEqualsAndToString() {
    Point p1 = new Point(4, 5);
    Point p2 = new Point(4, 5);
    assertEquals(p1, p2);
    assertEquals(p1.hashCode(), p2.hashCode());
    assertEquals("Point(4,5)", p1.toString());
  }
}
