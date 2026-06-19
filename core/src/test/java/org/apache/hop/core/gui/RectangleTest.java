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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class RectangleTest {

  @Test
  void copyConstructorCopiesFields() {
    Rectangle a = new Rectangle(1, 2, 3, 4);
    Rectangle b = new Rectangle(a);
    assertEquals(a.x, b.x);
    assertEquals(a.y, b.y);
    assertEquals(a.width, b.width);
    assertEquals(a.height, b.height);
  }

  @Test
  void containsPointInsideAndEdges() {
    Rectangle r = new Rectangle(10, 20, 5, 6);
    assertTrue(r.contains(10, 20));
    assertTrue(r.contains(15, 26));
    assertTrue(r.contains(new Point(12, 22)));
    assertFalse(r.contains(9, 25));
    assertFalse(r.contains(16, 25));
  }

  @Test
  void centreAndDistance() {
    Rectangle r1 = new Rectangle(0, 0, 10, 10);
    Rectangle r2 = new Rectangle(10, 0, 10, 10);
    assertEquals(5, r1.getCentreX());
    assertEquals(5, r1.getCentreY());
    assertEquals(15, r2.getCentreX());
    assertEquals(5, r2.getCentreY());
    assertEquals(10.0, r1.distance(r2), 1e-9);
  }

  @Test
  void isEmptyAndToString() {
    assertTrue(new Rectangle(0, 0, 0, 0).isEmpty());
    assertFalse(new Rectangle(0, 0, 1, 0).isEmpty());
    assertEquals("Rectangle(2,3-4x5)", new Rectangle(2, 3, 4, 5).toString());
  }
}
