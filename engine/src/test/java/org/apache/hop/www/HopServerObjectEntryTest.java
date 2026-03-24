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

package org.apache.hop.www;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class HopServerObjectEntryTest {

  @Test
  void equalsUsesIdOnly() {
    HopServerObjectEntry a = new HopServerObjectEntry("n1", "id-a");
    HopServerObjectEntry b = new HopServerObjectEntry("n2", "id-a");
    HopServerObjectEntry c = new HopServerObjectEntry("n1", "id-b");
    assertEquals(a, b);
    assertEquals(a, a);
    assertNotEquals(a, c);
    assertFalse(a.equals("x"));
  }

  @Test
  void hashCodeUsesId() {
    assertEquals(
        new HopServerObjectEntry("a", "same").hashCode(),
        new HopServerObjectEntry("b", "same").hashCode());
  }

  @Test
  void compareOrdersByNameThenId() {
    HopServerObjectEntry x = new HopServerObjectEntry("b", "2");
    HopServerObjectEntry y = new HopServerObjectEntry("a", "9");
    assertTrue(x.compareTo(y) > 0);
    HopServerObjectEntry z = new HopServerObjectEntry("b", "1");
    assertTrue(x.compareTo(z) > 0);
  }

  @Test
  void defaultConstructorAndSetters() {
    HopServerObjectEntry e = new HopServerObjectEntry();
    e.setName("nm");
    e.setId("i1");
    assertEquals("nm", e.getName());
    assertEquals("i1", e.getId());
  }
}
