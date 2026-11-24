/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.workflow.action;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.gui.Point;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.workflow.actions.missing.MissingAction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class ActionMetaLocationProvenanceTest {

  @Test
  void constructorsAndClearDoNotSupplyAnExplicitLocation() {
    for (final var meta :
        new ActionMeta[] {new ActionMeta(), new ActionMeta(new MissingAction("test", "missing"))}) {
      assertEquals(new Point(0, 0), meta.getLocation());
      assertFalse(meta.isLocationExplicit());
      meta.setLocation(10, 20);
      meta.clear();
      assertEquals(new Point(0, 0), meta.getLocation());
      assertFalse(meta.isLocationExplicit());
    }
  }

  @ParameterizedTest
  @CsvSource({
    "0, 0, 0, 0, true",
    "120, 240, 120, 240, true",
    "-10, -20, 0, 0, true",
    ", 240, 0, 240, false",
    "120, , 120, 0, false",
    ", , 0, 0, false",
    "'', 240, 0, 240, false",
    "120, '', 120, 0, false",
    "invalid, 240, 0, 240, false",
    "120, invalid, 120, 0, false",
    "2147483648, 240, 0, 240, false"
  })
  void xmlCoordinatesAndClonesRetainProvenance(
      final String x,
      final String y,
      final int expectedX,
      final int expectedY,
      final boolean explicit)
      throws Exception {
    final var document =
        XmlHandler.loadXmlString(
            "<action><name>test</name><type>LocationProvenanceMissingAction</type>"
                + coordinate("xloc", x)
                + coordinate("yloc", y)
                + "</action>");
    final var meta =
        new ActionMeta(XmlHandler.getSubNode(document, "action"), null, new Variables());
    assertEquals(new Point(expectedX, expectedY), meta.getLocation());
    assertEquals(explicit, meta.isLocationExplicit());
    final var clone = meta.clone();
    assertEquals(meta.getLocation(), clone.getLocation());
    assertEquals(explicit, clone.isLocationExplicit());
    assertEquals(explicit, ((ActionMeta) meta.cloneDeep()).isLocationExplicit());
    final var replacement = new ActionMeta();
    replacement.setLocation(10, 20);
    replacement.replaceMeta(meta);
    assertEquals(explicit, replacement.isLocationExplicit());
  }

  @Test
  void suppliedLocationsAreExplicitEvenAtTheDefaultPosition() {
    final var meta = new ActionMeta(new MissingAction("test", "missing"));
    assertFalse(meta.clone().isLocationExplicit());
    meta.setLocation(0, 0);
    assertTrue(meta.isLocationExplicit());
    meta.setLocation((Point) null);
    assertFalse(meta.isLocationExplicit());
    meta.setLocation(new Point(0, 0));
    assertTrue(meta.isLocationExplicit());
    assertTrue(meta.clone().isLocationExplicit());
    meta.setLocation(-10, 20);
    assertEquals(new Point(0, 20), meta.getLocation());
    assertTrue(meta.isLocationExplicit());
    meta.setLocation(new Point(-10, 20));
    assertEquals(new Point(-10, 20), meta.getLocation());
    assertTrue(meta.isLocationExplicit());
  }

  private static String coordinate(final String tag, final String value) {
    return value == null ? "" : "<" + tag + ">" + value + "</" + tag + ">";
  }
}
