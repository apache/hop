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

package org.apache.hop.ui.hopgui.file.shared;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import org.apache.hop.core.gui.AreaOwner.AreaType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class CanvasToolTipTest {

  /** Every area the canvas registers has an option that switches its tooltip off. */
  @ParameterizedTest
  @EnumSource(AreaType.class)
  void everyAreaTypeHasAnOption(AreaType areaType) {
    assertFalse(CanvasToolTip.forAreaType(areaType).isEmpty(), areaType + " has no option");
  }

  /** Every option is reachable from the canvas: the notice is the only one without an area. */
  @Test
  void everyOptionExceptTheNoticeCoversAnArea() {
    Set<CanvasToolTip> covered = new HashSet<>();
    for (AreaType areaType : AreaType.values()) {
      covered.addAll(CanvasToolTip.forAreaType(areaType));
    }
    assertEquals(EnumSet.complementOf(EnumSet.of(CanvasToolTip.NOTICE)), covered);
  }

  @Test
  void namesCarryTheEditHint() {
    assertEquals(
        EnumSet.of(CanvasToolTip.EDIT_HINT), CanvasToolTip.forAreaType(AreaType.TRANSFORM_NAME));
    assertEquals(
        EnumSet.of(CanvasToolTip.EDIT_HINT), CanvasToolTip.forAreaType(AreaType.ACTION_NAME));
  }

  /** An icon shows either the deprecation warning or the description, so both options apply. */
  @Test
  void iconsCarryDescriptionAndDeprecation() {
    for (AreaType icon :
        EnumSet.of(
            AreaType.TRANSFORM_ICON,
            AreaType.TRANSFORM_INFO_ICON,
            AreaType.ACTION_ICON,
            AreaType.ACTION_INFO_ICON)) {
      assertEquals(
          EnumSet.of(CanvasToolTip.DESCRIPTION, CanvasToolTip.DEPRECATION),
          CanvasToolTip.forAreaType(icon),
          icon.name());
    }
  }

  @Test
  void unknownAreasGoToPlugins() {
    assertEquals(EnumSet.of(CanvasToolTip.PLUGIN), CanvasToolTip.forAreaType(AreaType.CUSTOM));
    assertEquals(EnumSet.of(CanvasToolTip.PLUGIN), CanvasToolTip.forAreaType(AreaType.ACTION_BUSY));
    assertTrue(CanvasToolTip.forAreaType(null).isEmpty());
  }

  @Test
  void codesAreUniqueAndStable() {
    Set<String> codes = new HashSet<>();
    for (CanvasToolTip toolTip : CanvasToolTip.values()) {
      assertTrue(codes.add(toolTip.getCode()), "duplicate code " + toolTip.getCode());
      assertTrue(toolTip.getLabelKey().endsWith(toolTip.getCode() + ".Label"));
    }
  }
}
