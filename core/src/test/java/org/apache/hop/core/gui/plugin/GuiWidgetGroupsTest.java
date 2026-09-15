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

package org.apache.hop.core.gui.plugin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class GuiWidgetGroupsTest {

  @Test
  void noGroupsWhenNothingIsLabeled() {
    GuiElements a = element("a", "", GuiWidgetGroupType.NONE);
    GuiElements b = element("b", "", GuiWidgetGroupType.NONE);
    assertFalse(GuiWidgetGroups.hasGroups(List.of(a, b)));
  }

  @Test
  void partitionsByGroupAndKeepsOrder() {
    GuiElements second = element("second", "Two", "20", GuiWidgetGroupType.TABS);
    GuiElements first = element("first", "One", "10", GuiWidgetGroupType.TABS);
    assertTrue(GuiWidgetGroups.hasGroups(List.of(second, first)));

    List<GuiWidgetGroups.Bucket> buckets = GuiWidgetGroups.from(List.of(second, first), "General");
    assertEquals(2, buckets.size());
    assertEquals("One", buckets.get(0).getLabel());
    assertEquals("first", buckets.get(0).getElements().get(0).getId());
    assertEquals("Two", buckets.get(1).getLabel());
    assertEquals(GuiWidgetGroupType.TABS, GuiWidgetGroups.typeOf(List.of(second, first)));
    assertFalse(GuiWidgetGroups.hasMixedTypes(List.of(second, first)));
  }

  @Test
  void ungroupedSiblingsLandInGeneralWhenAnyGroupExists() {
    GuiElements grouped = element("g", "Tab", "10", GuiWidgetGroupType.TABS);
    GuiElements loose = element("l", "", GuiWidgetGroupType.TABS);
    List<GuiWidgetGroups.Bucket> buckets = GuiWidgetGroups.from(List.of(grouped, loose), "General");
    assertEquals(2, buckets.size());
    assertEquals("General", buckets.get(0).getLabel());
    assertEquals("l", buckets.get(0).getElements().get(0).getId());
    assertEquals("Tab", buckets.get(1).getLabel());
  }

  @Test
  void mixedTypesFallBackToTabs() {
    GuiElements tabs = element("a", "A", "10", GuiWidgetGroupType.TABS);
    GuiElements list = element("b", "B", "20", GuiWidgetGroupType.LIST);
    assertTrue(GuiWidgetGroups.hasMixedTypes(List.of(tabs, list)));
    assertEquals(GuiWidgetGroupType.TABS, GuiWidgetGroups.typeOf(List.of(tabs, list)));
  }

  private static GuiElements element(String id, String group, GuiWidgetGroupType type) {
    return element(id, group, "", type);
  }

  private static GuiElements element(
      String id, String group, String groupOrder, GuiWidgetGroupType type) {
    GuiElements element = new GuiElements();
    element.setId(id);
    element.setOrder(id);
    element.setGroup(group);
    element.setGroupOrder(groupOrder);
    element.setGroupType(type);
    return element;
  }
}
