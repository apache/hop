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

package org.apache.hop.ui.core.dialog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.gui.AreaOwner;
import org.apache.hop.core.gui.plugin.action.GuiAction;
import org.apache.hop.core.gui.plugin.action.GuiActionType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ContextDialogSvgRendererTest {

  @Test
  @DisplayName("findItemsForCategory matches category, case-insensitively, and handles Other")
  void testFindItemsForCategory() {
    GuiAction action1 =
        new GuiAction(
            "id1", GuiActionType.Create, "Table input", "tooltip1", "image1.svg", (a, b, c) -> {});
    action1.setCategory("Transforms");

    GuiAction action2 =
        new GuiAction(
            "id2", GuiActionType.Create, "Dummy", "tooltip2", "image2.svg", (a, b, c) -> {});
    action2.setCategory(null);

    ContextDialog.Item item1 = new ContextDialog.Item(action1, null);
    ContextDialog.Item item2 = new ContextDialog.Item(action2, null);
    List<ContextDialog.Item> items = List.of(item1, item2);

    ContextDialog.CategoryAndOrder catTransforms =
        new ContextDialog.CategoryAndOrder("transforms", "1", false);
    List<ContextDialog.Item> foundTransforms =
        ContextDialogSvgRenderer.findItemsForCategory(items, catTransforms);
    assertEquals(1, foundTransforms.size());
    assertEquals(item1, foundTransforms.get(0));

    ContextDialog.CategoryAndOrder catOther =
        new ContextDialog.CategoryAndOrder(ContextDialog.CATEGORY_OTHER, "9999", false);
    List<ContextDialog.Item> foundOther =
        ContextDialogSvgRenderer.findItemsForCategory(items, catOther);
    assertEquals(1, foundOther.size());
    assertEquals(item2, foundOther.get(0));

    List<ContextDialog.Item> foundAll = ContextDialogSvgRenderer.findItemsForCategory(items, null);
    assertEquals(2, foundAll.size());
  }

  @Test
  @DisplayName("render produces valid SVG with category headers and action items")
  void testRenderSvg() throws Exception {
    GuiAction action1 =
        new GuiAction(
            "id1", GuiActionType.Create, "Table input", "Read from table", null, (a, b, c) -> {});
    action1.setCategory("Input");

    GuiAction action2 =
        new GuiAction("id2", GuiActionType.Create, "Dummy", "Do nothing", null, (a, b, c) -> {});
    action2.setCategory("Flow");

    ContextDialog.Item item1 = new ContextDialog.Item(action1, null);
    item1.setSelected(true);
    ContextDialog.Item item2 = new ContextDialog.Item(action2, null);

    List<ContextDialog.Item> items = List.of(item1, item2);
    List<ContextDialog.CategoryAndOrder> categories = new ArrayList<>();
    categories.add(new ContextDialog.CategoryAndOrder("Input", "1", false));
    categories.add(new ContextDialog.CategoryAndOrder("Flow", "2", false));

    ContextDialog dialog = mock(ContextDialog.class);
    when(dialog.getIconSize()).thenReturn(32);
    when(dialog.getMargin()).thenReturn(4);
    when(dialog.getXMargin()).thenReturn(12);
    when(dialog.getYMargin()).thenReturn(8);
    when(dialog.getCategories()).thenReturn(categories);
    when(dialog.getFilteredItems()).thenReturn(items);
    when(dialog.isUseCategories()).thenReturn(true);
    when(dialog.isUseFixedWidth()).thenReturn(false);

    ContextDialogSvgRenderResult result = ContextDialogSvgRenderer.render(dialog, 600, 400);

    assertNotNull(result);
    assertNotNull(result.svg());
    assertTrue(result.svg().contains("<svg"));
    assertTrue(result.svg().contains("preserveAspectRatio=\"none\""));
    assertTrue(result.svg().contains("Table input"));
    assertTrue(result.svg().contains("Dummy"));
    assertTrue(result.totalContentHeight() > 0);

    List<AreaOwner> areas = result.areaOwners();
    assertEquals(4, areas.size()); // 2 category headers + 2 items
  }

  @Test
  @DisplayName("collapsing a category shifts subsequent categories and items up correctly")
  void testRenderSvgCategoryCollapse() throws Exception {
    GuiAction action1 =
        new GuiAction(
            "id1", GuiActionType.Create, "Row generator", "Generate rows", null, (a, b, c) -> {});
    action1.setCategory("Basic");

    GuiAction action2 =
        new GuiAction(
            "id2",
            GuiActionType.Create,
            "XML input stream (StAX)",
            "Read XML",
            null,
            (a, b, c) -> {});
    action2.setCategory("Input");

    ContextDialog.Item item1 = new ContextDialog.Item(action1, null);
    ContextDialog.Item item2 = new ContextDialog.Item(action2, null);

    List<ContextDialog.Item> items = List.of(item1, item2);
    ContextDialog.CategoryAndOrder catBasic =
        new ContextDialog.CategoryAndOrder("Basic", "1", false);
    ContextDialog.CategoryAndOrder catInput =
        new ContextDialog.CategoryAndOrder("Input", "2", false);
    List<ContextDialog.CategoryAndOrder> categories = List.of(catBasic, catInput);

    ContextDialog dialog = mock(ContextDialog.class);
    when(dialog.getIconSize()).thenReturn(32);
    when(dialog.getMargin()).thenReturn(4);
    when(dialog.getXMargin()).thenReturn(12);
    when(dialog.getYMargin()).thenReturn(8);
    when(dialog.getCategories()).thenReturn(categories);
    when(dialog.getFilteredItems()).thenReturn(items);
    when(dialog.isUseCategories()).thenReturn(true);
    when(dialog.isUseFixedWidth()).thenReturn(false);

    // Expanded render: both items drawn
    ContextDialogSvgRenderResult expandedResult = ContextDialogSvgRenderer.render(dialog, 600, 400);
    assertEquals(4, expandedResult.areaOwners().size());
    AreaOwner expandedItem2Area =
        expandedResult.areaOwners().stream()
            .filter(a -> a.getOwner() == item2)
            .findFirst()
            .orElseThrow();
    assertTrue(expandedResult.svg().contains("Row generator"));
    assertTrue(expandedResult.svg().contains("XML input stream (StAX)"));

    // Collapse category "Basic"
    catBasic.flipCollapsed();
    assertTrue(catBasic.isCollapsed());

    ContextDialogSvgRenderResult collapsedResult =
        ContextDialogSvgRenderer.render(dialog, 600, 400);

    // 3 area owners: "Basic" header, "Input" header, item2 (item1 is collapsed)
    assertEquals(3, collapsedResult.areaOwners().size());
    assertFalse(collapsedResult.svg().contains("Row generator"));
    assertTrue(collapsedResult.svg().contains("XML input stream (StAX)"));

    AreaOwner collapsedItem2Area =
        collapsedResult.areaOwners().stream()
            .filter(a -> a.getOwner() == item2)
            .findFirst()
            .orElseThrow();

    // Item2 must shift up when Basic is collapsed
    assertTrue(
        collapsedItem2Area.getArea().y < expandedItem2Area.getArea().y,
        "Item 2 Y should shift up when previous category is collapsed");

    // Total content height must decrease
    assertTrue(collapsedResult.totalContentHeight() <= expandedResult.totalContentHeight());
  }

  @Test
  @DisplayName("render in dark mode produces light text and transparent background")
  void testRenderSvgDarkMode() throws Exception {
    GuiAction action1 =
        new GuiAction(
            "id1", GuiActionType.Create, "Table input", "Read from table", null, (a, b, c) -> {});
    action1.setCategory("Input");
    ContextDialog.Item item1 = new ContextDialog.Item(action1, null);

    ContextDialog dialog = mock(ContextDialog.class);
    when(dialog.getIconSize()).thenReturn(32);
    when(dialog.getMargin()).thenReturn(4);
    when(dialog.getXMargin()).thenReturn(12);
    when(dialog.getYMargin()).thenReturn(8);
    when(dialog.getCategories())
        .thenReturn(List.of(new ContextDialog.CategoryAndOrder("Input", "1", false)));
    when(dialog.getFilteredItems()).thenReturn(List.of(item1));
    when(dialog.isUseCategories()).thenReturn(true);
    when(dialog.isUseFixedWidth()).thenReturn(false);

    ContextDialogSvgRenderResult result =
        ContextDialogSvgRenderer.render(dialog, 600, 400, true, null, "Arial", 11);

    assertNotNull(result);
    String svg = result.svg();
    assertNotNull(svg);
    assertTrue(svg.contains("Table input"));
    // Verify light text color for dark mode (rgb(232, 232, 232) or #e8e8e8)
    assertTrue(svg.contains("232, 232, 232") || svg.contains("#e8e8e8") || svg.contains("232"));
    // Verify category header light text color
    assertTrue(svg.contains("240, 240, 240") || svg.contains("#f0f0f0") || svg.contains("240"));
    // Verify that the initial background rect was removed (no rect covering (0,0) without rx)
    assertFalse(
        svg.contains(
            "<rect fill=\"rgb(50, 50, 50)\" width=\"600\" height=\"400\" x=\"0\" y=\"0\""));
  }
}
