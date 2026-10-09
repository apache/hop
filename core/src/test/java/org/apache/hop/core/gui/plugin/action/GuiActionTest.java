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

package org.apache.hop.core.gui.plugin.action;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.search.SearchMatcher;
import org.junit.jupiter.api.Test;

class GuiActionTest {

  private static GuiAction selectValues() {
    GuiAction action =
        new GuiAction(
            "create-transform-SelectValues",
            GuiActionType.Create,
            "Select values",
            "Select or remove fields in a row.",
            null,
            (shift, control, t) -> {});
    action
        .getTooltipHints()
        .add("\nAlt-Click to add to favorites. Click-drag to select and position.");
    return action;
  }

  private static GuiAction dummy() {
    GuiAction action =
        new GuiAction(
            "create-transform-Dummy",
            GuiActionType.Create,
            "Dummy (do nothing)",
            "This transform type doesn't do anything.",
            null,
            (shift, control, t) -> {});
    action
        .getTooltipHints()
        .add("\nAlt-Click to add to favorites. Click-drag to select and position.");
    return action;
  }

  /** Issue #8756: the hint's words used to match every transform in the context dialog. */
  @Test
  void tooltipHintsAreNotSearched() {
    for (String query : new String[] {"select", "position", "favorites", "drag"}) {
      assertEquals(0.0, dummy().matchScore(new SearchMatcher(query, false, false, true)), query);
    }
    assertTrue(selectValues().matchScore(new SearchMatcher("select", false, false, true)) > 0.0);
  }

  @Test
  void displayTooltipShowsHintsOnTheirOwnLines() {
    GuiAction action = selectValues();
    action.getTooltipHints().add("Can start without incoming hops (pipeline source).");
    action.getTooltipHints().add("  ");
    assertEquals(
        "Select or remove fields in a row.\n"
            + "Alt-Click to add to favorites. Click-drag to select and position.\n"
            + "Can start without incoming hops (pipeline source).",
        action.getDisplayTooltip());
  }

  @Test
  void displayTooltipWithoutDescriptionIsJustTheHints() {
    GuiAction action = selectValues();
    action.setTooltip(null);
    assertEquals(
        "Alt-Click to add to favorites. Click-drag to select and position.",
        action.getDisplayTooltip());
  }

  @Test
  void copyKeepsHintsIndependently() {
    GuiAction original = selectValues();
    GuiAction copy = new GuiAction(original);
    assertEquals(original.getTooltipHints(), copy.getTooltipHints());
    copy.getTooltipHints().clear();
    assertEquals(1, original.getTooltipHints().size());
  }
}
