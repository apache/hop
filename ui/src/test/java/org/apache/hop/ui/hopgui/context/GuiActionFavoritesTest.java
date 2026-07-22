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

package org.apache.hop.ui.hopgui.context;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.gui.plugin.action.GuiAction;
import org.apache.hop.core.gui.plugin.action.GuiActionType;
import org.apache.hop.ui.hopgui.context.GuiActionFavorites.Kind;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GuiActionFavoritesTest {

  private String savedTransforms;
  private String savedActions;

  @BeforeEach
  void setUp() {
    savedTransforms = HopConfig.getGuiProperty(GuiActionFavorites.CONFIG_KEY_TRANSFORMS);
    savedActions = HopConfig.getGuiProperty(GuiActionFavorites.CONFIG_KEY_WORKFLOW_ACTIONS);
    HopConfig.setGuiProperty(GuiActionFavorites.CONFIG_KEY_TRANSFORMS, "");
    HopConfig.setGuiProperty(GuiActionFavorites.CONFIG_KEY_WORKFLOW_ACTIONS, "");
  }

  @AfterEach
  void tearDown() {
    if (savedTransforms == null) {
      HopConfig.readGuiProperties().remove(GuiActionFavorites.CONFIG_KEY_TRANSFORMS);
    } else {
      HopConfig.setGuiProperty(GuiActionFavorites.CONFIG_KEY_TRANSFORMS, savedTransforms);
    }
    if (savedActions == null) {
      HopConfig.readGuiProperties().remove(GuiActionFavorites.CONFIG_KEY_WORKFLOW_ACTIONS);
    } else {
      HopConfig.setGuiProperty(GuiActionFavorites.CONFIG_KEY_WORKFLOW_ACTIONS, savedActions);
    }
  }

  @Test
  void parseAndSerializeIds() {
    assertTrue(GuiActionFavorites.parseIds(null).isEmpty());
    assertTrue(GuiActionFavorites.parseIds("").isEmpty());
    assertEquals(List.of("a", "b", "c"), GuiActionFavorites.parseIds("a, b,c"));
    assertEquals("a,b,c", GuiActionFavorites.serializeIds(Arrays.asList("a", "b", "c")));
    assertEquals("", GuiActionFavorites.serializeIds(List.of()));
  }

  @Test
  void toggleTransformFavorite() {
    assertFalse(GuiActionFavorites.isFavorite(Kind.TRANSFORM, "TextFileInput"));

    assertTrue(GuiActionFavorites.toggle(Kind.TRANSFORM, "TextFileInput"));
    assertTrue(GuiActionFavorites.isFavorite(Kind.TRANSFORM, "TextFileInput"));
    assertEquals(List.of("TextFileInput"), GuiActionFavorites.getFavoriteIds(Kind.TRANSFORM));

    assertFalse(GuiActionFavorites.toggle(Kind.TRANSFORM, "TextFileInput"));
    assertFalse(GuiActionFavorites.isFavorite(Kind.TRANSFORM, "TextFileInput"));
    assertTrue(GuiActionFavorites.getFavoriteIds(Kind.TRANSFORM).isEmpty());
  }

  @Test
  void transformAndActionListsAreIndependent() {
    GuiActionFavorites.toggle(Kind.TRANSFORM, "SelectValues");
    GuiActionFavorites.toggle(Kind.WORKFLOW_ACTION, "PIPELINE");

    assertTrue(GuiActionFavorites.isFavorite(Kind.TRANSFORM, "SelectValues"));
    assertFalse(GuiActionFavorites.isFavorite(Kind.TRANSFORM, "PIPELINE"));
    assertTrue(GuiActionFavorites.isFavorite(Kind.WORKFLOW_ACTION, "PIPELINE"));
    assertFalse(GuiActionFavorites.isFavorite(Kind.WORKFLOW_ACTION, "SelectValues"));
  }

  @Test
  void tryToggleFromActionId() {
    GuiAction create =
        new GuiAction(
            GuiActionFavorites.createId(Kind.TRANSFORM, "FilterRows"),
            GuiActionType.Create,
            "Filter rows",
            "desc",
            "image.svg",
            (s, c, t) -> {});
    assertTrue(GuiActionFavorites.tryToggleFromAction(create));
    assertTrue(GuiActionFavorites.isFavorite(Kind.TRANSFORM, "FilterRows"));

    GuiAction favorite =
        new GuiAction(
            GuiActionFavorites.favoriteId(Kind.TRANSFORM, "FilterRows"),
            GuiActionType.Create,
            "Filter rows",
            "desc",
            "image.svg",
            (s, c, t) -> {});
    assertTrue(GuiActionFavorites.tryToggleFromAction(favorite));
    assertFalse(GuiActionFavorites.isFavorite(Kind.TRANSFORM, "FilterRows"));

    GuiAction other =
        new GuiAction(
            "pipeline-graph-edit-properties",
            GuiActionType.Modify,
            "Edit",
            "d",
            "i",
            (s, c, t) -> {});
    assertFalse(GuiActionFavorites.tryToggleFromAction(other));
  }

  @Test
  void createFavoriteActionSetsCategoryAndOrder() {
    GuiAction base =
        new GuiAction(
            GuiActionFavorites.createId(Kind.WORKFLOW_ACTION, "START"),
            GuiActionType.Create,
            "Start",
            "desc",
            "image.svg",
            (s, c, t) -> {});
    base.setCategory("General");
    base.setCategoryOrder("9999_General");

    GuiAction favorite =
        GuiActionFavorites.createFavoriteAction(base, Kind.WORKFLOW_ACTION, "START");

    assertEquals(GuiActionFavorites.favoriteId(Kind.WORKFLOW_ACTION, "START"), favorite.getId());
    assertEquals(GuiActionFavorites.CATEGORY_ORDER, favorite.getCategoryOrder());
    assertEquals(base.getActionLambda(), favorite.getActionLambda());
    assertEquals("Start", favorite.getName());
  }

  @Test
  void multiFavoriteOrderPreserved() {
    GuiActionFavorites.toggle(Kind.TRANSFORM, "A");
    GuiActionFavorites.toggle(Kind.TRANSFORM, "B");
    GuiActionFavorites.toggle(Kind.TRANSFORM, "C");
    assertEquals(List.of("A", "B", "C"), GuiActionFavorites.getFavoriteIds(Kind.TRANSFORM));

    GuiActionFavorites.toggle(Kind.TRANSFORM, "B");
    assertEquals(List.of("A", "C"), GuiActionFavorites.getFavoriteIds(Kind.TRANSFORM));
  }
}
