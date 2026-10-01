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

package org.apache.hop.ui.hopgui.palette;

import org.apache.hop.core.gui.Point;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.context.GuiActionFavorites;

/** Pipeline or workflow graph that hosts a {@link GraphPaletteTree}. */
public interface IGraphPaletteHost {

  HopGui getHopGui();

  /** Stable id for event-listener registration (typically the graph UUID). */
  String getPaletteHostId();

  GuiActionFavorites.Kind getPaletteKind();

  /**
   * Place a create-transform / create-action at {@code graphLocation}.
   *
   * @param graphLocation canvas graph coordinates, or {@code null} to place in a row after the
   *     chain source (Shift-double-click)
   * @param chainHop when true, also hop from the selected item, or the last chained item
   */
  boolean placePaletteAction(String actionId, Point graphLocation, boolean chainHop);

  /** Last canvas click in graph coordinates, or the visible canvas center. */
  Point getPaletteDropLocation();

  /** Show or hide the palette sash from the persisted {@link GraphPalette#isVisible()} flag. */
  void applyPaletteVisibility();

  /** Persist a favorite toggle and notify other open palettes. */
  void persistFavoritesChange();
}
