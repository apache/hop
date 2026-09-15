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

import org.apache.hop.core.Const;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.ui.core.bus.HopGuiEvents;
import org.apache.hop.ui.hopgui.HopGui;

/**
 * Visibility and sash layout for the Spoon-style transform/action palette tree (issue #7114).
 * Hidden by default; the choice is stored in hop-config.json so it applies to new pipeline and
 * workflow tabs.
 */
public final class GraphPalette {

  public static final String CONFIG_KEY = "ShowGraphPaletteTree";
  public static final String SASH_AUDIT_KEY = "graph-palette-tree-width";
  public static final int[] DEFAULT_SASH_WEIGHTS = {20, 80};

  /** Horizontal offset when Shift-double-click hops from an existing transform or action. */
  public static final int CHAIN_OFFSET_X = 150;

  private GraphPalette() {
    // utility
  }

  /** False when the property is missing or not {@code Y} — the palette stays hidden by default. */
  public static boolean isVisible() {
    return "Y".equalsIgnoreCase(Const.NVL(HopConfig.getGuiProperty(CONFIG_KEY), "N"));
  }

  public static void setVisible(boolean visible) {
    HopConfig.setGuiProperty(CONFIG_KEY, visible ? "Y" : "N");
    try {
      HopConfig.getInstance().saveToFile();
    } catch (Exception e) {
      LogChannel.UI.logError("Error saving palette tree visibility", e);
    }
  }

  public static void fireFavoritesChanged(HopGui hopGui) {
    fireEvent(hopGui, HopGuiEvents.FavoritesChanged);
  }

  public static void fireVisibilityChanged(HopGui hopGui) {
    fireEvent(hopGui, HopGuiEvents.PaletteTreeVisibilityChanged);
  }

  private static void fireEvent(HopGui hopGui, HopGuiEvents event) {
    if (hopGui == null) {
      return;
    }
    try {
      hopGui.getEventsHandler().fire(null, true, event.name());
    } catch (HopException e) {
      LogChannel.UI.logError("Error firing " + event.name(), e);
    }
  }
}
