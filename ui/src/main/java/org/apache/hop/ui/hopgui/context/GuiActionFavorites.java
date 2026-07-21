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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.gui.plugin.action.GuiAction;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.dialog.ContextDialog;

/**
 * Manages user favorites for transforms (pipeline canvas) and workflow actions. Favorites are
 * stored in hop-config.json under guiProperties and shown as a "Favorites" category in the context
 * action dialog, immediately below the Basic category.
 */
public final class GuiActionFavorites {

  private static final Class<?> PKG = ContextDialog.class; // i18n

  public static final String CONFIG_KEY_TRANSFORMS = "FavoriteTransforms";
  public static final String CONFIG_KEY_WORKFLOW_ACTIONS = "FavoriteWorkflowActions";

  /** Category order: after Basic ("1"), before other contextual categories ("2", …). */
  public static final String CATEGORY_ORDER = "1.5";

  public static final String ID_PREFIX_TRANSFORM = "pipeline-graph-create-transform-";
  public static final String ID_PREFIX_TRANSFORM_FAVORITE =
      "pipeline-graph-create-transform-favorite-";
  public static final String ID_PREFIX_ACTION = "workflow-graph-create-workflow-action-";
  public static final String ID_PREFIX_ACTION_FAVORITE =
      "workflow-graph-create-workflow-action-favorite-";

  public enum Kind {
    TRANSFORM(CONFIG_KEY_TRANSFORMS),
    WORKFLOW_ACTION(CONFIG_KEY_WORKFLOW_ACTIONS);

    private final String configKey;

    Kind(String configKey) {
      this.configKey = configKey;
    }

    public String getConfigKey() {
      return configKey;
    }
  }

  private GuiActionFavorites() {
    // utility
  }

  public static String getFavoritesCategoryName() {
    return BaseMessages.getString(PKG, "ContextDialog.Category.Favorites");
  }

  /**
   * Append the ALT-Click hint to a plugin description for use as a GuiAction tooltip.
   *
   * @param description the plugin description (may be null)
   * @param favorite true if the plugin is already a favorite (show remove hint)
   * @return description plus newline and hint
   */
  public static String tooltipWithFavoriteHint(String description, boolean favorite) {
    String hint =
        favorite
            ? BaseMessages.getString(PKG, "ContextDialog.Favorite.RemoveHint")
            : BaseMessages.getString(PKG, "ContextDialog.Favorite.AddHint");
    return Const.NVL(description, "") + hint;
  }

  public static List<String> getFavoriteIds(Kind kind) {
    return parseIds(HopConfig.getGuiProperty(kind.getConfigKey()));
  }

  public static boolean isFavorite(Kind kind, String pluginId) {
    if (StringUtils.isEmpty(pluginId)) {
      return false;
    }
    return getFavoriteIds(kind).contains(pluginId);
  }

  /**
   * Toggle the given plugin ID in the favorites list and persist to hop config.
   *
   * @return true if the plugin is a favorite after the toggle, false if removed
   */
  public static boolean toggle(Kind kind, String pluginId) {
    if (StringUtils.isEmpty(pluginId)) {
      return false;
    }
    Set<String> ids = new LinkedHashSet<>(getFavoriteIds(kind));
    boolean nowFavorite;
    if (ids.contains(pluginId)) {
      ids.remove(pluginId);
      nowFavorite = false;
    } else {
      ids.add(pluginId);
      nowFavorite = true;
    }
    saveIds(kind, new ArrayList<>(ids));
    return nowFavorite;
  }

  /**
   * If the action id refers to a create-transform or create-action entry, toggle its favorite
   * status.
   *
   * @param action the selected gui action
   * @return true if a favorite was toggled (caller should refresh the dialog)
   */
  public static boolean tryToggleFromAction(GuiAction action) {
    if (action == null || StringUtils.isEmpty(action.getId())) {
      return false;
    }
    String id = action.getId();
    KindAndPluginId resolved = resolve(id);
    if (resolved == null) {
      return false;
    }
    toggle(resolved.kind, resolved.pluginId);
    return true;
  }

  /**
   * Create a Favorites-category copy of a create action for the given plugin id.
   *
   * @param base the original create action
   * @param kind transform or workflow action
   * @param pluginId plugin id
   * @return a new GuiAction with Favorites category and order
   */
  public static GuiAction createFavoriteAction(GuiAction base, Kind kind, String pluginId) {
    GuiAction favorite = new GuiAction(base);
    favorite.setId(favoriteId(kind, pluginId));
    favorite.setCategory(getFavoritesCategoryName());
    favorite.setCategoryOrder(CATEGORY_ORDER);
    return favorite;
  }

  public static String favoriteId(Kind kind, String pluginId) {
    return switch (kind) {
      case TRANSFORM -> ID_PREFIX_TRANSFORM_FAVORITE + pluginId;
      case WORKFLOW_ACTION -> ID_PREFIX_ACTION_FAVORITE + pluginId;
    };
  }

  public static String createId(Kind kind, String pluginId) {
    return switch (kind) {
      case TRANSFORM -> ID_PREFIX_TRANSFORM + pluginId;
      case WORKFLOW_ACTION -> ID_PREFIX_ACTION + pluginId;
    };
  }

  static List<String> parseIds(String csv) {
    List<String> result = new ArrayList<>();
    if (StringUtils.isEmpty(csv)) {
      return result;
    }
    Arrays.stream(csv.split(","))
        .map(String::trim)
        .filter(StringUtils::isNotEmpty)
        .forEach(result::add);
    return result;
  }

  static String serializeIds(List<String> ids) {
    if (ids == null || ids.isEmpty()) {
      return "";
    }
    return String.join(",", ids);
  }

  private static void saveIds(Kind kind, List<String> ids) {
    // Persist the in-memory guiProperties value; the context dialog saves hop-config.json after a
    // successful toggle so unit tests can exercise this without writing the config file.
    HopConfig.setGuiProperty(kind.getConfigKey(), serializeIds(ids));
  }

  private static KindAndPluginId resolve(String actionId) {
    if (actionId.startsWith(ID_PREFIX_TRANSFORM_FAVORITE)) {
      return new KindAndPluginId(
          Kind.TRANSFORM, actionId.substring(ID_PREFIX_TRANSFORM_FAVORITE.length()));
    }
    if (actionId.startsWith(ID_PREFIX_TRANSFORM)) {
      return new KindAndPluginId(Kind.TRANSFORM, actionId.substring(ID_PREFIX_TRANSFORM.length()));
    }
    if (actionId.startsWith(ID_PREFIX_ACTION_FAVORITE)) {
      return new KindAndPluginId(
          Kind.WORKFLOW_ACTION, actionId.substring(ID_PREFIX_ACTION_FAVORITE.length()));
    }
    if (actionId.startsWith(ID_PREFIX_ACTION)) {
      return new KindAndPluginId(
          Kind.WORKFLOW_ACTION, actionId.substring(ID_PREFIX_ACTION.length()));
    }
    return null;
  }

  private record KindAndPluginId(Kind kind, String pluginId) {}
}
