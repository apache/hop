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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.plugins.ActionPluginType;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.search.SearchMatcher;
import org.apache.hop.ui.core.dialog.ContextDialog;
import org.apache.hop.ui.hopgui.PaletteEngineFilter;
import org.apache.hop.ui.hopgui.context.GuiActionFavorites;
import org.apache.hop.ui.hopgui.context.GuiActionFavorites.Kind;

/**
 * Categorized list of pipeline transforms or workflow actions for the Spoon-style palette tree.
 * Favorites (when any exist) are always the first category.
 */
@Getter
public final class GraphPaletteModel {

  public record Item(
      String pluginId,
      String actionId,
      String name,
      String category,
      String description,
      String imageFile,
      List<String> keywords) {

    public Item {
      keywords = keywords == null ? List.of() : List.copyOf(keywords);
      name = Const.NVL(name, pluginId);
      category = Const.NVL(category, ContextDialog.CATEGORY_OTHER);
      description = Const.NVL(description, "");
      imageFile = Const.NVL(imageFile, "");
    }

    /**
     * Same field weighting as {@link org.apache.hop.core.gui.plugin.action.GuiAction#matchScore}.
     */
    public double matchScore(SearchMatcher matcher) {
      if (matcher == null) {
        return 1.0;
      }
      double best = matcher.score(name);
      best = Math.max(best, 0.9 * matcher.score(description));
      if (keywords != null) {
        for (String keyword : keywords) {
          best = Math.max(best, 0.8 * matcher.score(keyword));
        }
      }
      return Math.max(best, 0.7 * matcher.score(category));
    }
  }

  public record Category(String name, List<Item> items) {
    public Category {
      items = items == null ? List.of() : List.copyOf(items);
      name = Const.NVL(name, ContextDialog.CATEGORY_OTHER);
    }
  }

  private final List<Category> categories;

  public GraphPaletteModel(List<Category> categories) {
    this.categories = categories == null ? List.of() : List.copyOf(categories);
  }

  public static GraphPaletteModel fromPlugins(Kind kind) {
    PaletteEngineFilter filter =
        kind == Kind.TRANSFORM
            ? PaletteEngineFilter.forPipelineDesign()
            : PaletteEngineFilter.forWorkflowDesign();
    PluginRegistry registry = PluginRegistry.getInstance();
    List<IPlugin> plugins =
        kind == Kind.TRANSFORM
            ? registry.getPlugins(TransformPluginType.class)
            : registry.getPlugins(ActionPluginType.class);
    List<Item> items = new ArrayList<>();
    for (IPlugin plugin : plugins) {
      if (!filter.isPluginAllowed(plugin)
          || plugin.getIds() == null
          || plugin.getIds().length == 0) {
        continue;
      }
      String pluginId = plugin.getIds()[0];
      List<String> keywords = new ArrayList<>();
      if (plugin.getKeywords() != null) {
        keywords.addAll(Arrays.asList(plugin.getKeywords()));
      }
      if (plugin.getEnglishKeywords() != null) {
        keywords.addAll(Arrays.asList(plugin.getEnglishKeywords()));
      }
      if (StringUtils.isNotEmpty(plugin.getCategory())) {
        keywords.add(plugin.getCategory());
      }
      items.add(
          new Item(
              pluginId,
              GuiActionFavorites.createId(kind, pluginId),
              plugin.getName(),
              plugin.getCategory(),
              plugin.getDescription(),
              plugin.getImageFile(),
              keywords));
    }
    return fromItems(items, GuiActionFavorites.getFavoriteIds(kind), favoritesCategoryName());
  }

  public static String favoritesCategoryName() {
    return GuiActionFavorites.getFavoritesCategoryName();
  }

  /**
   * Group {@code items} into categories. Favorites (ids that still exist in {@code items}) come
   * first, in the stored favorite order. Remaining categories are alphabetical; items within a
   * category are alphabetical by name.
   */
  public static GraphPaletteModel fromItems(
      List<Item> items, List<String> favoriteIds, String favoritesCategoryName) {
    Map<String, Item> byPluginId = new LinkedHashMap<>();
    Map<String, List<Item>> byCategory = new LinkedHashMap<>();
    if (items != null) {
      for (Item item : items) {
        if (item == null || StringUtils.isEmpty(item.pluginId())) {
          continue;
        }
        byPluginId.put(item.pluginId(), item);
        byCategory.computeIfAbsent(item.category(), k -> new ArrayList<>()).add(item);
      }
    }

    List<Category> result = new ArrayList<>();
    if (favoriteIds != null && !favoriteIds.isEmpty()) {
      List<Item> favorites = new ArrayList<>();
      for (String favoriteId : favoriteIds) {
        Item item = byPluginId.get(favoriteId);
        if (item != null) {
          favorites.add(item);
        }
      }
      if (!favorites.isEmpty()) {
        result.add(new Category(Const.NVL(favoritesCategoryName, "Favorites"), favorites));
      }
    }

    List<String> categoryNames = new ArrayList<>(byCategory.keySet());
    categoryNames.sort(String.CASE_INSENSITIVE_ORDER);
    for (String categoryName : categoryNames) {
      List<Item> categoryItems = new ArrayList<>(byCategory.get(categoryName));
      categoryItems.sort(Comparator.comparing(Item::name, String.CASE_INSENSITIVE_ORDER));
      result.add(new Category(categoryName, categoryItems));
    }
    return new GraphPaletteModel(result);
  }

  /**
   * Filter categories/items with the same matcher as the context dialog. Empty categories are
   * omitted. When {@code text} is blank the full model is returned.
   */
  public List<Category> filter(String text) {
    if (StringUtils.isEmpty(text)) {
      return categories;
    }
    SearchMatcher matcher = new SearchMatcher(text, false, false, true);
    List<Category> result = new ArrayList<>();
    for (Category category : categories) {
      List<Item> matched = new ArrayList<>();
      Map<Item, Double> scores = new LinkedHashMap<>();
      for (Item item : category.items()) {
        double score = item.matchScore(matcher);
        if (score > 0.0) {
          scores.put(item, score);
          matched.add(item);
        }
      }
      if (matched.isEmpty()) {
        continue;
      }
      matched.sort(
          (a, b) -> {
            int byScore = Double.compare(scores.get(b), scores.get(a));
            if (byScore != 0) {
              return byScore;
            }
            return String.CASE_INSENSITIVE_ORDER.compare(a.name(), b.name());
          });
      result.add(new Category(category.name(), matched));
    }
    return result;
  }
}
