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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.gui.plugin.IGuiActionLambda;
import org.apache.hop.core.search.SearchMatcher;

public class GuiAction {
  private String id;
  private GuiActionType type;
  private String name;
  private String tooltip;

  /**
   * Usage hints shown below the tooltip (e.g. "Click-drag to select and position"). Unlike the
   * tooltip they describe the dialog rather than the action, so they are never searched.
   */
  private List<String> tooltipHints;

  private String image;
  private IGuiActionLambda actionLambda;
  private String guiPluginClassName;
  private String guiPluginMethodName;
  private ClassLoader classLoader;
  private List<String> keywords;
  private String category;
  private String categoryOrder;

  public GuiAction() {
    this.keywords = new ArrayList<>();
    this.tooltipHints = new ArrayList<>();
  }

  /**
   * It's a direct action using a simple lambda
   *
   * @param id
   * @param type
   * @param name
   * @param tooltip
   * @param image
   * @param actionLambda
   */
  public GuiAction(
      String id,
      GuiActionType type,
      String name,
      String tooltip,
      String image,
      IGuiActionLambda actionLambda) {
    this();
    this.id = id;
    this.type = type;
    this.name = name;
    this.tooltip = tooltip;
    this.image = image;
    this.actionLambda = actionLambda;
  }

  /**
   * We don't know the action to execute, just the method so we defer looking up the action until we
   * have the parent object in hand. We're just storing the method name in this case. You can use
   * method createLambda() to build it when you have the parent object and parameter to pass.
   *
   * @param id
   * @param type
   * @param name
   * @param tooltip
   * @param image
   * @param guiPluginMethodName
   */
  public GuiAction(
      String id,
      GuiActionType type,
      String name,
      String tooltip,
      String image,
      String guiPluginClassName,
      String guiPluginMethodName) {
    this();
    this.id = id;
    this.type = type;
    this.name = name;
    this.tooltip = tooltip;
    this.image = image;
    this.guiPluginClassName = guiPluginClassName;
    this.guiPluginMethodName = guiPluginMethodName;
  }

  public GuiAction(GuiAction guiAction) {
    this();
    this.id = guiAction.id;
    this.type = guiAction.type;
    this.name = guiAction.name;
    this.tooltip = guiAction.tooltip;
    this.image = guiAction.image;
    this.guiPluginClassName = guiAction.guiPluginClassName;
    this.guiPluginMethodName = guiAction.guiPluginMethodName;
    this.actionLambda = guiAction.actionLambda;
    for (String keyword : guiAction.getKeywords()) {
      keywords.add(keyword);
    }
    tooltipHints.addAll(guiAction.getTooltipHints());
    this.category = guiAction.category;
    this.categoryOrder = guiAction.categoryOrder;
    this.classLoader = guiAction.getClassLoader();
  }

  /**
   * Score this action against a prepared matcher. Used to filter and rank actions (e.g. in the
   * context dialog). Returns the best score across the action's name, tooltip, type, keywords and
   * category; {@code 0} means no match.
   *
   * @param matcher the shared search matcher built from the user's filter text
   * @return the best relevance score in {@code [0,1]}
   */
  public double matchScore(SearchMatcher matcher) {
    Match match = match(matcher);
    return match == null ? 0.0 : match.score();
  }

  /** The field of an action that a search matched on. */
  public enum MatchSource {
    NAME,
    DESCRIPTION,
    TYPE,
    KEYWORD,
    CATEGORY
  }

  /**
   * Why an action matched a search.
   *
   * @param score the weighted relevance score in {@code (0,1]}
   * @param source the field that gave the best score
   * @param matchedText the value of that field, e.g. the keyword that matched
   */
  public record Match(double score, MatchSource source, String matchedText) {}

  /**
   * Score this action against a prepared matcher and tell which field gave the best score, so the
   * user can see why an action is in the list. When several fields score the same, the first one
   * (name, description, type, keyword, category) is reported.
   *
   * @param matcher the shared search matcher built from the user's filter text
   * @return the best match, or null if nothing matched
   */
  public Match match(SearchMatcher matcher) {
    // Weight the fields so a hit on the name ranks above a hit on a tooltip/type, which in turn
    // ranks above a hit on a keyword or category. This keeps the most relevant actions (those whose
    // name matches) at the top and stops loosely-related keyword/category hits from crowding in.
    Match best = better(null, matcher.score(name), MatchSource.NAME, name);
    best = better(best, 0.9 * matcher.score(tooltip), MatchSource.DESCRIPTION, tooltip);
    if (type != null) {
      best = better(best, 0.9 * matcher.score(type.name()), MatchSource.TYPE, type.name());
    }
    if (keywords != null) {
      for (String keyword : keywords) {
        double score = 0.8 * matcher.score(keyword);
        if (score > 0.0) {
          best = better(best, score, MatchSource.KEYWORD, matchingPart(matcher, keyword));
        }
      }
    }
    return better(best, 0.7 * matcher.score(category), MatchSource.CATEGORY, category);
  }

  /**
   * Keywords are often a single comma separated string ("sql,query,database,select,jdbc"). Report
   * the part that matched best rather than the whole list, or the whole string when the match spans
   * several parts.
   */
  private static String matchingPart(SearchMatcher matcher, String keyword) {
    if (keyword == null || !keyword.contains(",")) {
      return keyword;
    }
    String best = keyword;
    double bestScore = 0.0;
    for (String part : keyword.split(",")) {
      double score = matcher.score(part);
      if (score > bestScore) {
        bestScore = score;
        best = part.trim();
      }
    }
    return best;
  }

  private static Match better(Match best, double score, MatchSource source, String text) {
    if (score > 0.0 && (best == null || score > best.score())) {
      return new Match(score, source, text);
    }
    return best;
  }

  @Override
  public String toString() {
    return "GuiAction{"
        + "id='"
        + id
        + '\''
        + ", type="
        + type
        + ", name='"
        + name
        + '\''
        + ", tooltip='"
        + tooltip
        + '\''
        + '}';
  }

  /**
   * For any name longer than 30 characters we return the first 28 characters + "...";
   *
   * @return the short name
   */
  public String getShortName() {
    if (name == null) {
      return null;
    }
    if (name.length() < 30) {
      return name;
    } else {
      return name.substring(0, 28) + "...";
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GuiAction guiAction = (GuiAction) o;
    return id.equals(guiAction.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }

  /**
   * Gets id
   *
   * @return value of id
   */
  public String getId() {
    return id;
  }

  /**
   * @param id The id to set
   */
  public void setId(String id) {
    this.id = id;
  }

  /**
   * Gets type
   *
   * @return value of type
   */
  public GuiActionType getType() {
    return type;
  }

  /**
   * @param type The type to set
   */
  public void setType(GuiActionType type) {
    this.type = type;
  }

  /**
   * Gets name
   *
   * @return value of name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name The name to set
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * Gets tooltip
   *
   * @return value of tooltip
   */
  public String getTooltip() {
    return tooltip;
  }

  /**
   * @param tooltip The tooltip to set
   */
  public void setTooltip(String tooltip) {
    this.tooltip = tooltip;
  }

  /**
   * Gets the usage hints shown below the tooltip. They are not searched.
   *
   * @return value of tooltipHints
   */
  public List<String> getTooltipHints() {
    return tooltipHints;
  }

  /**
   * @param tooltipHints The usage hints to set
   */
  public void setTooltipHints(List<String> tooltipHints) {
    this.tooltipHints = tooltipHints;
  }

  /**
   * The text to show for this action: the tooltip followed by each usage hint on its own line.
   *
   * @return the tooltip and hints, never null
   */
  public String getDisplayTooltip() {
    StringBuilder text = new StringBuilder(StringUtils.trimToEmpty(tooltip));
    if (tooltipHints != null) {
      for (String hint : tooltipHints) {
        String trimmed = StringUtils.trimToEmpty(hint);
        if (trimmed.isEmpty()) {
          continue;
        }
        if (!text.isEmpty()) {
          text.append('\n');
        }
        text.append(trimmed);
      }
    }
    return text.toString();
  }

  /**
   * Gets image
   *
   * @return value of image
   */
  public String getImage() {
    return image;
  }

  /**
   * @param image The image to set
   */
  public void setImage(String image) {
    this.image = image;
  }

  /**
   * Gets actionLambda
   *
   * @return value of actionLambda
   */
  public IGuiActionLambda getActionLambda() {
    return actionLambda;
  }

  /**
   * @param actionLambda The actionLambda to set
   */
  public void setActionLambda(IGuiActionLambda actionLambda) {
    this.actionLambda = actionLambda;
  }

  /**
   * Gets guiPluginClassName
   *
   * @return value of guiPluginClassName
   */
  public String getGuiPluginClassName() {
    return guiPluginClassName;
  }

  /**
   * @param guiPluginClassName The guiPluginClassName to set
   */
  public void setGuiPluginClassName(String guiPluginClassName) {
    this.guiPluginClassName = guiPluginClassName;
  }

  /**
   * Gets methodName
   *
   * @return value of methodName
   */
  public String getGuiPluginMethodName() {
    return guiPluginMethodName;
  }

  /**
   * @param guiPluginMethodName The methodName to set
   */
  public void setGuiPluginMethodName(String guiPluginMethodName) {
    this.guiPluginMethodName = guiPluginMethodName;
  }

  /**
   * Gets classLoader
   *
   * @return value of classLoader
   */
  public ClassLoader getClassLoader() {
    return classLoader;
  }

  /**
   * @param classLoader The classLoader to set
   */
  public void setClassLoader(ClassLoader classLoader) {
    this.classLoader = classLoader;
  }

  /**
   * Gets keywords
   *
   * @return value of keywords
   */
  public List<String> getKeywords() {
    return keywords;
  }

  /**
   * @param keywords The keywords to set
   */
  public void setKeywords(List<String> keywords) {
    this.keywords = keywords;
  }

  /**
   * Gets category
   *
   * @return value of category
   */
  public String getCategory() {
    return category;
  }

  /**
   * @param category The category to set
   */
  public void setCategory(String category) {
    this.category = category;
  }

  /**
   * Gets categoryOrder
   *
   * @return value of categoryOrder
   */
  public String getCategoryOrder() {
    return categoryOrder;
  }

  /**
   * @param categoryOrder The categoryOrder to set
   */
  public void setCategoryOrder(String categoryOrder) {
    this.categoryOrder = categoryOrder;
  }
}
