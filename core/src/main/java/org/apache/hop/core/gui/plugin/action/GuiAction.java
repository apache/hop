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
import org.apache.hop.core.gui.plugin.IGuiActionLambda;
import org.apache.hop.core.search.SearchMatcher;

public class GuiAction {
  private String id;
  private GuiActionType type;
  private String name;
  private String tooltip;
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
    // Weight the fields so a hit on the name ranks above a hit on a tooltip/type, which in turn
    // ranks above a hit on a keyword or category. This keeps the most relevant actions (those whose
    // name matches) at the top and stops loosely-related keyword/category hits from crowding in.
    double best = matcher.score(name);
    best = Math.max(best, 0.9 * matcher.score(tooltip));
    if (type != null) {
      best = Math.max(best, 0.9 * matcher.score(type.name()));
    }
    if (keywords != null) {
      for (String keyword : keywords) {
        best = Math.max(best, 0.8 * matcher.score(keyword));
      }
    }
    return Math.max(best, 0.7 * matcher.score(category));
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
