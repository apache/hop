/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.projects.gui;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.projects.project.ProjectConfig;

/**
 * How registered projects are split between the recent list and group submenus.
 *
 * <p>A project with a group topic is not repeated in the recent list. It is listed under that topic
 * instead, so a long project list collapses to one menu entry per topic.
 */
public final class ProjectMenuGroups {

  private ProjectMenuGroups() {}

  /**
   * @param projectConfig registration, or null
   * @return trimmed group topic, or an empty string when the project is ungrouped
   */
  public static String groupOf(ProjectConfig projectConfig) {
    if (projectConfig == null) {
      return "";
    }
    return StringUtils.trimToEmpty(projectConfig.getGroup());
  }

  /**
   * @param projectConfig registration, or null
   * @return true when the project belongs in a group submenu
   */
  public static boolean isGrouped(ProjectConfig projectConfig) {
    return StringUtils.isNotEmpty(groupOf(projectConfig));
  }

  /**
   * Projects that share a group topic.
   *
   * <p>The map iteration order is the topic names sorted case-insensitively. Names inside a topic
   * are sorted the same way. Blank topics, blank names and null registrations are omitted.
   * Whitespace around a topic is ignored, so {@code " Clients "} joins {@code "Clients"}. Topics
   * that differ only by case stay separate. A name is listed once per topic.
   *
   * @param projects registered projects, or null
   * @return topic to project names; empty when nothing is grouped
   */
  public static Map<String, List<String>> byGroup(List<ProjectConfig> projects) {
    if (projects == null || projects.isEmpty()) {
      return Map.of();
    }
    Map<String, List<String>> exact = new LinkedHashMap<>();
    for (ProjectConfig projectConfig : projects) {
      if (projectConfig == null) {
        continue;
      }
      String group = groupOf(projectConfig);
      String name = StringUtils.trimToEmpty(projectConfig.getProjectName());
      if (StringUtils.isEmpty(group) || StringUtils.isEmpty(name)) {
        continue;
      }
      List<String> names = exact.computeIfAbsent(group, key -> new ArrayList<>());
      if (!containsIgnoreCase(names, name)) {
        names.add(name);
      }
    }
    if (exact.isEmpty()) {
      return Map.of();
    }
    List<String> labels = new ArrayList<>(exact.keySet());
    labels.sort(String.CASE_INSENSITIVE_ORDER);
    Map<String, List<String>> ordered = new LinkedHashMap<>();
    for (String label : labels) {
      List<String> names = exact.get(label);
      names.sort(String.CASE_INSENSITIVE_ORDER);
      ordered.put(label, names);
    }
    return ordered;
  }

  /**
   * Flat recent-menu entries.
   *
   * <p>Only the first {@code max} names are considered, which is the same window the menu used
   * before grouping. Names in that window that have a group topic are left out (they belong in
   * {@link #byGroup}). A missing registration is treated as ungrouped. Empty names are skipped but
   * still consume a slot in the window.
   *
   * @param recentNames recent project names, most relevant first, or null
   * @param lookup registration for a name; null treats every name as ungrouped
   * @param max number of leading names to consider
   * @return names to show as top-level recent items, in the same order
   */
  public static List<String> recentUngrouped(
      List<String> recentNames, Function<String, ProjectConfig> lookup, int max) {
    if (recentNames == null || recentNames.isEmpty() || max <= 0) {
      return List.of();
    }
    List<String> shown = new ArrayList<>();
    int considered = 0;
    for (String name : recentNames) {
      if (considered == max) {
        break;
      }
      considered++;
      if (StringUtils.isEmpty(name)) {
        continue;
      }
      ProjectConfig projectConfig = lookup == null ? null : lookup.apply(name);
      if (isGrouped(projectConfig)) {
        continue;
      }
      shown.add(name);
    }
    return shown;
  }

  private static boolean containsIgnoreCase(List<String> names, String name) {
    for (String existing : names) {
      if (existing.equalsIgnoreCase(name)) {
        return true;
      }
    }
    return false;
  }
}
