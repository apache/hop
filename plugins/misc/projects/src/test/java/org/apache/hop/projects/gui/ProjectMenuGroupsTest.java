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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.projects.project.ProjectConfig;
import org.junit.jupiter.api.Test;

class ProjectMenuGroupsTest {

  @Test
  void blankGroupIsUngrouped() {
    assertFalse(ProjectMenuGroups.isGrouped(null));
    assertFalse(ProjectMenuGroups.isGrouped(config("plain", null)));
    assertFalse(ProjectMenuGroups.isGrouped(config("plain", "")));
    assertFalse(ProjectMenuGroups.isGrouped(config("plain", "   ")));
    assertTrue(ProjectMenuGroups.isGrouped(config("client", " Clients ")));
    assertEquals("Clients", ProjectMenuGroups.groupOf(config("client", " Clients ")));
    assertEquals("", ProjectMenuGroups.groupOf(null));
  }

  @Test
  void groupsTopicsAndSkipsProjectsWithoutATopic() {
    List<ProjectConfig> projects = new ArrayList<>();
    projects.add(config("zeta", "beta"));
    projects.add(config("alpha", "Alpha"));
    projects.add(config("mid", " alpha "));
    projects.add(config("none", "  "));
    projects.add(config("  ", "Clients"));
    projects.add(config("Demo", "Clients"));
    projects.add(config("demo", "Clients"));
    projects.add(config("other", "clients"));
    projects.add(null);

    Map<String, List<String>> groups = ProjectMenuGroups.byGroup(projects);

    assertEquals(
        List.of("Alpha", "alpha", "beta", "Clients", "clients"), new ArrayList<>(groups.keySet()));
    assertEquals(List.of("alpha"), groups.get("Alpha"));
    assertEquals(List.of("mid"), groups.get("alpha"));
    assertEquals(List.of("zeta"), groups.get("beta"));
    assertEquals(List.of("Demo"), groups.get("Clients"));
    assertEquals(List.of("other"), groups.get("clients"));
    assertEquals(Map.of(), ProjectMenuGroups.byGroup(null));
    assertEquals(Map.of(), ProjectMenuGroups.byGroup(List.of()));
    assertEquals(Map.of(), ProjectMenuGroups.byGroup(List.of(config("solo", null))));
  }

  @Test
  void namesInsideATopicAreSortedIgnoringCase() {
    Map<String, List<String>> groups =
        ProjectMenuGroups.byGroup(
            List.of(config("b", "Topic"), config("A", "Topic"), config("c", "Topic")));

    assertEquals(List.of("A", "b", "c"), groups.get("Topic"));
  }

  @Test
  void recentWindowDropsGroupedNamesAndIgnoresNamesPastTheCap() {
    Map<String, ProjectConfig> byName = new HashMap<>();
    byName.put("grouped", config("grouped", "Clients"));
    byName.put("plain", config("plain", null));
    byName.put("later", config("later", null));

    assertEquals(
        List.of("missing", "plain"),
        ProjectMenuGroups.recentUngrouped(
            List.of("grouped", "missing", "plain", "later"), byName::get, 3));
    assertEquals(
        List.of("missing"),
        ProjectMenuGroups.recentUngrouped(List.of("grouped", "missing", "plain"), byName::get, 2));
    assertEquals(List.of("a"), ProjectMenuGroups.recentUngrouped(List.of("a", "b"), null, 1));
    assertEquals(List.of(), ProjectMenuGroups.recentUngrouped(List.of("", "a"), null, 1));
    assertEquals(List.of("a"), ProjectMenuGroups.recentUngrouped(List.of("", "a"), null, 2));
    assertEquals(List.of(), ProjectMenuGroups.recentUngrouped(null, byName::get, 5));
    assertEquals(List.of(), ProjectMenuGroups.recentUngrouped(List.of("plain"), byName::get, 0));
  }

  private static ProjectConfig config(String name, String group) {
    ProjectConfig projectConfig = new ProjectConfig(name, "/tmp/" + name, "project-config.json");
    projectConfig.setGroup(group);
    return projectConfig;
  }
}
