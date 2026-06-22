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

package org.apache.hop.ui.hopgui.search;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.search.ISearchResult;
import org.apache.hop.core.search.ISearchable;
import org.apache.hop.core.search.SearchResult;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.hopgui.file.pipeline.HopPipelineFileType;
import org.apache.hop.ui.hopgui.search.HopGuiSearchHelper.SearchObjectGroup;
import org.apache.hop.ui.hopgui.search.HopGuiSearchHelper.SearchSection;
import org.apache.hop.ui.hopgui.search.HopGuiSearchHelper.SearchTypeGroup;
import org.junit.jupiter.api.Test;

/** Unit tests for the two-tier grouping that both search windows render. */
class HopGuiSearchHelperTest {

  /** A pipeline searchable (its searchable object is a PipelineMeta so it can be "open"). */
  private static ISearchable pipeline(String name, String filename) {
    ISearchable searchable = mock(ISearchable.class);
    when(searchable.getName()).thenReturn(name);
    when(searchable.getType()).thenReturn(HopPipelineFileType.PIPELINE_FILE_TYPE_DESCRIPTION);
    when(searchable.getFilename()).thenReturn(filename);
    when(searchable.getSearchableObject()).thenReturn(mock(PipelineMeta.class));
    return searchable;
  }

  /** A non-file searchable (metadata / variable) - never "open", grouped by its type. */
  private static ISearchable other(String name, String type) {
    ISearchable searchable = mock(ISearchable.class);
    when(searchable.getName()).thenReturn(name);
    when(searchable.getType()).thenReturn(type);
    when(searchable.getFilename()).thenReturn(null);
    when(searchable.getSearchableObject()).thenReturn(new Object());
    return searchable;
  }

  private static ISearchResult nameMatch(ISearchable searchable) {
    return new SearchResult(searchable, searchable.getName(), "name", null, searchable.getName());
  }

  private static ISearchResult fieldMatch(ISearchable searchable, String component, String value) {
    return new SearchResult(searchable, value, component + " : field", component, value);
  }

  private static Map<String, Integer> source(ISearchable searchable, int index) {
    Map<String, Integer> map = new HashMap<>();
    map.put(HopGuiSearchHelper.searchableKey(searchable), index);
    return map;
  }

  @Test
  void openPipelineGoesToOpenSection() {
    ISearchable open = pipeline("loader", "/p/loader.hpl");
    List<SearchSection> sections =
        HopGuiSearchHelper.groupResults(List.of(nameMatch(open)), source(open, 0));

    assertEquals(1, sections.size());
    assertEquals(HopGuiSearchHelper.SECTION_OPEN, sections.get(0).getKey());
    assertEquals(1, sections.get(0).getObjectCount());
  }

  @Test
  void projectPipelineGoesToProjectSection() {
    ISearchable onDisk = pipeline("loader", "/p/loader.hpl");
    // Found in the project location (index 1), not the GUI/open location (index 0).
    List<SearchSection> sections =
        HopGuiSearchHelper.groupResults(List.of(nameMatch(onDisk)), source(onDisk, 1));

    assertEquals(1, sections.size());
    assertEquals(HopGuiSearchHelper.SECTION_PROJECT, sections.get(0).getKey());
  }

  @Test
  void metadataIsNeverOpenEvenFromGuiLocation() {
    ISearchable meta = other("mysql-prod", "Relational Database Connection");
    // Even though it is reported by the GUI location (index 0), it is not a file => Project.
    List<SearchSection> sections =
        HopGuiSearchHelper.groupResults(List.of(nameMatch(meta)), source(meta, 0));

    assertEquals(HopGuiSearchHelper.SECTION_PROJECT, sections.get(0).getKey());
    assertFalse(HopGuiSearchHelper.isOpenObject(meta, source(meta, 0)));
  }

  @Test
  void nameMatchMarksObjectButAddsNoChildRow() {
    ISearchable open = pipeline("loader", "/p/loader.hpl");
    List<SearchSection> sections =
        HopGuiSearchHelper.groupResults(List.of(nameMatch(open)), source(open, 0));

    SearchObjectGroup group = sections.get(0).getTypeGroups().get(0).getObjects().get(0);
    assertTrue(group.getMatches().isEmpty(), "a name match should not become a child row");
  }

  @Test
  void fieldMatchesBecomeChildRowsUnderTheirObject() {
    ISearchable open = pipeline("loader", "/p/loader.hpl");
    ISearchResult name = nameMatch(open);
    ISearchResult t1 = fieldMatch(open, "Table input", "select 1");
    ISearchResult t2 = fieldMatch(open, "Output", "customers");

    List<SearchSection> sections =
        HopGuiSearchHelper.groupResults(List.of(name, t1, t2), source(open, 0));

    // One object node with two inner matches.
    assertEquals(1, sections.get(0).getObjectCount());
    SearchObjectGroup group = sections.get(0).getTypeGroups().get(0).getObjects().get(0);
    assertEquals(2, group.getMatches().size());
    // The name match represents the object node itself.
    assertSame(name, group.getPrimary());
  }

  @Test
  void identicalMatchesCollapseButDistinctOnesAreKept() {
    ISearchable pipe = pipeline("loader", "/p/loader.hpl");
    // The same transform matched as both its "name" and its "plugin name" (same component + value).
    ISearchResult byName =
        new SearchResult(
            pipe, "PG Bulk Loader", "transform name", "PG Bulk Loader", "PG Bulk Loader");
    ISearchResult byPlugin =
        new SearchResult(
            pipe, "PG Bulk Loader", "transform plugin name", "PG Bulk Loader", "PG Bulk Loader");
    // A genuinely different match inside the same transform (different value) must be kept.
    ISearchResult byField =
        new SearchResult(
            pipe,
            "public.orders",
            "transform property : schema",
            "PG Bulk Loader",
            "public.orders");

    List<SearchSection> sections =
        HopGuiSearchHelper.groupResults(List.of(byName, byPlugin, byField), source(pipe, 0));

    SearchObjectGroup group = sections.get(0).getTypeGroups().get(0).getObjects().get(0);
    assertEquals(2, group.getMatches().size(), "duplicate (component, value) rows collapse to one");
  }

  @Test
  void typeGroupsAreOrderedPipelinesThenOthersThenVariables() {
    ISearchable pipe = pipeline("loader", "/p/loader.hpl");
    ISearchable conn = other("mysql-prod", "Relational Database Connection");
    ISearchable variable = other("HTTP_PROXY", "Variable");

    Map<String, Integer> sourceByKey = new HashMap<>();
    sourceByKey.put(HopGuiSearchHelper.searchableKey(pipe), 1);
    sourceByKey.put(HopGuiSearchHelper.searchableKey(conn), 0);
    sourceByKey.put(HopGuiSearchHelper.searchableKey(variable), 0);

    // Feed them in a deliberately "wrong" order to prove the helper sorts the type groups.
    List<SearchSection> sections =
        HopGuiSearchHelper.groupResults(
            List.of(nameMatch(variable), nameMatch(conn), nameMatch(pipe)), sourceByKey);

    // All three land in the Project section (pipe is on disk, the others are non-files).
    assertEquals(1, sections.size());
    List<SearchTypeGroup> typeGroups = sections.get(0).getTypeGroups();
    assertEquals(HopPipelineFileType.PIPELINE_FILE_TYPE_DESCRIPTION, typeGroups.get(0).getType());
    assertEquals("Relational Database Connection", typeGroups.get(1).getType());
    assertEquals("Variable", typeGroups.get(2).getType());
  }

  @Test
  void isNameMatchDistinguishesNameFromFieldMatches() {
    ISearchable open = pipeline("loader", "/p/loader.hpl");
    assertTrue(HopGuiSearchHelper.isNameMatch(nameMatch(open)));
    assertFalse(HopGuiSearchHelper.isNameMatch(fieldMatch(open, "Table input", "select 1")));
  }
}
