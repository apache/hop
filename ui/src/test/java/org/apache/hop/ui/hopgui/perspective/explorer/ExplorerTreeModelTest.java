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

package org.apache.hop.ui.hopgui.perspective.explorer;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.search.SearchMatcher;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerTreeModel.Node;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ExplorerTreeModelTest {

  private ExplorerTreeModel model;
  private Node root;
  private Node folderA;
  private Node deepFile;
  private Node otherFile;

  @BeforeEach
  void setUp() {
    model = new ExplorerTreeModel();
    root = new Node("/project", "project", true);
    folderA = new Node("/project/a", "a", true);
    Node folderB = new Node("/project/a/b", "b", true);
    deepFile = new Node("/project/a/b/pipeline.hpl", "pipeline.hpl", false);
    otherFile = new Node("/project/readme.md", "readme.md", false);

    folderB.addChild(deepFile);
    folderA.addChild(folderB);
    root.addChild(folderA);
    root.addChild(otherFile);
    model.setRoot(root, true);
  }

  @Test
  void isUsableForRootPath() {
    assertTrue(model.isUsableFor("/project"));
    assertFalse(model.isUsableFor("/other"));
    model.clear();
    assertFalse(model.isUsableFor("/project"));
  }

  @Test
  void withoutFilterEverythingIsVisible() {
    SearchMatcher matcher = new SearchMatcher("", false, false, false);
    assertTrue(ExplorerTreeModel.isVisible(root, matcher, false));
    assertTrue(ExplorerTreeModel.isVisible(deepFile, matcher, false));
  }

  @Test
  void matchingLeafMakesAncestorsVisible() {
    SearchMatcher matcher = new SearchMatcher("pipeline", false, false, false);
    assertTrue(ExplorerTreeModel.isVisible(deepFile, matcher, true));
    assertTrue(ExplorerTreeModel.isVisible(folderA, matcher, true));
    assertTrue(ExplorerTreeModel.isVisible(root, matcher, true));
    assertFalse(ExplorerTreeModel.isVisible(otherFile, matcher, true));
  }

  @Test
  void nonMatchingNameHiddenWhenNoDescendantMatches() {
    SearchMatcher matcher = new SearchMatcher("zzz-no-match", false, false, false);
    assertFalse(ExplorerTreeModel.isVisible(otherFile, matcher, true));
    assertFalse(ExplorerTreeModel.isVisible(folderA, matcher, true));
    // Root name "project" does not match and no descendant matches → invisible as a child check
    assertFalse(ExplorerTreeModel.isVisible(deepFile, matcher, true));
  }

  @Test
  void folderNameMatchShowsFolderEvenWithoutMatchingChildren() {
    Node emptyFolder = new Node("/project/special-folder", "special-folder", true);
    root.addChild(emptyFolder);
    SearchMatcher matcher = new SearchMatcher("special-folder", false, false, false);
    assertTrue(ExplorerTreeModel.isVisible(emptyFolder, matcher, true));
    assertFalse(ExplorerTreeModel.isVisible(otherFile, matcher, true));
    assertFalse(ExplorerTreeModel.isVisible(deepFile, matcher, true));
  }

  @Test
  void isFilteringRequiresNonEmptyText() {
    assertFalse(ExplorerTreeModel.isFiltering(null));
    assertFalse(ExplorerTreeModel.isFiltering(""));
    assertTrue(ExplorerTreeModel.isFiltering("abc"));
  }
}
