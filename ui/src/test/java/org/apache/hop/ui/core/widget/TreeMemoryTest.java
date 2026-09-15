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

package org.apache.hop.ui.core.widget;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TreeMemoryTest {

  private static final String TREE_NAME = "test-tree";

  @BeforeEach
  void setUp() {
    TreeMemory.getInstance().clearTree(TREE_NAME);
  }

  @Test
  void testStoreAndCheckExpanded() {
    TreeMemory memory = TreeMemory.getInstance();
    assertNotNull(memory);

    String[] rootPath = new String[] {"project"};
    String[] subPath = new String[] {"project", "subfolder"};
    String[] otherPath = new String[] {"project", "other"};

    assertFalse(memory.isExpanded(TREE_NAME, rootPath));
    assertFalse(memory.isExpanded(TREE_NAME, subPath));

    memory.storeExpanded(TREE_NAME, rootPath, true);
    memory.storeExpanded(TREE_NAME, subPath, true);

    assertTrue(memory.isExpanded(TREE_NAME, rootPath));
    assertTrue(memory.isExpanded(TREE_NAME, subPath));
    assertFalse(memory.isExpanded(TREE_NAME, otherPath));

    // Collapse subfolder
    memory.storeExpanded(TREE_NAME, subPath, false);
    assertTrue(memory.isExpanded(TREE_NAME, rootPath));
    assertFalse(memory.isExpanded(TREE_NAME, subPath));
  }

  @Test
  void testClearTree() {
    TreeMemory memory = TreeMemory.getInstance();

    String[] path1 = new String[] {"root", "folder1"};
    String[] path2 = new String[] {"root", "folder2"};

    memory.storeExpanded(TREE_NAME, path1, true);
    memory.storeExpanded(TREE_NAME, path2, true);
    memory.storeExpanded("another-tree", path1, true);

    memory.clearTree(TREE_NAME);

    assertFalse(memory.isExpanded(TREE_NAME, path1));
    assertFalse(memory.isExpanded(TREE_NAME, path2));
    assertTrue(memory.isExpanded("another-tree", path1));

    memory.clearTree("another-tree");
    assertFalse(memory.isExpanded("another-tree", path1));
  }
}
