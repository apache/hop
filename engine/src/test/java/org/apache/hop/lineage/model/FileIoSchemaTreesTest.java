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

package org.apache.hop.lineage.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.List;
import org.junit.jupiter.api.Test;

class FileIoSchemaTreesTest {

  @Test
  void mergePaths_jsonPathsSharePrefix_buildsSingleTree() {
    List<FileIoTabularColumn> cols =
        List.of(
            new FileIoTabularColumn(
                "a", "String", -1, -1, "$.store.book", FileIoPathSyntax.JSON_PATH, false),
            new FileIoTabularColumn(
                "b", "Integer", -1, -1, "$.store.price", FileIoPathSyntax.JSON_PATH, false));
    List<FileIoSchemaNode> roots = FileIoSchemaTrees.mergePaths(cols);
    assertEquals(1, roots.size());
    FileIoSchemaNode store = roots.get(0);
    assertEquals("store", store.getSegment());
    assertEquals("object", store.getKind());
    assertEquals(2, store.getChildren().size());
  }

  @Test
  void mergePaths_xpathSplitsOnSlash() {
    List<FileIoTabularColumn> cols =
        List.of(
            new FileIoTabularColumn(
                "x", "String", -1, -1, "/root/item/title", FileIoPathSyntax.XPATH, false));
    List<FileIoSchemaNode> roots = FileIoSchemaTrees.mergePaths(cols);
    assertFalse(roots.isEmpty());
    assertEquals("root", roots.get(0).getSegment());
    assertEquals(1, roots.get(0).getChildren().size());
    assertEquals("item", roots.get(0).getChildren().get(0).getSegment());
  }
}
