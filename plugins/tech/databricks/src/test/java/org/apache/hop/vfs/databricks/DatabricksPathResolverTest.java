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

package org.apache.hop.vfs.databricks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.exception.HopException;
import org.junit.jupiter.api.Test;

class DatabricksPathResolverTest {

  @Test
  void normalizeRootPathStripsSlashAndValidates() throws Exception {
    assertEquals(
        "/Volumes/apache-hop/default/testing",
        DatabricksPathResolver.normalizeRootPath("/Volumes/apache-hop/default/testing/"));
    assertEquals("/Volumes/c/s/v", DatabricksPathResolver.normalizeRootPath("dbfs:/Volumes/c/s/v"));
  }

  @Test
  void normalizeRootPathRejectsNonFilesApi() {
    HopException ex =
        assertThrows(
            HopException.class, () -> DatabricksPathResolver.normalizeRootPath("/FileStore/x"));
    assertTrue(ex.getMessage().contains("/Volumes"));
  }

  @Test
  void toWorkspacePathWithoutRootKeepsLogical() {
    assertEquals("/input", DatabricksPathResolver.toWorkspacePath("/input", ""));
    assertEquals(
        "/Volumes/c/s/v/x", DatabricksPathResolver.toWorkspacePath("/Volumes/c/s/v/x", null));
  }

  @Test
  void toWorkspacePathWithRootJoinsRelative() {
    String root = "/Volumes/apache-hop/default/testing";
    assertEquals(root, DatabricksPathResolver.toWorkspacePath("/", root));
    assertEquals(root, DatabricksPathResolver.toWorkspacePath("", root));
    assertEquals(root + "/input", DatabricksPathResolver.toWorkspacePath("/input", root));
    assertEquals(
        root + "/input/customers-1M.txt",
        DatabricksPathResolver.toWorkspacePath("/input/customers-1M.txt", root));
  }

  @Test
  void toWorkspacePathWithRootAllowsAbsoluteEscape() {
    String root = "/Volumes/apache-hop/default/testing";
    assertEquals(
        "/Volumes/other/default/vol/file",
        DatabricksPathResolver.toWorkspacePath("/Volumes/other/default/vol/file", root));
  }
}
