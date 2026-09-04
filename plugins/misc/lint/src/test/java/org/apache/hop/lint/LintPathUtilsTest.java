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
package org.apache.hop.lint;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class LintPathUtilsTest {

  @Test
  public void pathsMatchForSameLocalPath() {
    assertTrue(LintPathUtils.pathsMatch("/tmp/project/foo.hpl", "/tmp/project/foo.hpl"));
  }

  @Test
  public void pathsMatchForFileUriAndLocalPath() {
    assertTrue(LintPathUtils.pathsMatch("file:///tmp/project/foo.hpl", "/tmp/project/foo.hpl"));
  }

  @Test
  public void pathsDoNotMatchDifferentFiles() {
    assertFalse(LintPathUtils.pathsMatch("/tmp/project/foo.hpl", "/tmp/project/bar.hpl"));
  }

  @Test
  public void resolveExplorerFilePathUsesFullHopFilePath() {
    String fullPath = "/tmp/project/pipelines/extract.hpl";
    assertTrue(
        LintPathUtils.pathsMatch(
            fullPath, LintPathUtils.resolveExplorerFilePath(fullPath, "extract.hpl")));
  }

  @Test
  public void resolveExplorerFilePathJoinsFolderAndName() {
    assertTrue(
        LintPathUtils.pathsMatch(
            "/tmp/project/pipelines/extract.hpl",
            LintPathUtils.resolveExplorerFilePath("/tmp/project/pipelines/", "extract.hpl")));
  }
}
