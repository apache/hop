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

package org.apache.hop.workflow.actions.folderscompare;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hop.core.Result;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** How entries and wildcards are matched, see https://github.com/apache/hop/issues/9117 */
class ActionFoldersCompareMatchingTest {

  @TempDir Path testFolder;

  private ActionFoldersCompare action;
  private Path left;
  private Path right;

  @BeforeAll
  static void setUpBeforeClass() {
    HopLogStore.init();
  }

  @BeforeEach
  void setUp() throws IOException {
    IWorkflowEngine<WorkflowMeta> workflow = new LocalWorkflowEngine(new WorkflowMeta());
    action = new ActionFoldersCompare();
    workflow.getWorkflowMeta().addAction(new ActionMeta(action));
    action.setParentWorkflow(workflow);
    action.setParentWorkflowMeta(mock(WorkflowMeta.class));
    workflow.setStopped(false);

    left = Files.createDirectories(testFolder.resolve("left"));
    right = Files.createDirectories(testFolder.resolve("right"));
    action.setFilename1(left.toString());
    action.setFilename2(right.toString());
    action.setCompareOnly("all");
  }

  @Test
  void sameFileNameInDifferentSubfoldersIsADifference() throws Exception {
    write(left, "s1/x.txt");
    write(left, "s2/x.txt");
    write(right, "s1/x.txt");
    write(right, "s2/y.txt");
    action.setIncludeSubFolders(true);

    assertFalse(action.execute(new Result(), 0).isResult());
  }

  @Test
  void identicalSubfoldersAreEqual() throws Exception {
    write(left, "s1/x.txt");
    write(left, "s2/x.txt");
    write(right, "s1/x.txt");
    write(right, "s2/x.txt");
    action.setIncludeSubFolders(true);

    assertTrue(action.execute(new Result(), 0).isResult());
  }

  @Test
  void wildcardIsResolved() throws Exception {
    write(left, "a.csv");
    write(left, "other.txt");
    write(right, "a.csv");
    write(right, "different.txt");
    action.setCompareOnly("specify");
    action.setWildcard("${MASK}");
    action.setVariable("MASK", ".*\\.csv");

    assertTrue(action.execute(new Result(), 0).isResult(), "only the csv files are compared");

    write(right, "b.csv");
    assertFalse(action.execute(new Result(), 0).isResult(), "b.csv is only on the right");
  }

  @Test
  void invalidWildcardIsAnError() throws Exception {
    write(left, "a.csv");
    write(right, "b.csv");
    action.setCompareOnly("specify");
    action.setWildcard("[");

    Result result = action.execute(new Result(), 0);

    assertFalse(result.isResult());
    assertEquals(1, result.getNrErrors());
  }

  @Test
  void storedWildcardIsIgnoredWhenComparingAll() throws Exception {
    write(left, "a.csv");
    write(right, "b.csv");
    action.setCompareOnly("all");
    action.setWildcard("[");

    Result result = action.execute(new Result(), 0);

    assertFalse(result.isResult(), "a.csv and b.csv are different folders");
    assertEquals(0, result.getNrErrors());
  }

  private static void write(Path root, String relativePath) throws IOException {
    Path file = root.resolve(relativePath);
    Files.createDirectories(file.getParent());
    Files.writeString(file, "same", StandardCharsets.UTF_8);
  }
}
