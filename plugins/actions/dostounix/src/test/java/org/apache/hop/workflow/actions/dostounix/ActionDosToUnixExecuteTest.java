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

package org.apache.hop.workflow.actions.dostounix;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** See https://github.com/apache/hop/issues/9117 */
class ActionDosToUnixExecuteTest {

  private static final String DOS_CONTENT = "line1\r\nline2\r\n";
  private static final String UNIX_CONTENT = "line1\nline2\n";

  @TempDir Path folder;

  private ActionDosToUnix action;
  private Path dosFile;

  @BeforeAll
  static void setUpBeforeClass() {
    HopLogStore.init();
  }

  @BeforeEach
  void setUp() throws Exception {
    IWorkflowEngine<WorkflowMeta> workflow = new LocalWorkflowEngine(new WorkflowMeta());
    action = new ActionDosToUnix();
    workflow.getWorkflowMeta().addAction(new ActionMeta(action));
    action.setParentWorkflow(workflow);
    action.setParentWorkflowMeta(mock(WorkflowMeta.class));
    workflow.setStopped(false);
    action.setResultFilenames("nothing");
    // A real run inherits the system properties, the temporary folder among them
    action.initializeFrom(Variables.getADefaultVariableSpace());

    dosFile = folder.resolve("dos.txt");
    Files.writeString(dosFile, DOS_CONTENT, StandardCharsets.UTF_8);
  }

  @Test
  void wildcardIsMatchedAgainstTheFileName() throws Exception {
    addField(folder.toString(), "dos\\.txt");

    Result result = action.execute(new Result(), 0);

    assertTrue(result.isResult());
    assertEquals(1, result.getNrLinesWritten(), "the file must be converted");
    assertEquals(UNIX_CONTENT, Files.readString(dosFile, StandardCharsets.UTF_8));
  }

  @Test
  void wildcardOnTheWholePathStillMatches() throws Exception {
    addField(folder.toString(), ".*/dos\\.txt");

    Result result = action.execute(new Result(), 0);

    assertTrue(result.isResult());
    assertEquals(1, result.getNrLinesWritten(), "the file must be converted");
    assertEquals(UNIX_CONTENT, Files.readString(dosFile, StandardCharsets.UTF_8));
  }

  @Test
  void resultRowWithOneFieldDoesNotStopTheWorkflow() throws Exception {
    action.setArgFromPrevious(true);
    RowMetaAndData row = new RowMetaAndData();
    row.addValue(new ValueMetaString("filename"), dosFile.toString());
    Result previous = new Result();
    previous.setRows(List.of(row));

    Result result = action.execute(previous, 0);

    assertTrue(result.isResult());
    assertEquals(0, result.getNrErrors());
    assertEquals(1, result.getNrLinesWritten(), "the file must be converted (guess)");
    assertEquals(UNIX_CONTENT, Files.readString(dosFile, StandardCharsets.UTF_8));
  }

  @Test
  void resultRowWithTwoFieldsDoesNotStopTheWorkflow() throws Exception {
    action.setArgFromPrevious(true);
    RowMetaAndData row = new RowMetaAndData();
    row.addValue(new ValueMetaString("folder"), folder.toString());
    row.addValue(new ValueMetaString("wildcard"), "dos\\.txt");
    Result previous = new Result();
    previous.setRows(List.of(row));

    Result result = action.execute(previous, 0);

    assertTrue(result.isResult());
    assertEquals(0, result.getNrErrors());
    assertEquals(1, result.getNrLinesWritten(), "the file must be converted (guess)");
  }

  private void addField(String sourceFileFolder, String wildcard) {
    ActionDosToUnix.Fields field = new ActionDosToUnix.Fields();
    field.setSourceFileFolder(sourceFileFolder);
    field.setWildcard(wildcard);
    field.setConversionTypes("dostounix");
    action.getFields().add(field);
  }
}
