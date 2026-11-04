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

package org.apache.hop.workflow.actions.xml.xmlwellformed;

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
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Result-behaviour tests for {@link XmlWellFormed} (apache/hop#9174). */
class XmlWellFormedResultTest {

  private static final String WELL_FORMED = "<?xml version=\"1.0\"?><root><a>1</a></root>";

  @TempDir Path folder;

  private XmlWellFormed action;

  @BeforeAll
  static void setUpBeforeClass() {
    HopLogStore.init();
  }

  @BeforeEach
  void setUp() {
    IWorkflowEngine<WorkflowMeta> workflow = new LocalWorkflowEngine(new WorkflowMeta());
    action = new XmlWellFormed();
    workflow.getWorkflowMeta().addAction(new ActionMeta(action));
    action.setParentWorkflow(workflow);
    action.setParentWorkflowMeta(mock(WorkflowMeta.class));
    workflow.setStopped(false);
  }

  @Test
  void basenameWildcardMatchesFileName() throws Exception {
    Files.writeString(folder.resolve("good.xml"), WELL_FORMED, StandardCharsets.UTF_8);

    XmlWellFormedField field = new XmlWellFormedField();
    field.setSourceFilefolder(folder.toString());
    field.setWildcard("good\\.xml");
    action.setSourceFileFolders(List.of(field));

    Result result = action.execute(new Result(), 0);

    assertTrue(result.isResult());
    assertEquals(0, result.getNrErrors());
    assertEquals(1, result.getNrLinesWritten(), "basename wildcard must check the file");
  }

  @Test
  void basenameWildcardMatchesFileNameInSubfolder() throws Exception {
    Path subFolder = Files.createDirectory(folder.resolve("nested"));
    Files.writeString(subFolder.resolve("good.xml"), WELL_FORMED, StandardCharsets.UTF_8);
    Files.writeString(folder.resolve("skip.xml"), WELL_FORMED, StandardCharsets.UTF_8);

    XmlWellFormedField field = new XmlWellFormedField();
    field.setSourceFilefolder(folder.toString());
    field.setWildcard("good\\.xml");
    action.setSourceFileFolders(List.of(field));
    action.includeSubfolders = true;

    Result result = action.execute(new Result(), 0);

    assertTrue(result.isResult());
    assertEquals(0, result.getNrErrors());
    assertEquals(
        1, result.getNrLinesWritten(), "basename wildcard must check the nested file by name");
  }

  @Test
  void oneFieldResultRowUsesEmptyWildcard() throws Exception {
    Files.writeString(folder.resolve("good.xml"), WELL_FORMED, StandardCharsets.UTF_8);

    action.argFromPrevious = true;

    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("filename"));
    RowMetaAndData row = new RowMetaAndData(rowMeta, new Object[] {folder.toString()});

    Result previous = new Result();
    previous.getRows().add(row);

    Result result = action.execute(previous, 0);

    assertTrue(result.isResult());
    assertEquals(0, result.getNrErrors());
    assertEquals(
        1, result.getNrLinesWritten(), "a one-field folder row must check the whole folder");
  }
}
