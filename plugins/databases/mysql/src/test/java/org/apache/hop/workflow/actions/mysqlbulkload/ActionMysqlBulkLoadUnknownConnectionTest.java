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

package org.apache.hop.workflow.actions.mysqlbulkload;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.workflow.WorkflowMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedConstruction;

/** A connection that does not exist, the same case as #9166 in Bulk load from MySQL into file. */
class ActionMysqlBulkLoadUnknownConnectionTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @TempDir Path tempDir;

  @BeforeAll
  static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
  }

  @Test
  void unknownConnectionIsAFalseResultWithoutOpeningADatabase() throws Exception {
    ActionMysqlBulkLoad action = newAction();
    action.setParentWorkflowMeta(mock(WorkflowMeta.class));

    try (MockedConstruction<Database> databases = mockConstruction(Database.class)) {
      Result result = action.execute(new Result(), 0);

      assertFalse(result.getResult());
      assertEquals(1, result.getNrErrors());
      assertTrue(databases.constructed().isEmpty());
    }
  }

  @Test
  void unknownConnectionOnlyHasTheFileAsResourceDependency() throws Exception {
    ActionMysqlBulkLoad action = newAction();
    WorkflowMeta workflowMeta = mock(WorkflowMeta.class);
    action.setParentWorkflowMeta(workflowMeta);

    List<ResourceReference> references =
        action.getResourceDependencies(new Variables(), workflowMeta);

    assertEquals(1, references.size());
    List<ResourceEntry> entries = references.get(0).getEntries();
    assertEquals(1, entries.size());
    assertEquals(ResourceType.FILE, entries.get(0).getResourcetype());
  }

  private ActionMysqlBulkLoad newAction() throws Exception {
    Path dataFile = Files.writeString(tempDir.resolve("data.csv"), "1;2\n");
    ActionMysqlBulkLoad action = new ActionMysqlBulkLoad("bulkload");
    action.setFileName(dataFile.toAbsolutePath().toString());
    action.setTableName("mytable");
    action.setConnection("does-not-exist");
    action.setLocalInFile(true);
    return action;
  }
}
