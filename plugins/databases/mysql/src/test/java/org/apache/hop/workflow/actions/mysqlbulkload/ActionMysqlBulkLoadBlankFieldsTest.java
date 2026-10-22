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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.workflow.WorkflowMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.w3c.dom.Node;

/**
 * Blank schema, column list, separator, enclosure, escape and line settings must be left out of the
 * generated LOAD DATA statement. Up to 2.17 a saved empty tag such as {@code <schemaname/>} loaded
 * as null; since 2.18 it loads as an empty string, which must not produce {@code .mytable} or
 * {@code TERMINATED BY ''}.
 */
class ActionMysqlBulkLoadBlankFieldsTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @TempDir Path tempDir;

  @BeforeAll
  static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
  }

  @Test
  void blankSettingsAreLeftOutOfTheStatement() throws Exception {
    String sql =
        runAndCaptureSql(
            "<schemaname/><separator/><enclosed/><escaped/>"
                + "<linestarted/><lineterminated/><listattribut/>");

    assertTrue(sql.contains(" INTO TABLE mytable "), sql);
    assertFalse(sql.contains(".mytable"), sql);
    assertFalse(sql.contains("FIELDS"), sql);
    assertFalse(sql.contains("BY ''"), sql);
    assertFalse(sql.contains("LINES"), sql);
    assertFalse(sql.contains("(``)"), sql);
  }

  @Test
  void specifiedSettingsAreInTheStatement() throws Exception {
    String sql =
        runAndCaptureSql(
            "<schemaname>myschema</schemaname><separator>;</separator><enclosed>\"</enclosed>"
                + "<escaped>\\</escaped><linestarted>x</linestarted>"
                + "<lineterminated>|</lineterminated><listattribut>a,b</listattribut>");

    assertTrue(sql.contains(" INTO TABLE myschema.mytable "), sql);
    assertTrue(sql.contains("FIELDS TERMINATED BY ';' ENCLOSED BY '\"' ESCAPED BY '\\'"), sql);
    assertTrue(sql.contains("LINES STARTING BY 'x' TERMINATED BY '|'"), sql);
    assertTrue(sql.contains("(`a`, `b`)"), sql);
  }

  @Test
  void checkReadsFileNameAndTableName() throws Exception {
    Path dataFile = Files.writeString(tempDir.resolve("check.csv"), "a\n");
    ActionMysqlBulkLoad action = new ActionMysqlBulkLoad();
    action.setFileName(dataFile.toAbsolutePath().toString());
    action.setTableName("mytable");

    List<ICheckResult> remarks = new ArrayList<>();
    action.check(remarks, null, new Variables(), null);

    assertTrue(errors(remarks).isEmpty(), () -> errors(remarks).toString());
  }

  private static List<String> errors(List<ICheckResult> remarks) {
    List<String> errors = new ArrayList<>();
    for (ICheckResult remark : remarks) {
      if (remark.getType() == ICheckResult.TYPE_RESULT_ERROR) {
        errors.add(remark.getText());
      }
    }
    return errors;
  }

  private String runAndCaptureSql(String settingsXml) throws Exception {
    Path dataFile = Files.writeString(tempDir.resolve("data.csv"), "1;2\n");
    String xml =
        "<action><name>bulk</name><type>MYSQL_BULK_LOAD</type>"
            + "<connection>mysql</connection>"
            + "<tablename>mytable</tablename>"
            + "<filename>"
            + dataFile.toAbsolutePath()
            + "</filename>"
            + "<ignorelines>0</ignorelines><replacedata>N</replacedata>"
            + "<localinfile>Y</localinfile><prorityvalue>0</prorityvalue>"
            + "<addfiletoresult>N</addfiletoresult>"
            + settingsXml
            + "</action>";
    Node actionNode = XmlHandler.getSubNode(XmlHandler.loadXmlString(xml), "action");
    ActionMysqlBulkLoad action = new ActionMysqlBulkLoad();
    action.loadXml(actionNode, new MemoryMetadataProvider(), new Variables());

    WorkflowMeta workflowMeta = mock(WorkflowMeta.class);
    when(workflowMeta.findDatabase(eq("mysql"), any())).thenReturn(mock(DatabaseMeta.class));
    action.setParentWorkflowMeta(workflowMeta);

    try (MockedConstruction<Database> databases =
        mockConstruction(
            Database.class,
            (db, context) -> when(db.checkTableExists(any(), any())).thenReturn(true))) {
      Result result = action.execute(new Result(), 0);

      assertEquals(1, databases.constructed().size());
      ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
      verify(databases.constructed().get(0)).execStatement(sql.capture());
      assertTrue(result.getResult());
      return sql.getValue();
    }
  }
}
