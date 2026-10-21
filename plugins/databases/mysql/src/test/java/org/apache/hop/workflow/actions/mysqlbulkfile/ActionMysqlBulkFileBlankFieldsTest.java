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

package org.apache.hop.workflow.actions.mysqlbulkfile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.sql.PreparedStatement;
import org.apache.hop.core.HopEnvironment;
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
 * Blank schema, column list, separator, enclosure and line settings must be left out of the
 * generated SELECT ... INTO OUTFILE statement. Up to 2.17 a saved empty tag such as {@code
 * <schemaname/>} loaded as null; since 2.18 it loads as an empty string, which must not produce
 * {@code FROM .mytable}, an empty column list or {@code TERMINATED BY ''}.
 */
class ActionMysqlBulkFileBlankFieldsTest {
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
        runAndCaptureSql("<schemaname/><listcolumn/><separator/><enclosed/><lineterminated/>");

    assertTrue(sql.startsWith("SELECT  * INTO OUTFILE '"), sql);
    assertTrue(sql.contains(" FROM mytable "), sql);
    assertFalse(sql.contains(".mytable"), sql);
    assertFalse(sql.contains("FIELDS"), sql);
    assertFalse(sql.contains("ENCLOSED"), sql);
    assertFalse(sql.contains("LINES"), sql);
    assertFalse(sql.contains("BY ''"), sql);
    assertFalse(sql.contains("``"), sql);
  }

  @Test
  void specifiedSettingsAreInTheStatement() throws Exception {
    String sql =
        runAndCaptureSql(
            "<schemaname>myschema</schemaname><listcolumn>a,b</listcolumn>"
                + "<separator>;</separator><enclosed>\"</enclosed>"
                + "<lineterminated>|</lineterminated>");

    assertTrue(sql.startsWith("SELECT  `a`, `b` INTO OUTFILE '"), sql);
    assertTrue(
        sql.contains("FIELDS TERMINATED BY ';' ENCLOSED BY '\"' LINES TERMINATED BY '|'"), sql);
    assertTrue(sql.contains(" FROM myschema.mytable "), sql);
  }

  private String runAndCaptureSql(String settingsXml) throws Exception {
    Path outFile = tempDir.resolve("out.txt");
    String xml =
        "<action><name>bulkfile</name><type>MYSQL_BULK_FILE</type>"
            + "<connection>mysql</connection>"
            + "<tablename>mytable</tablename>"
            + "<filename>"
            + outFile.toAbsolutePath()
            + "</filename>"
            + "<limitlines>0</limitlines><highpriority>N</highpriority>"
            + "<optionenclosed>N</optionenclosed><outdumpvalue>0</outdumpvalue>"
            + "<iffileexists>2</iffileexists><addfiletoresult>N</addfiletoresult>"
            + settingsXml
            + "</action>";
    Node actionNode = XmlHandler.getSubNode(XmlHandler.loadXmlString(xml), "action");
    ActionMysqlBulkFile action = new ActionMysqlBulkFile();
    action.loadXml(actionNode, new MemoryMetadataProvider(), new Variables());

    WorkflowMeta workflowMeta = mock(WorkflowMeta.class);
    when(workflowMeta.findDatabase(eq("mysql"), any())).thenReturn(mock(DatabaseMeta.class));
    action.setParentWorkflowMeta(workflowMeta);

    try (MockedConstruction<Database> databases =
        mockConstruction(
            Database.class,
            (db, context) -> {
              when(db.checkTableExists(any(), any())).thenReturn(true);
              when(db.prepareSql(anyString())).thenReturn(mock(PreparedStatement.class));
            })) {
      Result result = action.execute(new Result(), 0);

      assertEquals(1, databases.constructed().size());
      ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
      verify(databases.constructed().get(0)).prepareSql(sql.capture());
      assertTrue(result.getResult());
      return sql.getValue();
    }
  }
}
