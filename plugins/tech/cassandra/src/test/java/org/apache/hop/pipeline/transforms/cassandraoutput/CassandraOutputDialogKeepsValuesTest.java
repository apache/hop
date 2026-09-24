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

package org.apache.hop.pipeline.transforms.cassandraoutput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.databases.cassandra.datastax.DriverConnection;
import org.apache.hop.databases.cassandra.metadata.CassandraConnection;
import org.apache.hop.databases.cassandra.spi.Keyspace;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.apache.hop.ui.testing.UpstreamFixture;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The table name of Cassandra output must survive "Get table names", also when the keyspace does
 * not (yet) hold that table (follow-up of issue #5953). The keyspace lookup is stubbed, so no
 * Cassandra server is needed.
 */
@Tag("uitest")
class CassandraOutputDialogKeepsValuesTest extends SwtBotTestBase {

  private static final String TRANSFORM_NAME = "cassandra output";
  private static final String CONNECTION_NAME = "cassandra";
  private static final String TITLE =
      BaseMessages.getString(CassandraOutputMeta.class, "CassandraOutputDialog.Shell.Title");
  private static final String GET_TABLES =
      BaseMessages.getString(CassandraOutputMeta.class, "CassandraOutputDialog.GetTable.Button");

  @Test
  void tableNameSurvivesGetTableNames() throws Exception {
    CassandraOutputMeta meta = new CassandraOutputMeta();
    meta.setDefault();
    meta.setConnectionName(CONNECTION_NAME);
    meta.setTableName("new_table");
    PipelineMeta pipelineMeta = UpstreamFixture.withoutUpstream(TRANSFORM_NAME, meta);

    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    metadataProvider
        .getSerializer(CassandraConnection.class)
        .save(new StubConnection(List.of("users", "orders")));

    withDialog(
        parent -> {
          CassandraOutputDialog dialog =
              new CassandraOutputDialog(parent, new Variables(), meta, pipelineMeta);
          dialog.setMetadataProvider(metadataProvider);
          dialog.open();
        },
        bot -> {
          bot.shell(TITLE).activate();
          Button getTables = findButton(TITLE, GET_TABLES);
          postEvent(getTables, SWT.Selection);
          closeOtherShells(TITLE, 1000);
          bot.shell(TITLE).activate().bot().button(buttonLabel("System.Button.OK")).click();
        });

    assertEquals("new_table", meta.getTableName(), "OK must keep the configured table name");
  }

  /** A connection whose keyspace lists the given tables without talking to a server. */
  private static class StubConnection extends CassandraConnection {
    private final Keyspace keyspace;

    StubConnection(List<String> tableNames) throws Exception {
      setName(CONNECTION_NAME);
      keyspace = mock(Keyspace.class);
      when(keyspace.getTableNamesCQL3()).thenReturn(tableNames);
    }

    @Override
    public DriverConnection createConnection(IVariables variables, boolean output) {
      return null;
    }

    @Override
    public Keyspace lookupKeyspace(DriverConnection connection, IVariables variables) {
      return keyspace;
    }
  }

  /**
   * Finds a button of the dialog by its label, also on a tab that isn't in front (SWTBot's finder
   * never looks inside the tab items of a CTabFolder).
   */
  private static Button findButton(String shellTitle, String label) {
    AtomicReference<Button> found = new AtomicReference<>();
    display.syncExec(
        () -> {
          for (Shell shell : display.getShells()) {
            if (shellTitle.equals(shell.getText())) {
              found.set(findButton(shell, label));
            }
          }
        });
    assertNotNull(found.get(), "no button labeled " + label);
    return found.get();
  }

  private static Button findButton(Composite parent, String label) {
    for (Control child : parent.getChildren()) {
      if (child instanceof Button button
          && label.equals(button.getText().replace("&", "").trim())) {
        return button;
      }
      if (child instanceof Composite composite) {
        Button button = findButton(composite, label);
        if (button != null) {
          return button;
        }
      }
    }
    return null;
  }
}
