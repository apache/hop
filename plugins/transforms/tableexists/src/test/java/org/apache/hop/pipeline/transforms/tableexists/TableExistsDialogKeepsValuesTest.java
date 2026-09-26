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

package org.apache.hop.pipeline.transforms.tableexists;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.apache.hop.ui.testing.UpstreamFixture;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Widget;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.eclipse.swtbot.swt.finder.waits.DefaultCondition;
import org.eclipse.swtbot.swt.finder.waits.ICondition;
import org.eclipse.swtbot.swt.finder.widgets.SWTBotShell;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The table name field of {@link TableExistsDialog} is filled with the incoming fields when the
 * user clicks into it. The configured value must survive that, also when the incoming fields can't
 * be loaded (for example because an upstream Table Input has broken SQL, issue #5953).
 */
@Tag("uitest")
class TableExistsDialogKeepsValuesTest extends SwtBotTestBase {

  private static final String CONNECTION = "db";
  private static final String TRANSFORM_NAME = "tableexists";
  private static final String TITLE =
      BaseMessages.getString(TableExistsMeta.class, "TableExistsDialog.Shell.Title");

  @Test
  void tableNameFieldSurvivesFailingUpstream() {
    TableExistsMeta meta = configured("configured_field");
    PipelineMeta pipelineMeta = UpstreamFixture.failingUpstream(TRANSFORM_NAME, meta);

    focusFieldAndPressOk(meta, pipelineMeta);

    assertEquals(
        "configured_field",
        meta.getTableNameField(),
        "OK must keep the configured table name field");
  }

  @Test
  void tableNameFieldSurvivesUpstreamWithFields() {
    TableExistsMeta meta = configured("renamed_field");
    PipelineMeta pipelineMeta = UpstreamFixture.upstreamWithFields(TRANSFORM_NAME, meta, "a", "b");

    focusFieldAndPressOk(meta, pipelineMeta);

    assertEquals(
        "renamed_field",
        meta.getTableNameField(),
        "Loading the incoming fields must not clear the table name field");
  }

  private void focusFieldAndPressOk(TableExistsMeta meta, PipelineMeta pipelineMeta) {
    pipelineMeta.setMetadataProvider(connectionProvider());
    AtomicReference<TableExistsDialog> dialog = new AtomicReference<>();
    withDialog(
        parent -> {
          TableExistsDialog d = new TableExistsDialog(parent, new Variables(), meta, pipelineMeta);
          dialog.set(d);
          d.open();
        },
        bot -> {
          SWTBotShell shell = bot.shell(TITLE);
          // The shell is found as soon as it is created; wait until open() has built and shown it.
          bot.waitUntil(visible(shell));
          SWTBot dialogBot = shell.bot();
          // Clicking into the field loads the incoming fields; dismiss the error that may show.
          postEvent(field(dialog.get()), SWT.FocusIn);
          closeOtherShells(TITLE, 2000);
          // OK can show a message box of its own (e.g. about the connection), so post it too.
          postEvent(dialogBot.button(buttonLabel("System.Button.OK")).widget, SWT.Selection);
          closeOtherShells(TITLE, 1000);
        });
  }

  /** Metadata holding the connection the transform uses, so the dialog finds it. */
  private static MemoryMetadataProvider connectionProvider() {
    MemoryMetadataProvider provider = new MemoryMetadataProvider();
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setName(CONNECTION);
    try {
      provider.getSerializer(DatabaseMeta.class).save(databaseMeta);
    } catch (HopException e) {
      throw new IllegalStateException(e);
    }
    return provider;
  }

  private static ICondition visible(SWTBotShell shell) {
    return new DefaultCondition() {
      @Override
      public boolean test() {
        return shell.isVisible();
      }

      @Override
      public String getFailureMessage() {
        return "Dialog " + TITLE + " was never shown";
      }
    };
  }

  private static Widget field(TableExistsDialog dialog) {
    try {
      return (Widget) FieldUtils.readField(dialog, "wTableName", true);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(e);
    }
  }

  private static TableExistsMeta configured(String fieldName) {
    TableExistsMeta meta = new TableExistsMeta();
    meta.setDefault();
    meta.setConnection(CONNECTION);
    meta.setTableNameField(fieldName);
    return meta;
  }
}
