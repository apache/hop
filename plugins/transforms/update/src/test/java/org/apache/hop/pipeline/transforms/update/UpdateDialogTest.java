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

package org.apache.hop.pipeline.transforms.update;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.eclipse.swtbot.swt.finder.waits.DefaultCondition;
import org.eclipse.swtbot.swt.finder.widgets.SWTBotShell;
import org.eclipse.swtbot.swt.finder.widgets.SWTBotTable;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * End-to-end SWTBot coverage for the Update transform's {@link UpdateDialog}. The dialog runs its
 * own blocking event loop in {@code open()}, so {@link SwtBotTestBase#withDialog} pumps it on the
 * UI thread while the assertions drive it from a worker thread.
 *
 * <p>Both grids are Hop {@link org.apache.hop.ui.core.widget.TableView}s whose cell editors only
 * materialise on click, so cell content is staged directly on the underlying SWT {@link Table} (on
 * the UI thread) the way {@code ConstantDialogTest} does it. The real OK/Cancel buttons are then
 * clicked so the dialog's own {@code ok()}/{@code cancel()} logic is what gets exercised.
 *
 * <p>Tagged {@code uitest} so it is skipped when there is no display. The default reactor run still
 * includes it on a desktop; wrap Maven with {@code tools/with-isolated-display.sh} so the dialog
 * does not steal focus.
 */
@Tag("uitest")
class UpdateDialogTest extends SwtBotTestBase {

  private static final String TRANSFORM_NAME = "update";
  private static final String DIALOG_TITLE = "Update";
  private static final String CONNECTION_NAME = "test-db";

  // Tab order as built by open(): General, Lookup keys, Fields.
  private static final int KEYS_TAB = 1;
  private static final int FIELDS_TAB = 2;

  // Text widgets in creation order. The connection is a CCombo, not a Text, so it does not shift
  // these; address it with ccomboBox(0).
  private static final int TRANSFORM_NAME_TEXT = 0;
  private static final int SCHEMA_TEXT = 1;
  private static final int TABLE_TEXT = 2;
  private static final int COMMIT_TEXT = 3;
  private static final int FLAG_FIELD_TEXT = 4;

  // Check boxes in creation order, all on the General tab.
  private static final int BATCH_CHECK = 0;
  private static final int SKIP_LOOKUP_CHECK = 1;
  private static final int ERROR_IGNORED_CHECK = 2;

  // Grid layout: column 0 holds the row number, the mapped columns follow.
  private static final int KEY_TABLE_FIELD_COLUMN = 1;
  private static final int KEY_CONDITION_COLUMN = 2;
  private static final int KEY_STREAM_COLUMN = 3;
  private static final int KEY_STREAM2_COLUMN = 4;
  private static final int UPDATE_TABLE_FIELD_COLUMN = 1;
  private static final int UPDATE_STREAM_COLUMN = 2;

  @Test
  void existingMetaIsShownInTheDialog() throws Exception {
    UpdateMeta meta =
        metaWith(
            List.of(
                new UpdateKeyField("id_stream", "id", "=", ""),
                new UpdateKeyField("from_stream", "day", "BETWEEN", "to_stream")),
            List.of(new UpdateField("name", "name_stream")));
    meta.setCommitSize("500");
    meta.setUseBatchUpdate(true);
    meta.setErrorIgnored(true);
    meta.setIgnoreFlagField("was_found");

    withDialog(
        openerFor(meta),
        bot -> {
          SWTBot dialog = dialogBot(bot);

          assertEquals(TRANSFORM_NAME, dialog.text(TRANSFORM_NAME_TEXT).getText());
          assertEquals(CONNECTION_NAME, dialog.ccomboBox(0).getText());
          assertEquals("myschema", dialog.text(SCHEMA_TEXT).getText());
          assertEquals("mytable", dialog.text(TABLE_TEXT).getText());
          assertEquals("500", dialog.text(COMMIT_TEXT).getText());
          assertEquals("was_found", dialog.text(FLAG_FIELD_TEXT).getText());
          assertTrue(dialog.checkBox(BATCH_CHECK).isChecked());
          assertFalse(dialog.checkBox(SKIP_LOOKUP_CHECK).isChecked());
          assertTrue(dialog.checkBox(ERROR_IGNORED_CHECK).isChecked());

          SWTBotTable keys = grid(dialog, KEYS_TAB);
          assertEquals("id", keys.cell(0, KEY_TABLE_FIELD_COLUMN));
          assertEquals("=", keys.cell(0, KEY_CONDITION_COLUMN));
          assertEquals("id_stream", keys.cell(0, KEY_STREAM_COLUMN));
          assertEquals("day", keys.cell(1, KEY_TABLE_FIELD_COLUMN));
          assertEquals("BETWEEN", keys.cell(1, KEY_CONDITION_COLUMN));
          assertEquals("from_stream", keys.cell(1, KEY_STREAM_COLUMN));
          assertEquals("to_stream", keys.cell(1, KEY_STREAM2_COLUMN));

          SWTBotTable fields = grid(dialog, FIELDS_TAB);
          assertEquals("name", fields.cell(0, UPDATE_TABLE_FIELD_COLUMN));
          assertEquals("name_stream", fields.cell(0, UPDATE_STREAM_COLUMN));

          dialog.button(buttonLabel("System.Button.Cancel")).click();
        });
  }

  @Test
  void okWritesTheEditedDialogBackToMeta() throws Exception {
    UpdateMeta meta =
        metaWith(
            List.of(new UpdateKeyField("old_stream", "old_key", "=", "")),
            List.of(new UpdateField("old_field", "old_field_stream")));

    withDialog(
        openerFor(meta),
        bot -> {
          SWTBot dialog = dialogBot(bot);

          dialog.text(SCHEMA_TEXT).setText("newschema");
          dialog.text(TABLE_TEXT).setText("newtable");
          dialog.text(COMMIT_TEXT).setText("250");
          dialog.checkBox(BATCH_CHECK).click();

          setCells(
              grid(dialog, KEYS_TAB).widget,
              0,
              Map.of(
                  KEY_TABLE_FIELD_COLUMN, "id",
                  KEY_CONDITION_COLUMN, ">=",
                  KEY_STREAM_COLUMN, "id_stream",
                  KEY_STREAM2_COLUMN, ""));
          setCells(
              grid(dialog, FIELDS_TAB).widget,
              0,
              Map.of(
                  UPDATE_TABLE_FIELD_COLUMN, "name",
                  UPDATE_STREAM_COLUMN, "name_stream"));

          dialog.button(buttonLabel("System.Button.OK")).click();
        });

    assertEquals(CONNECTION_NAME, meta.getConnection());
    assertEquals("newschema", meta.getLookupField().getSchemaName());
    assertEquals("newtable", meta.getLookupField().getTableName());
    assertEquals("250", meta.getCommitSizeVar());
    assertTrue(meta.isUseBatchUpdate());

    assertEquals(1, meta.getLookupField().getLookupKeys().size());
    UpdateKeyField key = meta.getLookupField().getLookupKeys().get(0);
    assertEquals("id", key.getKeyLookup());
    assertEquals(">=", key.getKeyCondition());
    assertEquals("id_stream", key.getKeyStream());

    assertEquals(1, meta.getLookupField().getUpdateFields().size());
    UpdateField field = meta.getLookupField().getUpdateFields().get(0);
    assertEquals("name", field.getUpdateLookup());
    assertEquals("name_stream", field.getUpdateStream());
  }

  /**
   * Skipping the lookup means no SELECT is issued, so there is no lookup that could fail and no key
   * to flag as found. The dialog has to clear and disable both options, and the cleared state is
   * what gets saved. This is the UI half of the confusion behind issue #4772.
   */
  @Test
  void skipLookupClearsAndDisablesTheLookupFailureOptions() throws Exception {
    UpdateMeta meta =
        metaWith(
            List.of(new UpdateKeyField("id_stream", "id", "=", "")),
            List.of(new UpdateField("name", "name_stream")));
    meta.setErrorIgnored(true);
    meta.setIgnoreFlagField("was_found");

    withDialog(
        openerFor(meta),
        bot -> {
          SWTBot dialog = dialogBot(bot);
          assertTrue(dialog.checkBox(ERROR_IGNORED_CHECK).isChecked(), "precondition");
          assertEquals("was_found", dialog.text(FLAG_FIELD_TEXT).getText(), "precondition");

          dialog.checkBox(SKIP_LOOKUP_CHECK).click();

          assertFalse(dialog.checkBox(ERROR_IGNORED_CHECK).isChecked());
          assertFalse(dialog.checkBox(ERROR_IGNORED_CHECK).isEnabled());
          assertEquals("", dialog.text(FLAG_FIELD_TEXT).getText());
          assertFalse(dialog.text(FLAG_FIELD_TEXT).isEnabled());

          dialog.button(buttonLabel("System.Button.OK")).click();
        });

    assertTrue(meta.isSkipLookup());
    assertFalse(meta.isErrorIgnored());
    assertEquals("", meta.getIgnoreFlagField());
  }

  /** Rows the user blanked out are dropped rather than saved as empty keys/fields. */
  @Test
  void blankGridRowsAreNotSaved() throws Exception {
    UpdateMeta meta =
        metaWith(
            List.of(
                new UpdateKeyField("id_stream", "id", "=", ""),
                new UpdateKeyField("other_stream", "other", "=", "")),
            List.of(
                new UpdateField("name", "name_stream"), new UpdateField("label", "label_stream")));

    withDialog(
        openerFor(meta),
        bot -> {
          SWTBot dialog = dialogBot(bot);

          setCells(
              grid(dialog, KEYS_TAB).widget,
              1,
              Map.of(
                  KEY_TABLE_FIELD_COLUMN, "",
                  KEY_CONDITION_COLUMN, "",
                  KEY_STREAM_COLUMN, "",
                  KEY_STREAM2_COLUMN, ""));
          setCells(
              grid(dialog, FIELDS_TAB).widget,
              1,
              Map.of(UPDATE_TABLE_FIELD_COLUMN, "", UPDATE_STREAM_COLUMN, ""));

          dialog.button(buttonLabel("System.Button.OK")).click();
        });

    assertEquals(1, meta.getLookupField().getLookupKeys().size());
    assertEquals("id", meta.getLookupField().getLookupKeys().get(0).getKeyLookup());
    assertEquals(1, meta.getLookupField().getUpdateFields().size());
    assertEquals("name", meta.getLookupField().getUpdateFields().get(0).getUpdateLookup());
  }

  @Test
  void cancelLeavesMetaUntouched() throws Exception {
    UpdateMeta meta =
        metaWith(
            List.of(new UpdateKeyField("id_stream", "id", "=", "")),
            List.of(new UpdateField("name", "name_stream")));

    withDialog(
        openerFor(meta),
        bot -> {
          SWTBot dialog = dialogBot(bot);

          dialog.text(TABLE_TEXT).setText("discarded");
          dialog.checkBox(SKIP_LOOKUP_CHECK).click();
          setCells(
              grid(dialog, KEYS_TAB).widget, 0, Map.of(KEY_TABLE_FIELD_COLUMN, "discarded_key"));

          dialog.button(buttonLabel("System.Button.Cancel")).click();
        });

    assertEquals("mytable", meta.getLookupField().getTableName());
    assertFalse(meta.isSkipLookup());
    assertEquals("id", meta.getLookupField().getLookupKeys().get(0).getKeyLookup());
  }

  /**
   * {@code createShell()} registers the dialog's shell well before {@code open()} reaches the event
   * loop that puts it on screen, so the worker can find a shell that is not visible yet. Waiting
   * for visibility before activating keeps that race out of the tests - this dialog builds three
   * tabs, two grids and a background field lookup - and turns a dialog that fails to open into a
   * clear message rather than an activation timeout.
   */
  private SWTBot dialogBot(SWTBot bot) {
    SWTBotShell dialogShell = bot.shell(DIALOG_TITLE);
    bot.waitUntil(
        new DefaultCondition() {
          @Override
          public boolean test() {
            return dialogShell.isOpen() && dialogShell.isVisible();
          }

          @Override
          public String getFailureMessage() {
            return "The " + DIALOG_TITLE + " dialog never became visible";
          }
        });
    return dialogShell.activate().bot();
  }

  /**
   * Activates the tab holding a grid and returns it. Only the visible tab's table is reachable, so
   * after activating there is exactly one table to find.
   */
  private SWTBotTable grid(SWTBot dialog, int tabIndex) {
    dialog.cTabItem(tabIndex).activate();
    return dialog.table(0);
  }

  private Consumer<Shell> openerFor(UpdateMeta meta) throws HopException {
    PipelineMeta pipelineMeta = pipelineWith(meta);
    return parent -> new UpdateDialog(parent, new Variables(), meta, pipelineMeta).open();
  }

  /**
   * Stages cell text on one grid row from the UI thread. The dialog's TableView installs its
   * editors on click, so there is no SWTBot path to type into a cell; writing the item text leaves
   * the same state the editors would, and {@code ok()} then reads it for real.
   */
  private void setCells(Table table, int row, Map<Integer, String> valuesByColumn) {
    display.syncExec(
        () -> valuesByColumn.forEach((column, value) -> table.getItem(row).setText(column, value)));
  }

  private static UpdateMeta metaWith(List<UpdateKeyField> keys, List<UpdateField> fields) {
    UpdateMeta meta = new UpdateMeta();
    // Same starting point a transform dropped on the canvas gets; notably it is what fills in the
    // commit size, which getData() reads without a null guard.
    meta.setDefault();
    meta.setConnection(CONNECTION_NAME);
    UpdateLookupField lookupField = new UpdateLookupField();
    lookupField.setSchemaName("myschema");
    lookupField.setTableName("mytable");
    lookupField.setLookupKeys(new ArrayList<>(keys));
    lookupField.setUpdateFields(new ArrayList<>(fields));
    meta.setLookupField(lookupField);
    return meta;
  }

  /**
   * The dialog warns - through a native MessageBox that SWTBot cannot dismiss - when the configured
   * connection cannot be resolved, so the pipeline gets a real (driverless "NONE") connection of
   * that name to look up.
   */
  private static PipelineMeta pipelineWith(UpdateMeta meta) throws HopException {
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setName(CONNECTION_NAME);
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    metadataProvider.getSerializer(DatabaseMeta.class).save(databaseMeta);

    String pluginId = PluginRegistry.getInstance().getPluginId(TransformPluginType.class, meta);
    assertNotNull(pluginId, "Update transform must be registered via HopEnvironment.init()");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setMetadataProvider(metadataProvider);
    pipelineMeta.addTransform(new TransformMeta(pluginId, TRANSFORM_NAME, meta));
    return pipelineMeta;
  }
}
