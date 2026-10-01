/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.selectvalues;

import static org.eclipse.swtbot.swt.finder.matchers.WidgetMatcherFactory.widgetOfType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.eclipse.swtbot.swt.finder.widgets.SWTBotTable;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * SWTBot coverage for the Meta-data tab of {@link SelectValuesDialog}, in particular for what
 * happens to the cell being edited when the user leaves it by clicking the empty area under the
 * grid.
 *
 * <p>The Format column is a {@code COLUMN_TYPE_FORMAT} column, edited with the same combo as a
 * {@code COLUMN_TYPE_CCOMBO} column - but the TableView mouse listener that commits an open editor
 * before handling the click only knows the TEXT and CCOMBO types. The format combo is therefore
 * still open, and still what TableView's shared {@code combo} field points at, when the click below
 * the rows moves the active cell to the Fieldname column and inserts a row; the commit that the
 * insert then does reads that stale combo and writes the mask into the Fieldname cell. {@link
 * #editingTheFormatCellMustNotLeakIntoTheFieldnameColumn()} pins that down; {@link
 * #editingTheTypeCellStaysInTheTypeColumn()} is the same scene on a plain CCOMBO column, which is
 * committed correctly today.
 *
 * <p>Tagged {@code uitest} so it is skipped when there is no display. The default reactor run still
 * includes it on a desktop; wrap Maven with {@code tools/with-isolated-display.sh} so the dialog
 * does not steal focus.
 */
@Tag("uitest")
class SelectValuesDialogMetaTabTest extends SwtBotTestBase {

  private static final String TRANSFORM_NAME = "select values";

  private static final String DIALOG_TITLE =
      BaseMessages.getString(SelectValuesMeta.class, "SelectValuesDialog.Shell.Label");
  private static final String META_TAB =
      BaseMessages.getString(SelectValuesMeta.class, "SelectValuesDialog.MetaTab.TabItem");
  private static final String FORMAT_HEADER =
      BaseMessages.getString(SelectValuesMeta.class, "SelectValuesDialog.ColumnInfo.Format");

  /** Grid layout of the Meta-data tab: column 0 holds the row number, the fields follow. */
  private static final int FIELDNAME_COLUMN = 1;

  private static final int TYPE_COLUMN = 3;
  private static final int FORMAT_COLUMN = 7;

  /** The Meta-data tab is the third of the dialog's tabs. */
  private static final int META_TAB_INDEX = 2;

  private static final String MASK = "MM/dd/yyyy HH:mm:ss";

  /**
   * Issue: typing a format mask and then clicking outside the editor copies the mask into the
   * Fieldname column of another row.
   */
  @Test
  void editingTheFormatCellMustNotLeakIntoTheFieldnameColumn() {
    SelectValuesMeta meta = metaWith(change("sdfsdfsdf", "Timestamp"), change("other", "String"));

    withDialog(
        openerFor(meta),
        bot -> {
          SWTBot dialog = dialogBot(bot);
          activateTheMetaTab(dialog);

          SWTBotTable grid = metaGrid(dialog);
          grid.click(0, FORMAT_COLUMN);
          typeIntoTheOpenComboEditor(grid.widget, MASK);

          clickTheEmptyAreaUnderTheGrid(grid.widget);

          assertEquals(
              MASK, grid.cell(0, FORMAT_COLUMN), "the mask belongs to the cell it was typed in");
          assertEquals(
              "other",
              grid.cell(1, FIELDNAME_COLUMN),
              "leaving the format editor must not overwrite another row's fieldname");

          dialog.button(buttonLabel("System.Button.Cancel")).click();
        });
  }

  /** The same scene on a plain CCOMBO column, which TableView does commit before the click. */
  @Test
  void editingTheTypeCellStaysInTheTypeColumn() {
    SelectValuesMeta meta = metaWith(change("sdfsdfsdf", "Timestamp"), change("other", "String"));

    withDialog(
        openerFor(meta),
        bot -> {
          SWTBot dialog = dialogBot(bot);
          activateTheMetaTab(dialog);

          SWTBotTable grid = metaGrid(dialog);
          grid.click(0, TYPE_COLUMN);
          typeIntoTheOpenComboEditor(grid.widget, "Date");

          clickTheEmptyAreaUnderTheGrid(grid.widget);

          assertEquals(
              "Date", grid.cell(0, TYPE_COLUMN), "the type belongs to the cell it was typed in");
          assertEquals(
              "other",
              grid.cell(1, FIELDNAME_COLUMN),
              "leaving the type editor must not overwrite another row's fieldname");

          dialog.button(buttonLabel("System.Button.Cancel")).click();
        });
  }

  private SWTBot dialogBot(SWTBot bot) {
    return bot.shell(DIALOG_TITLE).activate().bot();
  }

  /**
   * Brings the Meta-data tab to the front. Done on the folder rather than through {@code
   * bot.cTabItem(...)} because SWTBot's widget finder only walks controls and never sees the tab
   * items of a dialog's CTabFolder.
   */
  private void activateTheMetaTab(SWTBot dialog) {
    CTabFolder tabFolder = (CTabFolder) dialog.widget(widgetOfType(CTabFolder.class));
    display.syncExec(
        () -> {
          CTabItem metaTab = tabFolder.getItem(META_TAB_INDEX);
          // The look-and-feel pads tab labels with spaces, hence the trim().
          assertEquals(META_TAB, metaTab.getText().trim(), "expected the Meta-data tab");
          tabFolder.setSelection(metaTab);
          tabFolder.layout();
        });
  }

  /**
   * The Meta-data grid. Every tab of the dialog holds one, but SWTBot only looks at visible
   * controls, so with the Meta-data tab up front this one is the only grid it finds.
   */
  private SWTBotTable metaGrid(SWTBot dialog) {
    SWTBotTable grid = dialog.table();
    assertEquals(
        FORMAT_HEADER,
        grid.columns().get(FORMAT_COLUMN),
        "expected the Meta-data grid, whose column " + FORMAT_COLUMN + " is the format mask");
    return grid;
  }

  /**
   * Writes into the combo the click on a cell opened, which is what typing in it amounts to: the
   * TableView modify listener copies the text into the cell as it is entered.
   */
  private void typeIntoTheOpenComboEditor(Table table, String value) {
    AtomicReference<Combo> editor = new AtomicReference<>();
    display.syncExec(
        () -> {
          for (Control child : table.getChildren()) {
            if (child instanceof Combo combo) {
              editor.set(combo);
            }
          }
        });
    assertNotNull(editor.get(), "clicking the cell should have opened a combo editor on the grid");
    display.syncExec(() -> editor.get().setText(value));
  }

  /**
   * Clicks the empty space below the last row - the "click outside the editor" of the report. The
   * grid answers that with a new row, which is where the still-open editor's text can end up.
   */
  private void clickTheEmptyAreaUnderTheGrid(Table table) {
    display.syncExec(
        () -> {
          TableItem lastRow = table.getItem(table.getItemCount() - 1);
          Rectangle bounds = lastRow.getBounds(FIELDNAME_COLUMN);
          Event click = new Event();
          click.widget = table;
          click.button = 1;
          click.count = 1;
          click.x = bounds.x + bounds.width / 2;
          click.y = bounds.y + bounds.height + bounds.height / 2;
          assertTrue(
              table.getClientArea().contains(click.x, click.y),
              "the grid must be tall enough to have empty space under its rows");
          table.notifyListeners(SWT.MouseDown, click);
        });
  }

  private Consumer<Shell> openerFor(SelectValuesMeta meta) {
    PipelineMeta pipelineMeta = pipelineWith(meta);
    return parent -> new SelectValuesDialog(parent, new Variables(), meta, pipelineMeta).open();
  }

  private static SelectMetadataChange change(String name, String type) {
    SelectMetadataChange change = new SelectMetadataChange();
    change.setName(name);
    change.setType(type);
    return change;
  }

  private static SelectValuesMeta metaWith(SelectMetadataChange... changes) {
    SelectValuesMeta meta = new SelectValuesMeta();
    meta.getSelectOption().getMeta().addAll(Arrays.asList(changes));
    return meta;
  }

  private static PipelineMeta pipelineWith(SelectValuesMeta meta) {
    String pluginId = PluginRegistry.getInstance().getPluginId(TransformPluginType.class, meta);
    assertNotNull(pluginId, "Select values transform must be registered via HopEnvironment.init()");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(new TransformMeta(pluginId, TRANSFORM_NAME, meta));
    return pipelineMeta;
  }
}
