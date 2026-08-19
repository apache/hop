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

package org.apache.hop.pipeline.transforms.constant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.eclipse.swtbot.swt.finder.widgets.SWTBotTable;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * End-to-end SWTBot coverage for the Add Constants transform's {@link ConstantDialog}, kept next to
 * the transform it exercises. The dialog runs its own blocking event loop in {@code open()}, so
 * {@link SwtBotTestBase#withDialog} pumps it on the UI thread while the assertions drive it from a
 * worker thread.
 *
 * <p>The fields grid is a Hop {@link org.apache.hop.ui.core.widget.TableView}, whose cell editors
 * only materialise on click and are not addressable through SWTBot's table API. Cell content is
 * therefore staged directly on the underlying SWT {@link Table} (on the UI thread), after which the
 * real OK/Cancel buttons are clicked so the dialog's own {@code ok()}/{@code cancel()} logic is
 * what gets exercised.
 *
 * <p>Tagged {@code uitest} so it is skipped when there is no display. The default reactor run still
 * includes it on a desktop; wrap Maven with {@code tools/with-isolated-display.sh} so the dialog
 * does not steal focus.
 */
@Tag("uitest")
class ConstantDialogTest extends SwtBotTestBase {

  private static final String TRANSFORM_NAME = "constant";
  private static final String DIALOG_TITLE = "Add constants";

  // Resolved the way ConstantDialog resolves them, rather than hardcoding: these come out of the
  // shared System.Combo.* keys and are short forms ("Y"/"N"), not the words.
  private static final String YES = BaseMessages.getString(ConstantMeta.class, "System.Combo.Yes");
  private static final String NO = BaseMessages.getString(ConstantMeta.class, "System.Combo.No");

  // Grid layout: column 0 holds the row number, the ConstantField columns follow.
  private static final int NAME_COLUMN = 1;
  private static final int TYPE_COLUMN = 2;
  private static final int FORMAT_COLUMN = 3;
  private static final int LENGTH_COLUMN = 4;
  private static final int PRECISION_COLUMN = 5;
  private static final int CURRENCY_COLUMN = 6;
  private static final int DECIMAL_COLUMN = 7;
  private static final int GROUP_COLUMN = 8;
  private static final int VALUE_COLUMN = 9;
  private static final int EMPTY_STRING_COLUMN = 10;

  @Test
  void existingFieldsAreShownInTheGrid() {
    ConstantField amount = new ConstantField("amount", "Number", "1234.56");
    amount.setFieldFormat("#.##");
    amount.setFieldLength(9);
    amount.setFieldPrecision(2);
    amount.setCurrency("EUR");
    amount.setDecimal(".");
    amount.setGroup(",");
    ConstantMeta meta = metaWith(amount, new ConstantField("label", "String", "hello"));

    withDialog(
        openerFor(meta),
        bot -> {
          SWTBot dialog = dialogBot(bot);
          SWTBotTable grid = dialog.table();

          assertEquals("amount", grid.cell(0, NAME_COLUMN));
          assertEquals("Number", grid.cell(0, TYPE_COLUMN));
          assertEquals("#.##", grid.cell(0, FORMAT_COLUMN));
          assertEquals("9", grid.cell(0, LENGTH_COLUMN));
          assertEquals("2", grid.cell(0, PRECISION_COLUMN));
          assertEquals("EUR", grid.cell(0, CURRENCY_COLUMN));
          assertEquals(".", grid.cell(0, DECIMAL_COLUMN));
          assertEquals(",", grid.cell(0, GROUP_COLUMN));
          assertEquals("1234.56", grid.cell(0, VALUE_COLUMN));
          assertEquals(NO, grid.cell(0, EMPTY_STRING_COLUMN));

          assertEquals("label", grid.cell(1, NAME_COLUMN));
          assertEquals("String", grid.cell(1, TYPE_COLUMN));
          assertEquals("hello", grid.cell(1, VALUE_COLUMN));

          dialog.button(buttonLabel("System.Button.Cancel")).click();
        });
  }

  /**
   * A JSON constant survives the round trip through the dialog. The Type column has always offered
   * every registered value type, so this is the UI half of issue #2239.
   */
  @Test
  void okWritesTheEditedGridBackToMeta() {
    ConstantMeta meta = metaWith(new ConstantField("old", "String", "old value"));

    withDialog(
        openerFor(meta),
        bot -> {
          SWTBot dialog = dialogBot(bot);
          assertEquals(TRANSFORM_NAME, dialog.text(0).getText(), "transform name field");

          setCells(
              dialog.table().widget,
              Map.of(
                  NAME_COLUMN, "payload",
                  TYPE_COLUMN, "JSON",
                  VALUE_COLUMN, "{\"a\":1}",
                  LENGTH_COLUMN, "12",
                  PRECISION_COLUMN, "3",
                  EMPTY_STRING_COLUMN, NO));

          dialog.button(buttonLabel("System.Button.OK")).click();
        });

    assertEquals(1, meta.getFields().size());
    ConstantField saved = meta.getFields().get(0);
    assertEquals("payload", saved.getFieldName());
    assertEquals("JSON", saved.getFieldType());
    assertEquals("{\"a\":1}", saved.getValue());
    assertEquals(12, saved.getFieldLength());
    assertEquals(3, saved.getFieldPrecision());
    assertFalse(saved.isEmptyString());
  }

  /** Ticking "Set empty string?" forces the field to a String with no value. */
  @Test
  void okAppliesTheSetEmptyStringFlag() {
    ConstantMeta meta = metaWith(new ConstantField("old", "Integer", "42"));

    withDialog(
        openerFor(meta),
        bot -> {
          SWTBot dialog = dialogBot(bot);
          setCells(
              dialog.table().widget,
              Map.of(
                  NAME_COLUMN, "blank",
                  TYPE_COLUMN, "Integer",
                  VALUE_COLUMN, "42",
                  EMPTY_STRING_COLUMN, YES));

          dialog.button(buttonLabel("System.Button.OK")).click();
        });

    ConstantField saved = meta.getFields().get(0);
    assertTrue(saved.isEmptyString());
    assertEquals("String", saved.getFieldType(), "the empty-string flag forces the String type");
    assertEquals("", saved.getValue());
  }

  /** A non-numeric length or precision falls back to -1 rather than failing the dialog. */
  @Test
  void okFallsBackToUnsetLengthAndPrecision() {
    ConstantMeta meta = metaWith(new ConstantField("old", "String", "old value"));

    withDialog(
        openerFor(meta),
        bot -> {
          SWTBot dialog = dialogBot(bot);
          setCells(
              dialog.table().widget,
              Map.of(
                  NAME_COLUMN, "text",
                  TYPE_COLUMN, "String",
                  VALUE_COLUMN, "value",
                  LENGTH_COLUMN, "not a number",
                  PRECISION_COLUMN, ""));

          dialog.button(buttonLabel("System.Button.OK")).click();
        });

    ConstantField saved = meta.getFields().get(0);
    assertEquals(-1, saved.getFieldLength());
    assertEquals(-1, saved.getFieldPrecision());
  }

  /**
   * A row that was filled in but never named can't become an output field, so accepting the save
   * would quietly lose what was typed. The dialog has to say so and stay open instead.
   */
  @Test
  void okRefusesToSaveARowWithoutAFieldName() {
    ConstantMeta meta = metaWith(new ConstantField("kept", "String", "keep me"));

    withDialog(
        openerFor(meta),
        bot -> {
          SWTBot dialog = dialogBot(bot);
          setCells(dialog.table().widget, Map.of(NAME_COLUMN, "", VALUE_COLUMN, "no name typed"));

          dialog.button(buttonLabel("System.Button.OK")).click();

          // The save is refused with an explanation and the dialog stays open.
          SWTBot warning = bot.shell("Missing field name").activate().bot();
          warning.button(buttonLabel("System.Button.OK")).click();
          dialogBot(bot).button(buttonLabel("System.Button.Cancel")).click();
        });

    assertEquals(1, meta.getFields().size());
    assertEquals("kept", meta.getFields().get(0).getFieldName(), "nothing may have been saved");
  }

  @Test
  void cancelLeavesMetaUntouched() {
    ConstantMeta meta = metaWith(new ConstantField("keep", "String", "keep me"));

    withDialog(
        openerFor(meta),
        bot -> {
          SWTBot dialog = dialogBot(bot);
          setCells(
              dialog.table().widget,
              Map.of(NAME_COLUMN, "discarded", VALUE_COLUMN, "discarded value"));

          dialog.button(buttonLabel("System.Button.Cancel")).click();
        });

    assertEquals(1, meta.getFields().size());
    assertEquals("keep", meta.getFields().get(0).getFieldName());
    assertEquals("keep me", meta.getFields().get(0).getValue());
  }

  private SWTBot dialogBot(SWTBot bot) {
    return bot.shell(DIALOG_TITLE).activate().bot();
  }

  private Consumer<Shell> openerFor(ConstantMeta meta) {
    PipelineMeta pipelineMeta = pipelineWith(meta);
    return parent -> new ConstantDialog(parent, new Variables(), meta, pipelineMeta).open();
  }

  /**
   * Stages cell text on the grid's first row from the UI thread. The dialog's TableView installs
   * its editors on click, so there is no SWTBot path to type into a cell; writing the item text
   * leaves the same state the editors would, and {@code ok()} then reads it for real.
   */
  private void setCells(Table table, Map<Integer, String> valuesByColumn) {
    display.syncExec(
        () -> valuesByColumn.forEach((column, value) -> table.getItem(0).setText(column, value)));
  }

  private static ConstantMeta metaWith(ConstantField... fields) {
    ConstantMeta meta = new ConstantMeta();
    meta.getFields().addAll(Arrays.asList(fields));
    return meta;
  }

  private static PipelineMeta pipelineWith(ConstantMeta meta) {
    String pluginId = PluginRegistry.getInstance().getPluginId(TransformPluginType.class, meta);
    assertNotNull(pluginId, "Add Constants transform must be registered via HopEnvironment.init()");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(new TransformMeta(pluginId, TRANSFORM_NAME, meta));
    return pipelineMeta;
  }
}
