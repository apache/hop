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

package org.apache.hop.pipeline.transforms.normaliser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.eclipse.swtbot.swt.finder.widgets.SWTBotButton;
import org.eclipse.swtbot.swt.finder.widgets.SWTBotTable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** SWTBot coverage for the grouped {@link NormaliserDialog} (#9398). */
@Tag("uitest")
class NormaliserDialogTest extends SwtBotTestBase {

  private static final String TRANSFORM_NAME = "row normaliser";

  private static final String SHELL_TITLE =
      BaseMessages.getString(NormaliserMeta.class, "NormaliserDialog.Shell.Title");

  /**
   * The dialog builds its boxes from the annotated widgets on {@link NormaliserMeta}. The unit-test
   * JVM does not always run the {@code GuiPluginType} scan, so register them the way it does.
   */
  @BeforeAll
  static void registerMetaWidgets() {
    GuiRegistry registry = GuiRegistry.getInstance();
    if (registry.findGuiElements(
            NormaliserMeta.class.getName(), NormaliserMeta.GUI_PLUGIN_ELEMENT_PARENT_ID)
        != null) {
      return;
    }
    for (Field field : NormaliserMeta.class.getDeclaredFields()) {
      GuiWidgetElement element = field.getAnnotation(GuiWidgetElement.class);
      if (element != null) {
        registry.addGuiWidgetElement(NormaliserMeta.class.getName(), element, field);
      }
    }
  }

  /** Opening the dialog and pressing OK keeps every setting of the "all fields" mode. */
  @Test
  void okKeepsTheTypeValueAndKeyFields() {
    NormaliserMeta meta = allFieldsMeta();

    withDialog(
        parent -> new NormaliserDialog(parent, new Variables(), meta, pipelineWith(meta)).open(),
        bot -> {
          SWTBot dialog = bot.shell(SHELL_TITLE).activate().bot();
          assertEquals(
              "Month", dialog.textWithLabel(label("NormaliserDialog.TypeField.Label")).getText());
          assertEquals(
              "Sales", dialog.textWithLabel(label("NormaliserDialog.ValueField.Label")).getText());
          assertEquals("Region", keyTable(dialog).cell(0, 1));
          ok(dialog);
        });

    assertEquals("Month", meta.getTypeField());
    assertEquals("Sales", meta.getValueField());
    assertEquals(
        List.of("Region", "Year"),
        meta.getKeyFields().stream().map(NormaliserKeyField::getName).toList());
    assertTrue(meta.getNormaliserFields().isEmpty());
  }

  /** Listed fields come back unchanged after OK. */
  @Test
  void okKeepsTheListedFields() {
    NormaliserMeta meta = new NormaliserMeta();
    meta.setDefault();
    meta.setNormaliserFields(
        new ArrayList<>(
            List.of(field("PR1_SL", "Product1", "Sales"), field("PR1_NR", "Product1", "Number"))));

    withDialog(
        parent -> new NormaliserDialog(parent, new Variables(), meta, pipelineWith(meta)).open(),
        bot -> ok(bot.shell(SHELL_TITLE).activate().bot()));

    assertEquals(
        List.of(field("PR1_SL", "Product1", "Sales"), field("PR1_NR", "Product1", "Number")),
        meta.getNormaliserFields());
  }

  /** An edited value field is stored. */
  @Test
  void okStoresAnEditedValueField() {
    NormaliserMeta meta = allFieldsMeta();

    withDialog(
        parent -> new NormaliserDialog(parent, new Variables(), meta, pipelineWith(meta)).open(),
        bot -> {
          SWTBot dialog = bot.shell(SHELL_TITLE).activate().bot();
          dialog.textWithLabel(label("NormaliserDialog.ValueField.Label")).setText("Amount");
          ok(dialog);
        });

    assertEquals("Amount", meta.getValueField());
  }

  /** The key fields only count without listed fields, so their table follows the fields table. */
  @Test
  void theKeyFieldsAreOnlyEnabledWhileNoFieldsAreListed() {
    NormaliserMeta empty = allFieldsMeta();
    withDialog(
        parent -> new NormaliserDialog(parent, new Variables(), empty, pipelineWith(empty)).open(),
        bot -> {
          SWTBot dialog = bot.shell(SHELL_TITLE).activate().bot();
          assertTrue(keyTable(dialog).isEnabled(), "no fields listed: key fields in use");
          ok(dialog);
        });

    NormaliserMeta listed = allFieldsMeta();
    listed.setNormaliserFields(new ArrayList<>(List.of(field("a", "A", "value"))));
    withDialog(
        parent ->
            new NormaliserDialog(parent, new Variables(), listed, pipelineWith(listed)).open(),
        bot -> {
          SWTBot dialog = bot.shell(SHELL_TITLE).activate().bot();
          assertFalse(keyTable(dialog).isEnabled(), "fields listed: key fields not used");
          ok(dialog);
        });
  }

  /** The OK button stays below the fields table, the last box, rather than on top of it. */
  @Test
  void theButtonsStayBelowTheLastTable() {
    NormaliserMeta meta = allFieldsMeta();
    AtomicReference<Rectangle> table = new AtomicReference<>();
    AtomicReference<Rectangle> button = new AtomicReference<>();

    withDialog(
        parent -> new NormaliserDialog(parent, new Variables(), meta, pipelineWith(meta)).open(),
        bot -> {
          SWTBot dialog = bot.shell(SHELL_TITLE).activate().bot();
          SWTBotTable fields = dialog.table(1);
          SWTBotButton okButton = dialog.button(buttonLabel("System.Button.OK"));
          display.syncExec(
              () -> {
                table.set(onDisplay(fields.widget));
                button.set(onDisplay(okButton.widget));
              });
          ok(dialog);
        });

    assertNotNull(table.get());
    assertTrue(
        table.get().y + table.get().height <= button.get().y,
        "fields table " + table.get() + " must end above the OK button " + button.get());
  }

  private static Rectangle onDisplay(Control control) {
    return control.getDisplay().map(control.getParent(), null, control.getBounds());
  }

  private static SWTBotTable keyTable(SWTBot dialog) {
    return dialog.table(0);
  }

  private static void ok(SWTBot dialog) {
    dialog.button(buttonLabel("System.Button.OK")).click();
  }

  private static String label(String key) {
    return BaseMessages.getString(NormaliserMeta.class, key);
  }

  private static NormaliserMeta allFieldsMeta() {
    NormaliserMeta meta = new NormaliserMeta();
    meta.setDefault();
    meta.setTypeField("Month");
    meta.setValueField("Sales");
    meta.setKeyFields(
        new ArrayList<>(List.of(new NormaliserKeyField("Region"), new NormaliserKeyField("Year"))));
    return meta;
  }

  private static NormaliserField field(String name, String type, String norm) {
    NormaliserField field = new NormaliserField();
    field.setName(name);
    field.setValue(type);
    field.setNorm(norm);
    return field;
  }

  private static PipelineMeta pipelineWith(NormaliserMeta meta) {
    String pluginId = PluginRegistry.getInstance().getPluginId(TransformPluginType.class, meta);
    assertNotNull(pluginId, "Row normaliser transform plugin must be registered");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(new TransformMeta(pluginId, TRANSFORM_NAME, meta));
    return pipelineMeta;
  }
}
