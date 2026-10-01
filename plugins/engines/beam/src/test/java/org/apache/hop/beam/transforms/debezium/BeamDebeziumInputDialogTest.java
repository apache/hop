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

package org.apache.hop.beam.transforms.debezium;

import static org.eclipse.swtbot.swt.finder.matchers.WidgetMatcherFactory.widgetOfType;
import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Field;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("uitest")
class BeamDebeziumInputDialogTest extends SwtBotTestBase {
  @BeforeAll
  static void registerWidgets() {
    GuiRegistry registry = GuiRegistry.getInstance();
    if (registry.findGuiElements(
            BeamDebeziumInputMeta.class.getName(),
            BeamDebeziumInputMeta.GUI_PLUGIN_ELEMENT_PARENT_ID)
        != null) {
      return;
    }
    for (Field field : BeamDebeziumInputMeta.class.getDeclaredFields()) {
      GuiWidgetElement element = field.getAnnotation(GuiWidgetElement.class);
      if (element != null) {
        registry.addGuiWidgetElement(BeamDebeziumInputMeta.class.getName(), element, field);
      }
    }
  }

  @Test
  void okPersistsEveryAnnotatedConnectionAndSourceOption() {
    assertDoesNotThrow(
        () -> Class.forName("org.apache.hop.beam.transforms.debezium.BeamDebeziumInputDialog"));
    BeamDebeziumInputMeta meta = new BeamDebeziumInputMeta();
    withDialog(
        parent -> open(parent, meta),
        bot -> {
          SWTBot dialog = bot.shell("Beam Debezium input").activate().bot();
          display.syncExec(
              () -> {
                org.apache.hop.ui.core.widget.ComboVar combo =
                    (org.apache.hop.ui.core.widget.ComboVar)
                        dialog.widget(widgetOfType(org.apache.hop.ui.core.widget.ComboVar.class));
                assertArrayEquals(new String[] {"PostgreSQL", "Custom"}, combo.getItems());
                combo.setText("Custom");
              });
          dialog.textWithLabel("Custom connector class").setText("${CDC_CONNECTOR_CLASS}");
          dialog.textWithLabel("Hostname").setText("${CDC_HOST}");
          dialog.textWithLabel("Port").setText("5433");
          dialog.textWithLabel("Username").setText("cdc");
          dialog.textWithLabel("Password").setText("${CDC_PASSWORD}");
          selectTab(dialog, "Connector properties");
          dialog
              .textWithLabel("Connector properties (JSON)")
              .setText("{\"database.dbname\":\"orders\"}");
          selectTab(dialog, "Output and limits");
          dialog.textWithLabel("JSON field").setText("change");
          dialog.textWithLabel("Maximum records").setText("12");
          dialog.textWithLabel("Maximum run time (ms)").setText("2000");
          dialog.textWithLabel("Polling timeout (ms)").setText("25");
          dialog.button(buttonLabel("System.Button.OK")).click();
        });
    assertEquals("Custom", meta.getConnector());
    assertEquals("${CDC_CONNECTOR_CLASS}", meta.getConnectorClass());
    assertEquals("${CDC_HOST}", meta.getHostname());
    assertEquals("5433", meta.getPort());
    assertEquals("cdc", meta.getUsername());
    assertEquals("${CDC_PASSWORD}", meta.getPassword());
    assertEquals("{\"database.dbname\":\"orders\"}", meta.getConnectorProperties());
    assertEquals("change", meta.getJsonField());
    assertEquals("12", meta.getMaxRecords());
    assertEquals("2000", meta.getMaxTimeMs());
    assertEquals("25", meta.getPollingTimeoutMs());
  }

  @Test
  void cancelDoesNotPersistEditsOrChangedFlag() {
    BeamDebeziumInputMeta meta = new BeamDebeziumInputMeta();
    meta.setChanged(false);
    withDialog(
        parent -> open(parent, meta),
        bot -> {
          SWTBot dialog = bot.shell("Beam Debezium input").activate().bot();
          dialog.textWithLabel("Hostname").setText("discarded");
          dialog.button(buttonLabel("System.Button.Cancel")).click();
        });
    assertEquals("localhost", meta.getHostname());
    assertFalse(meta.hasChanged());
  }

  private static void open(Shell parent, BeamDebeziumInputMeta meta) {
    String id = PluginRegistry.getInstance().getPluginId(TransformPluginType.class, meta);
    assertEquals("BeamDebeziumInput", id);
    PipelineMeta pipeline = new PipelineMeta();
    pipeline.addTransform(new TransformMeta(id, "CDC", meta));
    assertDoesNotThrow(
        () -> {
          Class<?> dialogType = Class.forName(meta.getDialogClassName());
          BaseTransformDialog dialog =
              (BaseTransformDialog)
                  dialogType
                      .getConstructor(
                          Shell.class,
                          IVariables.class,
                          BeamDebeziumInputMeta.class,
                          PipelineMeta.class)
                      .newInstance(parent, new Variables(), meta, pipeline);
          dialog.open();
        });
  }

  private static void selectTab(SWTBot bot, String title) {
    display.syncExec(
        () -> {
          CTabFolder folder = (CTabFolder) bot.widget(widgetOfType(CTabFolder.class));
          for (CTabItem item : folder.getItems()) {
            if (title.equals(item.getText().trim())) {
              folder.setSelection(item);
              folder.layout();
              return;
            }
          }
          fail("Missing tab " + title);
        });
  }
}
