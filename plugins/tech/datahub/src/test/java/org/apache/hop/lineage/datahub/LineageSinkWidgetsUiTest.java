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

package org.apache.hop.lineage.datahub;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.lineage.metadata.LineageSink;
import org.apache.hop.lineage.openlineage.OpenLineageDeliveryConfiguration;
import org.apache.hop.lineage.openlineage.OpenLineageSinkConfiguration;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.hopgui.HopGuiEnvironment;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** The options of the lineage sink types, as the lineage sink editor shows them: in tabs. */
@Tag("uitest")
class LineageSinkWidgetsUiTest extends SwtBotTestBase {

  @BeforeAll
  static void init() throws Exception {
    HopGuiEnvironment.init();
    // The widgets of the sink types, with the inherited delivery options, as Hop GUI registers
    // them from the annotations.
    for (Class<?> type :
        List.of(OpenLineageSinkConfiguration.class, DataHubSinkConfiguration.class)) {
      GuiRegistry registry = GuiRegistry.getInstance();
      if (registry.findGuiElements(type.getName(), LineageSink.GUI_PLUGIN_ELEMENT_PARENT_ID)
          != null) {
        continue;
      }
      for (Class<?> c = type; c != null; c = c.getSuperclass()) {
        for (Field field : c.getDeclaredFields()) {
          GuiWidgetElement element = field.getAnnotation(GuiWidgetElement.class);
          if (element != null) {
            registry.addGuiWidgetElement(type.getName(), element, field);
          }
        }
      }
    }
  }

  @Test
  void anOpenLineageSinkHasCollectorLineageAndDeliveryTabs() {
    OpenLineageSinkConfiguration sink = new OpenLineageSinkConfiguration();
    sink.setUrl("${MARQUEZ_URL}");
    sink.setMaxRetries("7");
    sink.setOverflowPolicy(OpenLineageDeliveryConfiguration.Overflow.BLOCK);

    OpenLineageSinkConfiguration read = new OpenLineageSinkConfiguration();
    List<String> tabs = showAndReadBack(sink, read);

    assertEquals(
        List.of(
            message(OpenLineageSinkConfiguration.class, "Group.Collector"),
            message(OpenLineageSinkConfiguration.class, "Group.Lineage"),
            message(OpenLineageSinkConfiguration.class, "Group.Delivery")),
        tabs);
    assertEquals("${MARQUEZ_URL}", read.getUrl());
    assertEquals("7", read.getMaxRetries());
    assertEquals(OpenLineageDeliveryConfiguration.Overflow.BLOCK, read.getOverflowPolicy());
    assertTrue(read.isParentRuns());
    assertTrue(read.isFileSpecNaming());
  }

  @Test
  void aDataHubSinkHasDataHubAndDeliveryTabs() {
    DataHubSinkConfiguration sink = new DataHubSinkConfiguration();
    sink.setPlatformInstance("emea");
    sink.setDatasetSchema(DataHubSinkConfig.SchemaPolicy.ALWAYS);
    sink.setLifecycle(true);
    sink.setSpoolDirectory("/var/spool/hop");

    DataHubSinkConfiguration read = new DataHubSinkConfiguration();
    List<String> tabs = showAndReadBack(sink, read);

    assertEquals(
        List.of(
            BaseMessages.getString(
                DataHubSinkConfiguration.class, "DataHubSinkConfiguration.Group.DataHub"),
            message(OpenLineageSinkConfiguration.class, "Group.Delivery")),
        tabs);
    assertEquals("emea", read.getPlatformInstance());
    assertEquals(DataHubSinkConfig.SchemaPolicy.ALWAYS, read.getDatasetSchema());
    assertTrue(read.isLifecycle());
    assertEquals("/var/spool/hop", read.getSpoolDirectory());
  }

  private static String message(Class<?> type, String key) {
    return BaseMessages.getString(type, "OpenLineageSinkConfiguration." + key);
  }

  /**
   * Shows the options of {@code shown} as the editor does, reads them into {@code read}, and gives
   * the titles of the tabs.
   */
  private List<String> showAndReadBack(Object shown, Object read) {
    AtomicReference<Composite> parent = new AtomicReference<>();
    AtomicReference<GuiCompositeWidgets> widgets = new AtomicReference<>();
    List<String> tabs = new ArrayList<>();
    withScene(
        shell -> {
          shell.setLayout(new FormLayout());
          shell.setSize(800, 600);
          GuiCompositeWidgets compositeWidgets = new GuiCompositeWidgets(new Variables());
          compositeWidgets.createCompositeWidgets(
              shown, null, shell, LineageSink.GUI_PLUGIN_ELEMENT_PARENT_ID, null);
          compositeWidgets.setWidgetsContents(
              shown, shell, LineageSink.GUI_PLUGIN_ELEMENT_PARENT_ID);
          shell.layout(true, true);
          parent.set(shell);
          widgets.set(compositeWidgets);
        },
        bot ->
            onUi(
                () -> {
                  for (CTabFolder folder : findAll(parent.get(), CTabFolder.class)) {
                    for (CTabItem item : folder.getItems()) {
                      tabs.add(item.getText().trim());
                    }
                  }
                  widgets.get().getWidgetsContents(read, LineageSink.GUI_PLUGIN_ELEMENT_PARENT_ID);
                  return null;
                }));
    return tabs;
  }

  private static <T> T onUi(Supplier<T> supplier) {
    AtomicReference<T> result = new AtomicReference<>();
    Display.getDefault().syncExec(() -> result.set(supplier.get()));
    return result.get();
  }

  private static <T extends Control> List<T> findAll(Composite parent, Class<T> type) {
    List<T> found = new ArrayList<>();
    for (Control child : parent.getChildren()) {
      if (type.isInstance(child)) {
        found.add(type.cast(child));
      }
      if (child instanceof Composite composite) {
        found.addAll(findAll(composite, type));
      }
    }
    return found;
  }
}
