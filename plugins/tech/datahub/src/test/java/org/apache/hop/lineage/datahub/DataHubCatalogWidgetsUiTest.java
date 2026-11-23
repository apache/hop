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
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.datacatalog.DataCatalog;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.hopgui.HopGuiEnvironment;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.TableItem;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** The database platform instances of a DataHub data catalog, as the catalog editor shows them. */
@Tag("uitest")
class DataHubCatalogWidgetsUiTest extends SwtBotTestBase {

  @BeforeAll
  static void init() throws Exception {
    HopGuiEnvironment.init();
    GuiRegistry registry = GuiRegistry.getInstance();
    String type = DataHubDataCatalog.class.getName();
    if (registry.findGuiElements(type, DataCatalog.GUI_PLUGIN_ELEMENT_PARENT_ID) == null) {
      for (Field field : DataHubDataCatalog.class.getDeclaredFields()) {
        GuiWidgetElement element = field.getAnnotation(GuiWidgetElement.class);
        if (element != null) {
          registry.addGuiWidgetElement(type, element, field);
        }
      }
    }
  }

  @Test
  void theInstancesAreATableOfConnectionsAndInstances() {
    DataHubDataCatalog shown = new DataHubDataCatalog();
    shown.setUrl("${DATAHUB_URL}");
    shown.getDatabaseInstances().add(new DataHubDatabaseInstance("sales", "warehouse"));
    DataHubDataCatalog read = new DataHubDataCatalog();
    AtomicReference<GuiCompositeWidgets> widgets = new AtomicReference<>();
    AtomicReference<List<String>> headers = new AtomicReference<>();

    withScene(
        shell -> {
          shell.setLayout(new FormLayout());
          shell.setSize(900, 600);
          // As the data catalog editor builds them: a flat form, no groups
          GuiCompositeWidgets compositeWidgets = new GuiCompositeWidgets(new Variables());
          compositeWidgets.createCompositeWidgets(
              shown, null, shell, DataCatalog.GUI_PLUGIN_ELEMENT_PARENT_ID, null);
          compositeWidgets.setWidgetsContents(
              shown, shell, DataCatalog.GUI_PLUGIN_ELEMENT_PARENT_ID);
          shell.layout(true, true);
          widgets.set(compositeWidgets);
        },
        bot -> {
          Display.getDefault()
              .syncExec(
                  () -> {
                    TableView table =
                        (TableView) widgets.get().getWidgetsMap().get("databaseInstances");
                    assertNotNull(table);
                    headers.set(
                        List.of(
                            table.getTable().getColumn(1).getText().trim(),
                            table.getTable().getColumn(2).getText().trim()));
                    assertEquals("sales", table.getTable().getItem(0).getText(1));
                    assertEquals("warehouse", table.getTable().getItem(0).getText(2));
                    TableItem added = new TableItem(table.getTable(), SWT.NONE);
                    added.setText(1, "crm");
                    added.setText(2, "${CRM_INSTANCE}");
                    widgets
                        .get()
                        .getWidgetsContents(read, DataCatalog.GUI_PLUGIN_ELEMENT_PARENT_ID);
                  });
        });

    assertEquals(List.of("Relational connection", "Platform instance"), headers.get());
    assertEquals("${DATAHUB_URL}", read.getUrl());
    assertEquals(2, read.getDatabaseInstances().size());
    assertEquals("sales", read.getDatabaseInstances().get(0).getConnection());
    assertEquals("warehouse", read.getDatabaseInstances().get(0).getPlatformInstance());
    assertEquals("crm", read.getDatabaseInstances().get(1).getConnection());
    assertEquals("${CRM_INSTANCE}", read.getDatabaseInstances().get(1).getPlatformInstance());
  }
}
