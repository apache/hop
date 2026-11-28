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

package org.apache.hop.airflow.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.hopgui.HopGuiEnvironment;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The Airflow DAG editor builds its four tabs with every field. Run with
 * tools/with-isolated-display.sh and -Puitest.
 */
@Tag("uitest")
class AirflowDagDefinitionLayoutUiTest extends SwtBotTestBase {

  private static final Class<?> PKG = AirflowDagDefinition.class;

  @BeforeAll
  static void init() throws Exception {
    HopGuiEnvironment.init();
    GuiRegistry registry = GuiRegistry.getInstance();
    String parentId = AirflowDagDefinitionEditor.GUI_PLUGIN_ELEMENT_PARENT_ID;
    if (registry.findGuiElements(AirflowDagDefinition.class.getName(), parentId) == null) {
      for (Field field : AirflowDagDefinition.class.getDeclaredFields()) {
        GuiWidgetElement element = field.getAnnotation(GuiWidgetElement.class);
        if (element != null) {
          registry.addGuiWidgetElement(AirflowDagDefinition.class.getName(), element, field);
        }
      }
    }
  }

  @Test
  void showsFourTabsWithEveryField() {
    AirflowDagDefinition dag = new AirflowDagDefinition("daily");
    dag.setWorkflow("${PROJECT_HOME}/check-greeting.hwf");
    dag.getParameters().add(new AirflowDagParameter("PRM_GREETING", "hello, airflow"));
    AtomicReference<Composite> parent = new AtomicReference<>();
    AtomicReference<GuiCompositeWidgets> widgets = new AtomicReference<>();
    AtomicReference<Throwable> error = new AtomicReference<>();
    withScene(
        shell -> {
          try {
            shell.setLayout(new FormLayout());
            shell.setSize(800, 600);
            Label top = new Label(shell, SWT.NONE);
            top.setText("Airflow DAG");
            Composite composite = new Composite(shell, SWT.NONE);
            composite.setLayout(new FormLayout());
            GuiCompositeWidgets compositeWidgets = new GuiCompositeWidgets(new Variables());
            compositeWidgets.createCompositeWidgets(
                dag, null, composite, AirflowDagDefinitionEditor.GUI_PLUGIN_ELEMENT_PARENT_ID, top);
            compositeWidgets.setWidgetsContents(
                dag, composite, AirflowDagDefinitionEditor.GUI_PLUGIN_ELEMENT_PARENT_ID);
            shell.layout(true, true);
            parent.set(shell);
            widgets.set(compositeWidgets);
          } catch (Throwable e) {
            error.set(e);
          }
        },
        bot -> {
          if (error.get() != null) {
            throw new AssertionError("The editor could not be built", error.get());
          }
          List<String> tabs =
              onUi(
                  () -> {
                    List<String> texts = new ArrayList<>();
                    for (CTabFolder folder : findAll(parent.get(), CTabFolder.class)) {
                      for (CTabItem item : folder.getItems()) {
                        texts.add(item.getText());
                      }
                    }
                    return texts;
                  });
          assertEquals(
              List.of(
                  BaseMessages.getString(PKG, "AirflowDag.Group.Dag"),
                  BaseMessages.getString(PKG, "AirflowDag.Group.Schedule"),
                  BaseMessages.getString(PKG, "AirflowDag.Group.Runs"),
                  BaseMessages.getString(PKG, "AirflowDag.Group.Parameters")),
              tabs);
          for (Field field : AirflowDagDefinition.class.getDeclaredFields()) {
            GuiWidgetElement element = field.getAnnotation(GuiWidgetElement.class);
            if (element != null) {
              assertNotNull(widgets.get().getWidgetsMap().get(element.id()), element.id());
            }
          }

          // What the widgets show is what OK saves
          AirflowDagDefinition read =
              onUi(
                  () -> {
                    AirflowDagDefinition copy = new AirflowDagDefinition();
                    widgets
                        .get()
                        .getWidgetsContents(
                            copy, AirflowDagDefinitionEditor.GUI_PLUGIN_ELEMENT_PARENT_ID);
                    return copy;
                  });
          assertEquals("${PROJECT_HOME}/check-greeting.hwf", read.getWorkflow());
          assertEquals(AirflowDagObjectTasks.BUNDLE_DEFAULT, read.getTasks());
          assertEquals(AirflowDagSwitch.AIRFLOW_DEFAULT, read.getCatchup());
          assertEquals(1, read.getParameters().size());
          assertEquals("PRM_GREETING", read.getParameters().get(0).getName());
          assertEquals("hello, airflow", read.getParameters().get(0).getValue());
        });
  }

  private static <T> T onUi(java.util.function.Supplier<T> supplier) {
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
