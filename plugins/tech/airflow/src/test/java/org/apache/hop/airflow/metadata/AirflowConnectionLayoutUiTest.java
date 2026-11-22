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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
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
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swtbot.swt.finder.utils.SWTUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The Airflow connection editor: two tabs, and only the credentials of the chosen authentication
 * are shown. Run with tools/with-isolated-display.sh and -Puitest.
 */
@Tag("uitest")
class AirflowConnectionLayoutUiTest extends SwtBotTestBase {

  private static final Class<?> PKG = AirflowConnection.class;

  @BeforeAll
  static void init() throws Exception {
    HopGuiEnvironment.init();
    GuiRegistry registry = GuiRegistry.getInstance();
    String parentId = AirflowConnectionEditor.GUI_PLUGIN_ELEMENT_PARENT_ID;
    if (registry.findGuiElements(AirflowConnection.class.getName(), parentId) == null) {
      for (Field field : AirflowConnection.class.getDeclaredFields()) {
        GuiWidgetElement element = field.getAnnotation(GuiWidgetElement.class);
        if (element != null) {
          registry.addGuiWidgetElement(AirflowConnection.class.getName(), element, field);
        }
      }
      for (Method method : AirflowConnection.class.getDeclaredMethods()) {
        GuiWidgetElement element = method.getAnnotation(GuiWidgetElement.class);
        if (element != null) {
          registry.addGuiWidgetElement(
              element,
              method,
              AirflowConnection.class.getName(),
              AirflowConnection.class.getClassLoader());
        }
      }
    }
  }

  @Test
  void showsTwoTabsAndOnlyTheCredentialsOfTheChosenAuthentication() {
    AirflowConnection connection = new AirflowConnection();
    AtomicReference<Composite> parent = new AtomicReference<>();
    AtomicReference<GuiCompositeWidgets> widgets = new AtomicReference<>();
    withScene(
        shell -> {
          shell.setLayout(new FormLayout());
          shell.setSize(700, 420);
          Label top = new Label(shell, SWT.NONE);
          top.setText("Airflow");
          GuiCompositeWidgets compositeWidgets =
              GuiCompositeWidgets.addScrolledComposite(
                  shell,
                  new Variables(),
                  top,
                  null,
                  AirflowConnectionEditor.GUI_PLUGIN_ELEMENT_PARENT_ID,
                  connection);
          // The editor passes these calls on to the connection.
          connection.widgetsPopulated(compositeWidgets);
          shell.layout(true, true);
          parent.set(shell);
          widgets.set(compositeWidgets);
        },
        bot -> {
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
                  BaseMessages.getString(PKG, "AirflowConnection.Group.Server"),
                  BaseMessages.getString(PKG, "AirflowConnection.Group.Authentication")),
              tabs);

          // Username and password is the default.
          assertTrue(visible(widgets.get(), AirflowConnection.WIDGET_USERNAME));
          assertTrue(visible(widgets.get(), AirflowConnection.WIDGET_PASSWORD));
          assertFalse(visible(widgets.get(), AirflowConnection.WIDGET_TOKEN));

          chooseAuthentication(connection, widgets.get(), AirflowAuthenticationType.TOKEN);
          assertFalse(visible(widgets.get(), AirflowConnection.WIDGET_USERNAME));
          assertFalse(visible(widgets.get(), AirflowConnection.WIDGET_PASSWORD));
          assertTrue(visible(widgets.get(), AirflowConnection.WIDGET_TOKEN));

          if (Boolean.getBoolean("airflow.gui.capture")) {
            Display.getDefault()
                .syncExec(
                    () -> {
                      CTabFolder folder = findAll(parent.get(), CTabFolder.class).get(0);
                      folder.setSelection(1);
                      parent.get().layout(true, true);
                    });
            bot.sleep(250);
            Display.getDefault()
                .syncExec(() -> SWTUtils.captureScreenshot("target/airflow-connection-token.png"));
          }

          chooseAuthentication(connection, widgets.get(), AirflowAuthenticationType.NONE);
          assertFalse(visible(widgets.get(), AirflowConnection.WIDGET_USERNAME));
          assertFalse(visible(widgets.get(), AirflowConnection.WIDGET_TOKEN));
        });
  }

  private static void chooseAuthentication(
      AirflowConnection connection, GuiCompositeWidgets widgets, AirflowAuthenticationType type) {
    Display.getDefault()
        .syncExec(
            () -> {
              Combo combo =
                  (Combo) widgets.getWidgetsMap().get(AirflowConnection.WIDGET_AUTHENTICATION_TYPE);
              combo.setText(type.getDescription());
              connection.widgetModified(widgets, AirflowConnection.WIDGET_AUTHENTICATION_TYPE);
            });
  }

  private static boolean visible(GuiCompositeWidgets widgets, String id) {
    return onUi(() -> widgets.getWidgetsMap().get(id).getVisible());
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
