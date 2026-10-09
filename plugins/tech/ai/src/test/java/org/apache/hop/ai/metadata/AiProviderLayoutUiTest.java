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

package org.apache.hop.ai.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** The AI Provider editor uses the grouped layout: tabs that fill the editor and scroll. */
@Tag("uitest")
class AiProviderLayoutUiTest extends SwtBotTestBase {

  private static final Class<?> PKG = AiProviderEditor.class;

  @BeforeAll
  static void init() throws Exception {
    HopGuiEnvironment.init();
    // The widgets of the provider, as Hop GUI registers them from the annotations.
    GuiRegistry registry = GuiRegistry.getInstance();
    if (registry.findGuiElements(AiProvider.class.getName(), AiProvider.GUI_WIDGETS_PARENT_ID)
        == null) {
      for (Field field : AiProvider.class.getDeclaredFields()) {
        GuiWidgetElement element = field.getAnnotation(GuiWidgetElement.class);
        if (element != null) {
          registry.addGuiWidgetElement(AiProvider.class.getName(), element, field);
        }
      }
    }
  }

  @Test
  void theOptionsAreOnThreeTabsWithStructuredAnswersOnTheModelTab() {
    AtomicReference<Composite> parent = new AtomicReference<>();
    withScene(
        shell -> {
          shell.setLayout(new FormLayout());
          shell.setSize(800, 500);
          Label top = new Label(shell, SWT.NONE);
          top.setText("Provider");
          GuiCompositeWidgets.addScrolledComposite(
              shell,
              new Variables(),
              top,
              null,
              AiProvider.GUI_WIDGETS_PARENT_ID,
              new AiProvider());
          shell.layout(true, true);
          parent.set(shell);
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
                  BaseMessages.getString(PKG, "AiProviderEditor.Group.Connection"),
                  BaseMessages.getString(PKG, "AiProviderEditor.Group.Model"),
                  BaseMessages.getString(PKG, "AiProviderEditor.Models.Label")),
              tabs);
          String structured = BaseMessages.getString(PKG, "AiProvider.StructuredAnswers.Label");
          assertTrue(
              onUi(
                  () ->
                      findAll(parent.get(), Control.class).stream()
                          .anyMatch(
                              control ->
                                  (control instanceof Label label
                                          && label.getText().startsWith(structured))
                                      || (control instanceof Button button
                                          && button.getText().startsWith(structured)))),
              "the Structured answers option is shown");
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
