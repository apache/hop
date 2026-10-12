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

package org.apache.hop.ai.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.hop.ai.metadata.AiProvider;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGuiEnvironment;
import org.apache.hop.ui.hopgui.perspective.configuration.tabs.ConfigPluginOptionsTab;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Link;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Configuration → Plugins → AI Assistant, built from its annotations as Hop GUI builds it. */
@Tag("uitest")
class HopAiConfigOptionPluginUiTest extends SwtBotTestBase {

  private static final String PARENT = ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID;

  private String originalProvider;

  @BeforeAll
  static void registerWidgets() throws Exception {
    HopGuiEnvironment.init();
    GuiRegistry registry = GuiRegistry.getInstance();
    String className = HopAiConfigOptionPlugin.class.getName();
    if (registry.findGuiElements(className, PARENT) != null) {
      return;
    }
    // The registry appends without checking for duplicates, so only when the scan did not.
    for (Field field : HopAiConfigOptionPlugin.class.getDeclaredFields()) {
      GuiWidgetElement element = field.getAnnotation(GuiWidgetElement.class);
      if (element != null) {
        registry.addGuiWidgetElement(className, element, field);
      }
    }
    for (Method method : HopAiConfigOptionPlugin.class.getDeclaredMethods()) {
      GuiWidgetElement element = method.getAnnotation(GuiWidgetElement.class);
      if (element != null) {
        registry.addGuiWidgetElement(
            element, method, className, HopAiConfigOptionPlugin.class.getClassLoader());
      }
    }
  }

  @BeforeEach
  void rememberProvider() {
    originalProvider = HopAiConfigSingleton.getConfig().getDefaultProviderName();
    HopAiConfigSingleton.getConfig().setDefaultProviderName("ollama");
  }

  @AfterEach
  void restoreProvider() {
    HopAiConfigSingleton.getConfig().setDefaultProviderName(originalProvider);
  }

  @Test
  void aDefaultProviderTheOpenProjectDoesNotHaveIsPointedOut() throws Exception {
    MemoryMetadataProvider projectA = new MemoryMetadataProvider();
    AiProvider ollama = new AiProvider();
    ollama.setName("ollama");
    projectA.getSerializer(AiProvider.class).save(ollama);
    MemoryMetadataProvider projectB = new MemoryMetadataProvider();
    AtomicReference<GuiCompositeWidgets> widgets = new AtomicReference<>();

    withScene(
        shell -> widgets.set(build(shell)),
        bot -> {
          onUi(
              () -> {
                HopAiConfigOptionPlugin.reloadProviders(widgets.get(), projectA);
                return null;
              });
          MetaSelectionLine<?> line = providerLine(widgets.get());
          assertEquals("ollama", onUi(line::getText));
          assertFalse(onUi(() -> warning(widgets.get()).isVisible()));

          // Another project is opened: its providers are listed, and the name it lacks is noted.
          onUi(
              () -> {
                HopAiConfigOptionPlugin.reloadProviders(widgets.get(), projectB);
                return null;
              });
          assertEquals(0, onUi(line::getItemCount));
          assertEquals("ollama", onUi(line::getText), "the setting itself stays");
          assertTrue(onUi(() -> warning(widgets.get()).isVisible()));
          assertTrue(onUi(() -> warning(widgets.get()).getText()).contains("'ollama'"));

          onUi(
              () -> {
                HopAiConfigOptionPlugin.reloadProviders(widgets.get(), projectA);
                return null;
              });
          assertFalse(onUi(() -> warning(widgets.get()).isVisible()));
        });
  }

  @Test
  void withoutADefaultProviderThereIsNoNoteAndNoEmptyRow() {
    HopAiConfigSingleton.getConfig().setDefaultProviderName("");
    AtomicReference<GuiCompositeWidgets> widgets = new AtomicReference<>();
    withScene(
        shell -> widgets.set(build(shell, "")),
        bot -> {
          assertFalse(onUi(() -> warning(widgets.get()).isVisible()));
          assertEquals(
              0,
              onUi(
                  () ->
                      ((org.eclipse.swt.layout.FormData) warning(widgets.get()).getLayoutData())
                          .height));
        });
  }

  @Test
  void contextFilesShowsAnExample() {
    AtomicReference<GuiCompositeWidgets> widgets = new AtomicReference<>();
    withScene(
        shell -> widgets.set(build(shell)),
        bot -> {
          TextVar files =
              (TextVar)
                  widgets.get().getWidgetsMap().get(HopAiConfigOptionPlugin.WIDGET_CONTEXT_FILES);
          assertNotNull(
              onUi(() -> files.getTextWidget().getData(HopAiConfigOptionPlugin.HINT_DATA_KEY)));
          assertNotNull(
              widgets.get().getWidgetsMap().get("0510-ai-add-context-file"),
              "the Add a file button");
        });
  }

  @Test
  void theVariableIconsLineUpWithTheProvider() {
    AtomicReference<GuiCompositeWidgets> widgets = new AtomicReference<>();
    withScene(
        shell -> widgets.set(build(shell)),
        bot -> {
          int provider = onUi(() -> iconX(providerLine(widgets.get()).getwCombo()));
          int notes =
              onUi(
                  () ->
                      iconX(
                          (Composite)
                              widgets
                                  .get()
                                  .getWidgetsMap()
                                  .get(HopAiConfigOptionPlugin.WIDGET_EXTRA_CONTEXT)));
          int files =
              onUi(
                  () ->
                      iconX(
                          (Composite)
                              widgets
                                  .get()
                                  .getWidgetsMap()
                                  .get(HopAiConfigOptionPlugin.WIDGET_CONTEXT_FILES)));
          assertTrue(Math.abs(provider - notes) <= 1, provider + " vs " + notes);
          assertTrue(Math.abs(provider - files) <= 1, provider + " vs " + files);
        });
  }

  // Helpers

  /** The page as ConfigPluginOptionsTab builds it, without the listener that saves changes. */
  private static GuiCompositeWidgets build(org.eclipse.swt.widgets.Shell shell) {
    return build(shell, "ollama");
  }

  private static GuiCompositeWidgets build(
      org.eclipse.swt.widgets.Shell shell, String defaultProvider) {
    shell.setLayout(new FormLayout());
    shell.setSize(1000, 700);
    Composite composite = new Composite(shell, org.eclipse.swt.SWT.NONE);
    composite.setLayout(new FormLayout());
    composite.setLayoutData(new org.apache.hop.ui.core.FormDataBuilder().fullSize().result());
    HopAiConfigOptionPlugin plugin = new HopAiConfigOptionPlugin();
    plugin.setDefaultProviderName(defaultProvider);
    plugin.setExtraContext("");
    plugin.setExtraContextFiles("");
    GuiCompositeWidgets widgets = new GuiCompositeWidgets(new Variables());
    widgets.createCompositeWidgets(plugin, null, composite, PARENT, null);
    widgets.setWidgetsContents(plugin, composite, PARENT);
    shell.layout(true, true);
    plugin.widgetsPopulated(widgets);
    shell.layout(true, true);
    return widgets;
  }

  private static MetaSelectionLine<?> providerLine(GuiCompositeWidgets widgets) {
    return (MetaSelectionLine<?>)
        widgets.getWidgetsMap().get(HopAiConfigOptionPlugin.WIDGET_DEFAULT_PROVIDER);
  }

  private static Link warning(GuiCompositeWidgets widgets) {
    return (Link)
        widgets.getWidgetsMap().get(HopAiConfigOptionPlugin.WIDGET_DEFAULT_PROVIDER_WARNING);
  }

  /** Where on the screen the variable icon of a field starts. */
  private static int iconX(Composite field) {
    for (Control child : field.getChildren()) {
      if (child instanceof Label label && label.getImage() != null) {
        return label.toDisplay(0, 0).x;
      }
    }
    throw new AssertionError("no variable icon in " + field);
  }

  private static <T> T onUi(Supplier<T> supplier) {
    AtomicReference<T> result = new AtomicReference<>();
    Display.getDefault().syncExec(() -> result.set(supplier.get()));
    return result.get();
  }
}
