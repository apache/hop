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

package org.apache.hop.ui.hopgui.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.Const;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.ui.hopgui.file.config.FileValidationConfigPlugin;
import org.apache.hop.ui.hopgui.perspective.explorer.config.ExplorerPerspectiveConfigPlugin;
import org.apache.hop.ui.hopgui.welcome.WelcomeDialogOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

/**
 * The hop-ui config plugins that keep a documented default in {@code
 * GuiWidgetElement#defaultValue()} rather than on a {@code configClass()}, checked against what
 * they really fall back to. See {@code LinterConfigPluginDefaultsTest} for why those two ways of
 * holding a default exist and why the annotated one needs guarding.
 */
class ConfigPluginDefaultsTest {

  /** Plugins whose no-argument constructor loads every option from {@link HopConfig}. */
  private static final List<Class<?>> SELF_LOADING =
      List.of(FileValidationConfigPlugin.class, WelcomeDialogOptions.class);

  private Map<String, Object> savedConfig;

  @AfterEach
  void restoreConfiguration() {
    if (savedConfig != null) {
      HopConfig.getInstance().getConfigMap().putAll(savedConfig);
      savedConfig = null;
    }
  }

  @TestFactory
  List<DynamicTest> everyDeclaredDefaultIsTheRealDefault() throws Exception {
    // An unconfigured Hop: every readOption... call now falls through to its inline default.
    savedConfig = new HashMap<>(HopConfig.getInstance().getConfigMap());
    HopConfig.getInstance().getConfigMap().clear();

    List<DynamicTest> tests = new ArrayList<>();
    for (Class<?> pluginClass : SELF_LOADING) {
      Object unconfigured = pluginClass.getDeclaredConstructor().newInstance();
      for (Field field : pluginClass.getDeclaredFields()) {
        GuiWidgetElement widget = field.getAnnotation(GuiWidgetElement.class);
        if (widget == null || widget.defaultValue().isEmpty()) {
          continue;
        }
        String declared = widget.defaultValue();
        tests.add(
            dynamicTest(
                pluginClass.getSimpleName() + "." + field.getName(),
                () -> {
                  field.setAccessible(true);
                  Object actual = field.get(unconfigured);
                  assertEquals(
                      declared,
                      String.valueOf(actual),
                      "@GuiWidgetElement(defaultValue) on "
                          + pluginClass.getSimpleName()
                          + "."
                          + field.getName()
                          + " says the option defaults to '"
                          + declared
                          + "', but with nothing configured it loads as '"
                          + actual
                          + "'. The documentation is generated from the annotation, so either the"
                          + " fallback at the readOption call site or this annotation is wrong.");
                }));
      }
    }
    assertFalse(
        tests.isEmpty(),
        "No option declares a defaultValue, so the cases below would pass without testing anything");
    return tests;
  }

  /**
   * The explorer perspective reads its undo limit from {@code PropsUi}, which needs a display, so
   * it is pinned to the constant that feeds it instead of to a loaded instance.
   */
  @Test
  void theUndoLimitDefaultMatchesTheConstantBehindIt() throws Exception {
    Field field = ExplorerPerspectiveConfigPlugin.class.getDeclaredField("maxUndo");
    String declared = field.getAnnotation(GuiWidgetElement.class).defaultValue();
    assertEquals(
        Integer.toString(Const.MAX_UNDO),
        declared,
        "@GuiWidgetElement(defaultValue) on ExplorerPerspectiveConfigPlugin.maxUndo has to keep up"
            + " with Const.MAX_UNDO, which is what PropsUi.getMaxUndo() falls back to");
  }
}
