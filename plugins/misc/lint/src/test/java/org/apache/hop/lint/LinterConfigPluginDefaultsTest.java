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

package org.apache.hop.lint;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

/**
 * The documentation for the configuration perspective reports what each option falls back to when
 * it has never been set. Most config plugins keep those values on the {@code configClass()} of
 * their {@code @ConfigPlugin}, where a generator reads the real thing. This one does not: it reads
 * each option straight from {@link HopConfig} with the fallback written inline at the call site, so
 * the documented value has to be repeated in {@code GuiWidgetElement#defaultValue()} and can fall
 * out of step with the code.
 *
 * <p>This closes that gap. With nothing configured, every {@code readOption...} call returns its
 * inline fallback, so the loaded field is by definition the real default - and has to match what
 * the annotation promises the reader.
 */
class LinterConfigPluginDefaultsTest {

  private Map<String, Object> savedConfig;

  @AfterEach
  void restoreConfiguration() {
    if (savedConfig != null) {
      HopConfig.getInstance().getConfigMap().putAll(savedConfig);
      savedConfig = null;
    }
  }

  @TestFactory
  List<DynamicTest> everyDeclaredDefaultIsTheRealDefault() {
    List<Field> annotated = new ArrayList<>();
    for (Field field : LinterConfigPlugin.class.getDeclaredFields()) {
      GuiWidgetElement widget = field.getAnnotation(GuiWidgetElement.class);
      if (widget != null && !widget.defaultValue().isEmpty()) {
        annotated.add(field);
      }
    }
    assertFalse(
        annotated.isEmpty(),
        "No option declares a defaultValue, so the cases below would pass without testing anything");

    // An unconfigured Hop: every readOption... call now falls through to its inline default.
    savedConfig = new HashMap<>(HopConfig.getInstance().getConfigMap());
    HopConfig.getInstance().getConfigMap().clear();
    LinterConfigPlugin unconfigured = new LinterConfigPlugin();

    List<DynamicTest> tests = new ArrayList<>();
    for (Field field : annotated) {
      String declared = field.getAnnotation(GuiWidgetElement.class).defaultValue();
      tests.add(
          dynamicTest(
              field.getName(),
              () -> {
                field.setAccessible(true);
                Object actual = field.get(unconfigured);
                assertEquals(
                    declared,
                    String.valueOf(actual),
                    "@GuiWidgetElement(defaultValue) on LinterConfigPlugin."
                        + field.getName()
                        + " says the option defaults to '"
                        + declared
                        + "', but with nothing configured it loads as '"
                        + actual
                        + "'. The documentation is generated from the annotation, so either the"
                        + " fallback at the readOption call site or this annotation is wrong.");
              }));
    }
    return tests;
  }
}
