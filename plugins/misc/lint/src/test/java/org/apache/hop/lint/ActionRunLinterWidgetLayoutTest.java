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
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.gui.plugin.GuiElements;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.gui.plugin.GuiWidgetGroups;
import org.apache.hop.core.util.TranslateUtil;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The Run Linter dialog is generated from the annotations on {@link ActionRunLinter}. This covers
 * the annotations rather than the SWT layout, so it runs without a display.
 */
class ActionRunLinterWidgetLayoutTest {

  /** The test JVM does not scan the plugin jars: register the fields as the scan does. */
  @BeforeAll
  static void registerActionWidgets() {
    GuiRegistry registry = GuiRegistry.getInstance();
    if (registry.findGuiElements(
            ActionRunLinter.class.getName(), ActionRunLinter.GUI_PLUGIN_ELEMENT_PARENT_ID)
        != null) {
      return;
    }
    for (Field field : ActionRunLinter.class.getDeclaredFields()) {
      GuiWidgetElement element = field.getAnnotation(GuiWidgetElement.class);
      if (element != null) {
        registry.addGuiWidgetElement(ActionRunLinter.class.getName(), element, field);
      }
    }
  }

  @Test
  void everyPersistedPropertyIsOnTheDialog() {
    for (Field field : ActionRunLinter.class.getDeclaredFields()) {
      if (field.getAnnotation(HopMetadataProperty.class) != null) {
        assertNotNull(
            field.getAnnotation(GuiWidgetElement.class),
            field.getName() + " is persisted but missing from the dialog");
      }
    }
  }

  @Test
  void fieldsAreInThreeBoxesInTheOrderOfTheCommandLine() {
    GuiElements elements =
        GuiRegistry.getInstance()
            .findGuiElements(
                ActionRunLinter.class.getName(), ActionRunLinter.GUI_PLUGIN_ELEMENT_PARENT_ID);
    assertNotNull(elements);
    assertFalse(GuiWidgetGroups.hasMixedTypes(elements.getChildren()));
    assertEquals(GuiWidgetGroupType.BOXES, GuiWidgetGroups.typeOf(elements.getChildren()));

    List<GuiWidgetGroups.Bucket> buckets = GuiWidgetGroups.from(elements.getChildren(), "General");
    assertEquals(
        List.of(
            TranslateUtil.translate(ActionRunLinter.GROUP_TARGET, ActionRunLinter.class),
            TranslateUtil.translate(ActionRunLinter.GROUP_OUTCOME, ActionRunLinter.class),
            TranslateUtil.translate(ActionRunLinter.GROUP_REPORT, ActionRunLinter.class)),
        buckets.stream().map(GuiWidgetGroups.Bucket::getLabel).toList());
    assertEquals(List.of("target", "includeMetadata", "configFile"), fieldNames(buckets.get(0)));
    assertEquals(
        List.of("minimumSeverity", "failOn", "maxWarnings", "baselineFile"),
        fieldNames(buckets.get(1)));
    assertEquals(List.of("reportFile", "reportFormat"), fieldNames(buckets.get(2)));
  }

  @Test
  void theEnumChoicesHaveLabels() {
    for (Enum<?> value : List.of(LintSeverity.Level.INFO, LintSeverity.FailOn.NONE)) {
      String description = ((IEnumHasCodeAndDescription) value).getDescription();
      assertFalse(description.startsWith("!"), description);
    }
    assertEquals("SARIF", LintReportFormat.SARIF.getDescription());
  }

  private static List<String> fieldNames(GuiWidgetGroups.Bucket bucket) {
    List<String> names = new ArrayList<>();
    for (GuiElements element : bucket.getElements()) {
      names.add(element.getFieldName());
    }
    return names;
  }
}
