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

package org.apache.hop.workflow.actions.dbt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The dbt dialog is generated from the {@link GuiWidgetElement} annotations on {@link ActionDbt},
 * so a property without an annotation silently disappears from the dialog, a mistyped label
 * silently renders as {@code !Some.Key!} and a wrong group quietly moves a field to another tab.
 * This test covers the annotations rather than the SWT layout, so it runs without a display.
 */
class ActionDbtWidgetLayoutTest {

  /** The two name/value tables the dialog adds itself through {@code registerExtraGroup}. */
  private static final List<String> HANDLED_OUTSIDE_THE_ANNOTATIONS = List.of("vars", "envVars");

  /**
   * The unit-test JVM does not scan the plugin jars, so register the annotated fields the same way
   * the {@code GuiPluginType} scan does at runtime.
   */
  @BeforeAll
  static void registerActionWidgets() {
    GuiRegistry registry = GuiRegistry.getInstance();
    if (registry.findGuiElements(ActionDbt.class.getName(), ActionDbt.GUI_PLUGIN_ELEMENT_PARENT_ID)
        != null) {
      return;
    }
    for (Field field : ActionDbt.class.getDeclaredFields()) {
      GuiWidgetElement element = field.getAnnotation(GuiWidgetElement.class);
      if (element != null) {
        registry.addGuiWidgetElement(ActionDbt.class.getName(), element, field);
      }
    }
  }

  @Test
  void everyPersistedPropertyIsOnTheDialog() {
    for (Field field : ActionDbt.class.getDeclaredFields()) {
      if (field.getAnnotation(HopMetadataProperty.class) == null
          || HANDLED_OUTSIDE_THE_ANNOTATIONS.contains(field.getName())) {
        continue;
      }
      assertNotNull(
          field.getAnnotation(GuiWidgetElement.class),
          "Property "
              + field.getName()
              + " is persisted but has no @GuiWidgetElement, so it is missing from the dialog");
    }
  }

  @Test
  void everyWidgetTextResolves() {
    for (Field field : ActionDbt.class.getDeclaredFields()) {
      GuiWidgetElement widget = field.getAnnotation(GuiWidgetElement.class);
      if (widget == null) {
        continue;
      }
      assertResolves(field.getName(), "label", widget.label());
      assertResolves(field.getName(), "toolTip", widget.toolTip());
      assertResolves(field.getName(), "group", widget.group());
    }
    // The fourth tab has no annotated fields: its label is resolved by the dialog itself.
    assertResolves("the variables tab", "group", ActionDbt.GROUP_VARIABLES);
  }

  @Test
  void fieldsAreLaidOutOnTabsInTheExpectedOrder() {
    GuiElements elements =
        GuiRegistry.getInstance()
            .findGuiElements(ActionDbt.class.getName(), ActionDbt.GUI_PLUGIN_ELEMENT_PARENT_ID);
    assertNotNull(elements, "No widgets are registered for the dbt action");
    assertTrue(GuiWidgetGroups.hasGroups(elements.getChildren()), "The fields are not grouped");
    assertFalse(
        GuiWidgetGroups.hasMixedTypes(elements.getChildren()),
        "Mixed group types fall back to tabs instead of using the declared one");
    assertEquals(GuiWidgetGroupType.TABS, GuiWidgetGroups.typeOf(elements.getChildren()));

    List<GuiWidgetGroups.Bucket> buckets = GuiWidgetGroups.from(elements.getChildren(), "General");
    assertEquals(
        List.of(
            TranslateUtil.translate(ActionDbt.GROUP_PROJECT, ActionDbt.class),
            TranslateUtil.translate(ActionDbt.GROUP_SELECTION, ActionDbt.class),
            TranslateUtil.translate(ActionDbt.GROUP_EXECUTION, ActionDbt.class)),
        buckets.stream().map(GuiWidgetGroups.Bucket::getLabel).toList(),
        "The annotated fields should be laid out on three tabs, in this order");

    assertEquals(List.of("dbtProjectName", "operation", "target"), fieldNames(buckets.get(0)));
    assertEquals(List.of("select", "exclude", "fullRefresh"), fieldNames(buckets.get(1)));
    assertEquals(List.of("threads", "timeout", "emitOpenLineage"), fieldNames(buckets.get(2)));
  }

  private static List<String> fieldNames(GuiWidgetGroups.Bucket bucket) {
    List<String> names = new ArrayList<>();
    for (GuiElements element : bucket.getElements()) {
      names.add(element.getFieldName());
    }
    return names;
  }

  private static void assertResolves(String fieldName, String what, String value) {
    String translated = TranslateUtil.translate(value, ActionDbt.class);
    assertFalse(
        translated.startsWith("!") && translated.endsWith("!"),
        "The " + what + " of " + fieldName + " does not resolve: " + translated);
  }
}
