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
package org.apache.hop.pgvector;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.List;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.util.TranslateUtil;
import org.apache.hop.metadata.api.HopMetadataProperty;

/**
 * Checks on the {@link GuiWidgetElement} annotations that drive the pgvector dialogs. Both dialogs
 * are generated from the metadata classes, so a property without an annotation silently disappears
 * from the dialog and a mistyped label silently renders as {@code !Some.Key!}.
 */
public final class GuiWidgetCoverage {

  private GuiWidgetCoverage() {
    // Utility class
  }

  /**
   * Asserts that every persisted property carries a widget annotation, apart from the ones listed
   * in {@code handledOutsideTheAnnotations}, which the dialog builds itself through {@code
   * registerExtraGroup}.
   */
  public static void assertEveryPropertyHasAWidget(
      Class<?> metaClass, List<String> handledOutsideTheAnnotations) {
    for (Field field : metaClass.getDeclaredFields()) {
      if (field.getAnnotation(HopMetadataProperty.class) == null
          || handledOutsideTheAnnotations.contains(field.getName())) {
        continue;
      }
      assertTrue(
          field.getAnnotation(GuiWidgetElement.class) != null,
          "Property "
              + field.getName()
              + " is persisted but has no @GuiWidgetElement, so it is missing from the dialog");
    }
  }

  /**
   * Asserts that every widget label, tooltip and group name resolves against the message bundle.
   */
  public static void assertWidgetTextResolves(Class<?> metaClass) {
    for (Field field : metaClass.getDeclaredFields()) {
      GuiWidgetElement widget = field.getAnnotation(GuiWidgetElement.class);
      if (widget == null) {
        continue;
      }
      assertResolves(metaClass, field.getName(), "label", widget.label());
      assertResolves(metaClass, field.getName(), "toolTip", widget.toolTip());
      assertResolves(metaClass, field.getName(), "group", widget.group());
    }
  }

  private static void assertResolves(
      Class<?> metaClass, String fieldName, String what, String value) {
    String translated = TranslateUtil.translate(value, metaClass);
    assertFalse(
        translated.startsWith("!") && translated.endsWith("!"),
        "The " + what + " of " + fieldName + " does not resolve: " + translated);
  }
}
