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

package org.apache.hop.ai.ui;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Hop Web runs on RAP/RWT, which ships an SWT subset. Classes loaded in Hop Web must not mention
 * desktop-only SWT types in field or method signatures, as reflection or class linking will fail
 * with {@link NoClassDefFoundError}.
 */
class AiAdvisorWebCompatibilityTest {

  private static final Set<String> DESKTOP_ONLY_SWT_TYPES =
      new HashSet<>(
          Arrays.asList(
              "org.eclipse.swt.custom.BidiSegmentListener",
              "org.eclipse.swt.custom.Bullet",
              "org.eclipse.swt.custom.CTabFolderRenderer",
              "org.eclipse.swt.custom.CaretListener",
              "org.eclipse.swt.custom.ExtendedModifyListener",
              "org.eclipse.swt.custom.LineBackgroundListener",
              "org.eclipse.swt.custom.LineStyleEvent",
              "org.eclipse.swt.custom.LineStyleListener",
              "org.eclipse.swt.custom.PaintObjectListener",
              "org.eclipse.swt.custom.PopupList",
              "org.eclipse.swt.custom.ST",
              "org.eclipse.swt.custom.StyleRange",
              "org.eclipse.swt.custom.StyledText",
              "org.eclipse.swt.custom.StyledTextContent",
              "org.eclipse.swt.custom.TableCursor",
              "org.eclipse.swt.custom.TreeCursor",
              "org.eclipse.swt.custom.VerifyKeyListener",
              "org.eclipse.swt.graphics.GlyphMetrics",
              "org.eclipse.swt.graphics.Pattern",
              "org.eclipse.swt.graphics.Region",
              "org.eclipse.swt.graphics.TextLayout",
              "org.eclipse.swt.graphics.TextStyle",
              "org.eclipse.swt.widgets.Caret",
              "org.eclipse.swt.widgets.Tracker"));

  private static final List<Class<?>> WEB_ELIGIBLE_UI_CLASSES =
      List.of(
          AiAdvisorPerspective.class,
          AiAdvisorWorkbench.class,
          AiAdvisorSessionPane.class,
          AiAdvisorTranscriptPanel.class,
          AiAdvisorViews.class,
          AiAdvisorProposalReviewDialog.class,
          AiAdvisorMetadataSelectionDialog.class,
          AiAdvisorDialog.class,
          PipelineAiGuiPlugin.class,
          WorkflowAiGuiPlugin.class);

  @Test
  void uiClassesAvoidDesktopOnlySwtTypesInSignatures() {
    List<String> violations = new ArrayList<>();

    for (Class<?> clazz : WEB_ELIGIBLE_UI_CLASSES) {
      for (Field field : clazz.getDeclaredFields()) {
        if (DESKTOP_ONLY_SWT_TYPES.contains(field.getType().getName())) {
          violations.add(
              clazz.getName()
                  + ": field "
                  + field.getName()
                  + " of type "
                  + field.getType().getName());
        }
      }
      for (Method method : clazz.getDeclaredMethods()) {
        List<Class<?>> types = new ArrayList<>();
        types.add(method.getReturnType());
        types.addAll(Arrays.asList(method.getParameterTypes()));
        for (Class<?> type : types) {
          if (DESKTOP_ONLY_SWT_TYPES.contains(type.getName())) {
            violations.add(
                clazz.getName() + ": method " + method.getName() + " uses " + type.getName());
          }
        }
      }
    }

    assertTrue(
        violations.isEmpty(),
        "UI classes eligible for Hop Web must not name desktop-only SWT types in field or method"
            + " signatures: "
            + violations);
  }
}
