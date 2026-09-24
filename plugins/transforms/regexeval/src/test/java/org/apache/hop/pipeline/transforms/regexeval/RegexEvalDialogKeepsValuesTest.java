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

package org.apache.hop.pipeline.transforms.regexeval;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.apache.hop.ui.testing.UpstreamFixture;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Widget;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.eclipse.swtbot.swt.finder.waits.DefaultCondition;
import org.eclipse.swtbot.swt.finder.widgets.SWTBotShell;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The "field to evaluate" of Regex Evaluation must survive a focus on its combo, whether or not the
 * incoming fields can be loaded, and an empty value must not be filled in by that focus.
 */
@Tag("uitest")
class RegexEvalDialogKeepsValuesTest extends SwtBotTestBase {

  private static final String TRANSFORM_NAME = "regex";
  private static final String TITLE =
      BaseMessages.getString(RegexEvalMeta.class, "RegexEvalDialog.Shell.Title");

  @Test
  void fieldToEvaluateSurvivesFailingUpstream() {
    RegexEvalMeta meta = matching("id_json");
    PipelineMeta pipelineMeta = UpstreamFixture.failingUpstream(TRANSFORM_NAME, meta);

    focusFieldToEvaluateAndPressOk(meta, pipelineMeta);

    assertEquals("id_json", meta.getMatcher());
  }

  @Test
  void fieldToEvaluateNotInUpstreamIsKept() {
    RegexEvalMeta meta = matching("renamed_field");
    PipelineMeta pipelineMeta =
        UpstreamFixture.upstreamWithFields(TRANSFORM_NAME, meta, "id", "id_json");

    focusFieldToEvaluateAndPressOk(meta, pipelineMeta);

    assertEquals("renamed_field", meta.getMatcher());
  }

  @Test
  void emptyFieldToEvaluateIsNotFilledInByFocus() {
    RegexEvalMeta meta = matching("");
    PipelineMeta pipelineMeta =
        UpstreamFixture.upstreamWithFields(TRANSFORM_NAME, meta, "id", "id_json");

    focusFieldToEvaluateAndPressOk(meta, pipelineMeta);

    assertEquals("", meta.getMatcher());
  }

  private void focusFieldToEvaluateAndPressOk(RegexEvalMeta meta, PipelineMeta pipelineMeta) {
    AtomicReference<RegexEvalDialog> dialog = new AtomicReference<>();
    withDialog(
        parent -> {
          dialog.set(new RegexEvalDialog(parent, new Variables(), meta, pipelineMeta));
          dialog.get().open();
        },
        bot -> {
          shownDialog(bot);
          postEvent(field(dialog.get(), "wFieldEvaluate"), SWT.FocusIn);
          // A failing upstream opens an error dialog; dismiss it.
          closeOtherShells(TITLE, 2000);
          bot.shell(TITLE).activate().bot().button(buttonLabel("System.Button.OK")).click();
        });
  }

  private static Widget field(Object dialog, String name) {
    try {
      return (Widget) FieldUtils.readField(dialog, name, true);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(e);
    }
  }

  private static RegexEvalMeta matching(String fieldName) {
    RegexEvalMeta meta = new RegexEvalMeta();
    meta.setDefault();
    meta.setMatcher(fieldName);
    meta.setResultFieldName("result");
    meta.setScript("[0-9]+");
    return meta;
  }

  /** The shell is found as soon as it is created; wait until open() has built and shown it. */
  private static SWTBot shownDialog(SWTBot bot) {
    SWTBotShell shell = bot.shell(TITLE);
    bot.waitUntil(
        new DefaultCondition() {
          @Override
          public boolean test() {
            return shell.isVisible();
          }

          @Override
          public String getFailureMessage() {
            return "Dialog " + TITLE + " was never shown";
          }
        });
    return shell.bot();
  }
}
