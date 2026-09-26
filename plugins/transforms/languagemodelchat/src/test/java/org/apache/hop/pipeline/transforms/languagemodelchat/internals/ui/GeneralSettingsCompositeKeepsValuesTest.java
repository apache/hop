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

package org.apache.hop.pipeline.transforms.languagemodelchat.internals.ui;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transforms.languagemodelchat.LanguageModelChatDialog;
import org.apache.hop.pipeline.transforms.languagemodelchat.LanguageModelChatMeta;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.apache.hop.ui.testing.UpstreamFixture;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swtbot.swt.finder.waits.Conditions;
import org.eclipse.swtbot.swt.finder.widgets.SWTBotShell;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The input field of Language Model Chat ({@link GeneralSettingsComposite}) must survive focusing
 * it when the incoming fields can't be loaded, for example because an upstream Table Input has
 * broken SQL.
 */
@Tag("uitest")
class GeneralSettingsCompositeKeepsValuesTest extends SwtBotTestBase {

  private static final String TRANSFORM_NAME = "languagemodelchat";
  private static final String TITLE = i18nUtil.i18n("LanguageModelChatDialog.Shell.Title");

  @Test
  void inputFieldSurvivesFailingUpstream() {
    LanguageModelChatMeta meta = inputFromField("question");
    PipelineMeta pipelineMeta = UpstreamFixture.failingUpstream(TRANSFORM_NAME, meta);

    focusInputFieldAndPressOk(meta, pipelineMeta);

    assertEquals("question", meta.getInputField(), "OK must keep the configured input field");
  }

  @Test
  void inputFieldNotInUpstreamIsKept() {
    LanguageModelChatMeta meta = inputFromField("question");
    PipelineMeta pipelineMeta =
        UpstreamFixture.upstreamWithFields(TRANSFORM_NAME, meta, "id", "name");

    focusInputFieldAndPressOk(meta, pipelineMeta);

    assertEquals("question", meta.getInputField());
  }

  private void focusInputFieldAndPressOk(LanguageModelChatMeta meta, PipelineMeta pipelineMeta) {
    AtomicReference<LanguageModelChatDialog> dialog = new AtomicReference<>();
    withDialog(
        parent -> {
          LanguageModelChatDialog chatDialog =
              new LanguageModelChatDialog(parent, new Variables(), meta, pipelineMeta);
          dialog.set(chatDialog);
          chatDialog.open();
        },
        bot -> {
          SWTBotShell shell = bot.shell(TITLE).activate();
          postEvent(inputFieldCombo(dialog.get()), SWT.FocusIn);
          closeOtherShells(TITLE, 2000);
          // OK refuses to close the dialog while the input field is empty.
          shell.activate().bot().button(buttonLabel("System.Button.OK")).click();
          bot.waitUntil(Conditions.shellCloses(shell));
        });
  }

  private static CCombo inputFieldCombo(LanguageModelChatDialog dialog) {
    try {
      Collection<?> composites = (Collection<?>) FieldUtils.readField(dialog, "composites", true);
      return composites.stream()
          .filter(GeneralSettingsComposite.class::isInstance)
          .map(GeneralSettingsComposite.class::cast)
          .findFirst()
          .orElseThrow()
          .getInputFieldInput();
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(e);
    }
  }

  private static LanguageModelChatMeta inputFromField(String inputField) {
    LanguageModelChatMeta meta = new LanguageModelChatMeta();
    meta.setDefault();
    meta.setInputField(inputField);
    return meta;
  }
}
