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

package org.apache.hop.pipeline.transforms.delay;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.apache.hop.ui.testing.UpstreamFixture;
import org.eclipse.swt.SWT;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.eclipse.swtbot.swt.finder.waits.DefaultCondition;
import org.eclipse.swtbot.swt.finder.widgets.SWTBotShell;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * A timeout read from a field must stay a field, not turn into a literal timeout (0 ms at runtime),
 * when the numeric incoming fields can't be listed. The failure is reported once, not on every
 * focus and again on OK.
 */
@Tag("uitest")
class DelayDialogKeepsValuesTest extends SwtBotTestBase {

  private static final String TRANSFORM_NAME = "delay";
  private static final String TITLE =
      BaseMessages.getString(DelayMeta.class, "DelayDialog.Shell.Title");

  @Test
  void timeoutFieldSurvivesFailingUpstream() {
    DelayMeta meta = timeoutFromField("delay_ms");
    PipelineMeta pipelineMeta = UpstreamFixture.failingUpstream(TRANSFORM_NAME, meta);

    focusCombosAndPressOk(meta, pipelineMeta);

    assertEquals("delay_ms", meta.getTimeoutField());
    assertNull(meta.getTimeout());
  }

  @Test
  void timeoutFieldSurvivesUpstreamWithoutNumericFields() {
    DelayMeta meta = timeoutFromField("delay_ms");
    PipelineMeta pipelineMeta =
        UpstreamFixture.upstreamWithFields(TRANSFORM_NAME, meta, "id", "name");

    focusCombosAndPressOk(meta, pipelineMeta);

    assertEquals("delay_ms", meta.getTimeoutField());
    assertNull(meta.getTimeout());
  }

  @Test
  void literalTimeoutStaysLiteral() {
    DelayMeta meta = new DelayMeta();
    meta.setDefault();
    meta.setTimeout("500");
    PipelineMeta pipelineMeta = UpstreamFixture.failingUpstream(TRANSFORM_NAME, meta);

    focusCombosAndPressOk(meta, pipelineMeta);

    assertEquals("500", meta.getTimeout());
    assertNull(meta.getTimeoutField());
  }

  @Test
  void failingUpstreamIsReportedOnce() {
    DelayMeta meta = timeoutFromField("delay_ms");
    PipelineMeta pipelineMeta = UpstreamFixture.failingUpstream(TRANSFORM_NAME, meta);

    int errorDialogs = focusCombosAndPressOk(meta, pipelineMeta);

    assertEquals(1, errorDialogs, "error dialogs shown for one failing upstream");
  }

  /** Opens the dialog, focuses both field combos, presses OK and returns the error dialogs seen. */
  private int focusCombosAndPressOk(DelayMeta meta, PipelineMeta pipelineMeta) {
    AtomicReference<DelayDialog> dialog = new AtomicReference<>();
    AtomicInteger errorDialogs = new AtomicInteger();
    withDialog(
        parent -> {
          dialog.set(new DelayDialog(parent, new Variables(), meta, pipelineMeta));
          dialog.get().open();
        },
        bot -> {
          errorDialogs.addAndGet(closeOtherShells(TITLE, 2000));
          shownDialog(bot);
          postEvent(combo(dialog.get(), "wTimeout").getCComboWidget(), SWT.FocusIn);
          errorDialogs.addAndGet(closeOtherShells(TITLE, 1500));
          postEvent(combo(dialog.get(), "wScaleTimeField").getCComboWidget(), SWT.FocusIn);
          errorDialogs.addAndGet(closeOtherShells(TITLE, 1500));
          postEvent(
              bot.shell(TITLE).bot().button(buttonLabel("System.Button.OK")).widget, SWT.Selection);
          errorDialogs.addAndGet(closeOtherShells(TITLE, 1500));
        });
    return errorDialogs.get();
  }

  private static ComboVar combo(Object dialog, String name) {
    try {
      return (ComboVar) FieldUtils.readField(dialog, name, true);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(e);
    }
  }

  private static DelayMeta timeoutFromField(String fieldName) {
    DelayMeta meta = new DelayMeta();
    meta.setDefault();
    meta.setTimeout(null);
    meta.setTimeoutField(fieldName);
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
