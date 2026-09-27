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

package org.apache.hop.pipeline.transforms.clonerow;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.apache.hop.ui.testing.UpstreamFixture;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Widget;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The "number of clones in field" value of CloneRowDialog must survive when the incoming fields
 * can't be loaded, for example because an upstream Table Input has broken SQL (follow-up of issue
 * #5953).
 */
@Tag("uitest")
class CloneRowDialogKeepsValuesTest extends SwtBotTestBase {

  private static final String TRANSFORM_NAME = "clone row";
  private static final String TITLE =
      BaseMessages.getString(CloneRowMeta.class, "CloneRowDialog.Shell.Title");

  @Test
  void valuesSurviveFailingUpstream() {
    CloneRowMeta meta = new CloneRowMeta();
    meta.setDefault();
    meta.setNrCloneInField(true);
    meta.setNrCloneField("nr_clones");
    PipelineMeta pipelineMeta = UpstreamFixture.failingUpstream(TRANSFORM_NAME, meta);
    AtomicReference<CloneRowDialog> dialog = new AtomicReference<>();
    AtomicInteger errorDialogs = new AtomicInteger();
    String[] combos = {"wNrCloneField", "wNrCloneField"};
    String[] shownValues = {"nr_clones", "nr_clones"};

    withDialog(
        parent -> {
          dialog.set(new CloneRowDialog(parent, new Variables(), meta, pipelineMeta));
          dialog.get().open();
        },
        bot -> {
          bot.shell(TITLE);
          // Focus the combo twice, the way a user clicks into it again after the error.
          for (int i = 0; i < combos.length; i++) {
            CCombo combo = (CCombo) widget(dialog.get(), combos[i]);
            postEvent(combo, SWT.FocusIn);
            // Check the combo while the error is still on screen: before the fix the dialog kept
            // re-opening the error every time the focus returned to the combo.
            waitForOtherShell(2000);
            assertEquals(shownValues[i], textOf(combo), combos[i] + " must keep its value");
            errorDialogs.addAndGet(closeOtherShells(TITLE, 1000));
          }
          bot.shell(TITLE).activate().bot().button(buttonLabel("System.Button.OK")).click();
        });

    assertEquals(
        "nr_clones", meta.getNrCloneField(), "OK must keep the configured number of clones field");
    assertEquals(1, errorDialogs.get(), "the failure to load the fields is reported once");
  }

  /** Waits until a shell other than the dialog (an error dialog) is visible. */
  private static void waitForOtherShell(long waitMillis) {
    long deadline = System.currentTimeMillis() + waitMillis;
    AtomicBoolean found = new AtomicBoolean();
    while (!found.get() && System.currentTimeMillis() < deadline) {
      display.syncExec(
          () -> {
            for (Shell shell : display.getShells()) {
              if (!shell.isDisposed() && shell.isVisible() && !TITLE.equals(shell.getText())) {
                found.set(true);
              }
            }
          });
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  private static String textOf(CCombo combo) {
    AtomicReference<String> text = new AtomicReference<>();
    display.syncExec(() -> text.set(combo.getText()));
    return text.get();
  }

  private static Widget widget(Object dialog, String fieldName) {
    try {
      return (Widget) FieldUtils.readField(dialog, fieldName, true);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(e);
    }
  }
}
