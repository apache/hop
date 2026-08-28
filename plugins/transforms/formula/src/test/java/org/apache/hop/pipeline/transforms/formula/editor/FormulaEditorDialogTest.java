/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.formula.editor;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.eclipse.swtbot.swt.finder.finders.UIThreadRunnable;
import org.eclipse.swtbot.swt.finder.widgets.SWTBotShell;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Covers the geometry handling of the formula expression editor (issue #8139): the shell used to be
 * created without any size, so Windows sized every new instance from its cascaded origin to the
 * screen edge and each reopening was smaller than the previous one.
 *
 * <p>Tagged {@code uitest} so it is skipped when there is no display.
 */
@Tag("uitest")
class FormulaEditorDialogTest extends SwtBotTestBase {

  private static final String TITLE =
      BaseMessages.getString(FormulaEditor.PKG, "FormulaEditor.Shell.Title");
  private static final Point CHOSEN_SIZE = new Point(720, 540);

  @Test
  void reopeningRestoresTheSizeTheUserChose() {
    openEditor(
        bot -> {
          SWTBotShell shell = bot.shell(TITLE).activate();
          UIThreadRunnable.syncExec(() -> shell.widget.setSize(CHOSEN_SIZE.x, CHOSEN_SIZE.y));
          shell.bot().button(buttonLabel("System.Button.Cancel")).click();
        });

    AtomicReference<Point> sizeOnReopen = new AtomicReference<>();
    openEditor(
        bot -> {
          SWTBotShell shell = bot.shell(TITLE).activate();
          sizeOnReopen.set(UIThreadRunnable.syncExec(() -> shell.widget.getSize()));
          shell.bot().button(buttonLabel("System.Button.Cancel")).click();
        });

    assertEquals(
        CHOSEN_SIZE,
        sizeOnReopen.get(),
        "the second opening must restore the geometry saved when the first one was closed");
  }

  private void openEditor(java.util.function.Consumer<SWTBot> interactions) {
    withDialog(
        parent -> {
          try {
            new FormulaEditor(
                    new Variables(),
                    parent,
                    SWT.APPLICATION_MODAL | SWT.SHEET,
                    "[field]",
                    new String[] {"field"})
                .open();
          } catch (HopException e) {
            throw new IllegalStateException("could not open the formula editor", e);
          }
        },
        interactions);
  }
}
