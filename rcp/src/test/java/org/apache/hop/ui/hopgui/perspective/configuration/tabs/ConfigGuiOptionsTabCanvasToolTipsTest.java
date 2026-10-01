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

package org.apache.hop.ui.hopgui.perspective.configuration.tabs;

import static org.eclipse.swtbot.swt.finder.matchers.WidgetMatcherFactory.allOf;
import static org.eclipse.swtbot.swt.finder.matchers.WidgetMatcherFactory.widgetOfType;
import static org.eclipse.swtbot.swt.finder.matchers.WidgetMatcherFactory.withText;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.EnumMap;
import java.util.Map;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.hopgui.HopGuiEnvironment;
import org.apache.hop.ui.hopgui.file.GraphCanvasTestBase;
import org.apache.hop.ui.hopgui.file.shared.CanvasToolTip;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swtbot.swt.finder.utils.SWTUtils;
import org.eclipse.swtbot.swt.finder.widgets.SWTBotCheckBox;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The Look &amp; Feel tab offers one checkbox per kind of canvas tooltip, in a group of its own
 * inside the "Pipeline and Workflow canvas" section, and a click on one lands in the properties.
 */
@Tag("uitest")
class ConfigGuiOptionsTabCanvasToolTipsTest extends GraphCanvasTestBase {

  @BeforeAll
  static void registerGuiPlugins() throws HopException {
    HopGuiEnvironment.init();
  }

  @Test
  void everyCanvasToolTipHasACheckboxInItsOwnGroup() {
    Map<CanvasToolTip, Boolean> before = new EnumMap<>(CanvasToolTip.class);
    try {
      withScene(
          shell -> {
            hopGui();
            for (CanvasToolTip toolTip : CanvasToolTip.values()) {
              before.put(toolTip, PropsUi.getInstance().isCanvasToolTipShown(toolTip));
              PropsUi.getInstance().setCanvasToolTipShown(toolTip, true);
            }
            shell.setSize(700, 1000);
            shell.setLayout(new FillLayout());
            CTabFolder tabFolder = new CTabFolder(shell, SWT.NONE);
            new ConfigGuiOptionsTab().addGuiOptionsTab(tabFolder);
            tabFolder.setSelection(0);
          },
          bot -> {
            Group group =
                bot.widget(
                    allOf(
                        widgetOfType(Group.class),
                        withText(msg("EnterOptionsDialog.CanvasToolTips.Label"))));

            Map<CanvasToolTip, SWTBotCheckBox> boxes = new EnumMap<>(CanvasToolTip.class);
            for (CanvasToolTip toolTip : CanvasToolTip.values()) {
              boxes.put(toolTip, bot.checkBox(msg(toolTip.getLabelKey())));
            }
            // Scroll the group into view and keep a picture of it, for a look at the layout.
            onUi(
                () -> {
                  Composite content = group.getParent();
                  while (!(content.getParent() instanceof ScrolledComposite)) {
                    content = content.getParent();
                  }
                  ScrolledComposite scrolled = (ScrolledComposite) content.getParent();
                  Rectangle inContent =
                      group.getDisplay().map(group.getParent(), content, group.getBounds());
                  scrolled.setOrigin(0, inContent.y);
                  scrolled.getShell().forceActive();
                });
            Rectangle shellBounds = onUi(() -> group.getShell().getBounds());
            SWTUtils.captureScreenshot(
                "target/screenshots/canvas-tooltips-options.png", shellBounds);

            assertAll(
                boxes.entrySet().stream()
                    .map(
                        entry ->
                            () -> {
                              assertSame(
                                  group,
                                  onUi(() -> entry.getValue().widget.getParent()),
                                  entry.getKey() + " should sit in the Tooltips group");
                              assertTrue(
                                  entry.getValue().isChecked(),
                                  entry.getKey() + " should start checked");
                            }));

            // A click lands in the properties at once, and only for that kind.
            boxes.get(CanvasToolTip.EDIT_HINT).click();
            assertFalse(
                onUi(() -> PropsUi.getInstance().isCanvasToolTipShown(CanvasToolTip.EDIT_HINT)),
                "unticking the edit hint should switch it off");
            assertTrue(
                onUi(() -> PropsUi.getInstance().isCanvasToolTipShown(CanvasToolTip.HOP)),
                "the other kinds stay on");

            boxes.get(CanvasToolTip.EDIT_HINT).click();
            assertTrue(
                onUi(() -> PropsUi.getInstance().isCanvasToolTipShown(CanvasToolTip.EDIT_HINT)),
                "ticking the edit hint again should switch it back on");
          });
    } finally {
      onUi(
          () ->
              before.forEach(
                  (toolTip, shown) -> PropsUi.getInstance().setCanvasToolTipShown(toolTip, shown)));
    }
  }

  private static String msg(String key) {
    return BaseMessages.getString(BaseDialog.class, key);
  }
}
