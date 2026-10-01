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

import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.HopGuiKeyHandler;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.widgets.Shell;

/** Non-modal floating window hosting {@link AiAdvisorWorkbench}. One per Hop GUI session. */
public class AiAdvisorDialog {

  public static final Class<?> PKG = AiAdvisorPerspective.class;

  static final String SHELL_DATA_KEY = AiAdvisorDialog.class.getName();

  private static final int MIN_WIDTH = 800;
  private static final int MIN_HEIGHT = 520;

  private final HopGui hopGui;
  private final PropsUi props;
  private Shell shell;
  private AiAdvisorWorkbench workbench;

  AiAdvisorDialog(HopGui hopGui) {
    this.hopGui = hopGui;
    this.props = PropsUi.getInstance();
  }

  public static void open(HopGui hopGui) {
    openOrCreate(hopGui);
  }

  static AiAdvisorDialog openOrCreate(HopGui hopGui) {
    if (hopGui == null || hopGui.getShell() == null || hopGui.getShell().isDisposed()) {
      return null;
    }
    Object existing = hopGui.getShell().getData(SHELL_DATA_KEY);
    if (existing instanceof AiAdvisorDialog dialog && dialog.isOpen()) {
      dialog.activate();
      return dialog;
    }
    AiAdvisorDialog dialog = new AiAdvisorDialog(hopGui);
    hopGui.getShell().setData(SHELL_DATA_KEY, dialog);
    dialog.openShell();
    return dialog;
  }

  boolean isOpen() {
    return shell != null && !shell.isDisposed();
  }

  void activate() {
    if (!isOpen()) {
      return;
    }
    shell.setMinimized(false);
    shell.setActive();
    shell.forceActive();
  }

  AiAdvisorWorkbench getWorkbench() {
    return workbench;
  }

  private void openShell() {
    shell = new Shell(hopGui.getShell(), SWT.SHELL_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    shell.setText(BaseMessages.getString(PKG, "AiAdvisorPerspective.Dialog.Title"));
    shell.setImage(
        GuiResource.getInstance()
            .getImage(
                "ai-provider.svg",
                AiAdvisorPerspective.class.getClassLoader(),
                ConstUi.MEDIUM_ICON_SIZE,
                ConstUi.MEDIUM_ICON_SIZE));
    PropsUi.setLook(shell);
    shell.setLayout(PropsUi.getInstance().createFormLayout());

    HopGuiAiAdvisorWorkbenchHost host =
        new HopGuiAiAdvisorWorkbenchHost(hopGui, this::isOpen, this::activate, shell);
    workbench = new AiAdvisorWorkbench(shell, host);
    workbench.setLayoutData(new FormDataBuilder().fullSize().result());

    HopGuiKeyHandler keyHandler = HopGuiKeyHandler.getInstance();
    keyHandler.addParentObjectToHandle(workbench);
    hopGui.replaceKeyboardShortcutListeners(workbench, keyHandler);
    hopGui.replaceKeyboardShortcutListeners(shell, keyHandler);

    shell.addDisposeListener(
        e -> {
          props.setScreen(new WindowProperty(shell));
          if (hopGui.getShell() != null
              && !hopGui.getShell().isDisposed()
              && hopGui.getShell().getData(SHELL_DATA_KEY) == this) {
            hopGui.getShell().setData(SHELL_DATA_KEY, null);
          }
        });

    restoreSize();
    shell.open();
  }

  private void restoreSize() {
    shell.setMinimumSize(MIN_WIDTH, MIN_HEIGHT);
    WindowProperty windowProperty = props.getScreen(shell.getText());
    if (windowProperty != null) {
      windowProperty.setShell(shell, MIN_WIDTH, MIN_HEIGHT);
      return;
    }
    Rectangle parentBounds = hopGui.getShell().getBounds();
    int width = Math.max(MIN_WIDTH, (int) (parentBounds.width * 0.8));
    int height = Math.max(MIN_HEIGHT, (int) (parentBounds.height * 0.8));
    int x = parentBounds.x + Math.max(0, (parentBounds.width - width) / 2);
    int y = parentBounds.y + Math.max(0, (parentBounds.height - height) / 2);
    shell.setBounds(x, y, width, height);
  }
}
