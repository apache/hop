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

package org.apache.hop.ui.hopgui.vfs.explorer;

import org.apache.commons.lang3.StringUtils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.HopGuiKeyHandler;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Shell;

/**
 * Non-modal floating window hosting a {@link VfsFileExplorer}. One instance per Hop Gui session.
 */
public class VfsFileExplorerDialog {

  public static final Class<?> PKG = VfsFileExplorer.class;

  static final String SHELL_DATA_KEY = VfsFileExplorerDialog.class.getName();

  private static final int MIN_WIDTH = 720;
  private static final int MIN_HEIGHT = 480;

  private final HopGui hopGui;
  private final PropsUi props;
  private Shell shell;
  private VfsFileExplorer explorer;

  VfsFileExplorerDialog(HopGui hopGui) {
    this.hopGui = hopGui;
    this.props = PropsUi.getInstance();
  }

  public static void open(HopGui hopGui) {
    openAt(hopGui, null);
  }

  /** Open or focus the window and show {@code location}. A blank location uses the user home. */
  public static void openAt(HopGui hopGui, String location) {
    VfsFileExplorerDialog dialog = openOrCreate(hopGui);
    if (dialog == null || dialog.explorer == null || dialog.explorer.isDisposed()) {
      return;
    }
    dialog.activate();
    if (StringUtils.isNotEmpty(location)) {
      dialog.explorer.openLocation(location);
    }
  }

  static VfsFileExplorerDialog openOrCreate(HopGui hopGui) {
    if (hopGui == null || hopGui.getShell() == null || hopGui.getShell().isDisposed()) {
      return null;
    }
    Object existing = hopGui.getShell().getData(SHELL_DATA_KEY);
    if (existing instanceof VfsFileExplorerDialog dialog && dialog.isOpen()) {
      dialog.activate();
      return dialog;
    }
    VfsFileExplorerDialog dialog = new VfsFileExplorerDialog(hopGui);
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
    if (explorer != null && !explorer.isDisposed()) {
      explorer.activate();
    }
  }

  private void openShell() {
    shell = new Shell(hopGui.getShell(), SWT.SHELL_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    shell.setText(BaseMessages.getString(PKG, "VfsFileExplorer.Dialog.Title"));
    shell.setImage(GuiResource.getInstance().getImageFolder());
    PropsUi.setLook(shell);
    FormLayout layout = new FormLayout();
    layout.marginWidth = PropsUi.getMargin();
    layout.marginHeight = PropsUi.getMargin();
    shell.setLayout(layout);

    explorer = new VfsFileExplorer(shell, hopGui);
    explorer.setLayoutData(new FormDataBuilder().fullSize().result());
    explorer.openLocation(null);
    explorer.activate();

    HopGuiKeyHandler keyHandler = HopGuiKeyHandler.getInstance();
    keyHandler.addParentObjectToHandle(explorer);
    hopGui.replaceKeyboardShortcutListeners(explorer, keyHandler);
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
