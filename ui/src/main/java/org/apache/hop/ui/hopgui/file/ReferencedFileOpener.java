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

package org.apache.hop.ui.hopgui.file;

import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.security.HopDialogEditGuard;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Shell;

/**
 * Opens the pipeline or workflow selected in a transform or action dialog.
 *
 * <p>When the dialog has unsaved edits, the user is asked to save them first. Saving applies the
 * dialog (the same as OK) and then opens the file. Declining opens the file and leaves the dialog
 * open. Cancelling does nothing.
 */
public final class ReferencedFileOpener {
  private static final Class<?> PKG = ReferencedFileOpener.class;

  private ReferencedFileOpener() {}

  /** What to do after the optional save question. */
  public enum Decision {
    APPLY_AND_OPEN,
    OPEN,
    CANCEL
  }

  /**
   * @param modified whether the dialog has edits that OK would keep
   * @param answer a {@link SWT} yes/no/cancel code; ignored when {@code modified} is false
   */
  public static Decision decide(boolean modified, int answer) {
    if (!modified) {
      return Decision.OPEN;
    }
    if (answer == SWT.YES) {
      return Decision.APPLY_AND_OPEN;
    }
    if (answer == SWT.NO) {
      return Decision.OPEN;
    }
    return Decision.CANCEL;
  }

  /** True when {@code filename} resolves to a non-blank path. */
  public static boolean hasOpenableFilename(IVariables variables, String filename) {
    if (variables == null || StringUtils.isBlank(filename)) {
      return false;
    }
    return StringUtils.isNotBlank(variables.resolve(filename));
  }

  /**
   * True when the dialog metadata is already marked changed, or the filename field differs from the
   * value stored on the transform or action.
   */
  public static boolean isDialogModified(
      boolean changed, String currentFilename, String storedFilename) {
    if (changed) {
      return true;
    }
    return !StringUtils.equals(
        StringUtils.defaultString(currentFilename), StringUtils.defaultString(storedFilename));
  }

  /** Label, tooltip and read-only behaviour shared by the Open buttons. */
  public static void configureOpenButton(Button button) {
    PropsUi.setLook(button);
    button.setText(BaseMessages.getString(PKG, "System.Button.Open"));
    button.setToolTipText(BaseMessages.getString(PKG, "ReferencedFileOpener.Open.Tooltip"));
    BaseDialog.keepEnabledInReadOnly(button);
  }

  /**
   * Open {@code filename} from a dialog.
   *
   * @param apply saves the dialog and returns the filename to open, or {@code null} when saving did
   *     not complete
   */
  public static void openFromDialog(
      Shell shell,
      IVariables variables,
      String filename,
      boolean modified,
      Supplier<String> apply) {
    if (!hasOpenableFilename(variables, filename)) {
      MessageBox box = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      box.setText(BaseMessages.getString(PKG, "ReferencedFileOpener.FilenameMissing.Title"));
      box.setMessage(BaseMessages.getString(PKG, "ReferencedFileOpener.FilenameMissing.Message"));
      box.open();
      return;
    }
    if (isReadOnly(shell)) {
      modified = false;
    }
    Decision decision = decide(modified, modified ? askToSave(shell) : SWT.NONE);
    if (decision == Decision.CANCEL) {
      return;
    }
    String toOpen = filename;
    if (decision == Decision.APPLY_AND_OPEN) {
      toOpen = apply == null ? null : apply.get();
      if (!hasOpenableFilename(variables, toOpen)) {
        return;
      }
    }
    openResolved(variables.resolve(toOpen), shell);
  }

  private static boolean isReadOnly(Shell shell) {
    if (shell == null || shell.isDisposed()) {
      return false;
    }
    return HopDialogEditGuard.isReadOnly(shell.getData(BaseDialog.DIALOG_SUBJECT));
  }

  private static int askToSave(Shell shell) {
    MessageBox box = new MessageBox(shell, SWT.YES | SWT.NO | SWT.CANCEL | SWT.ICON_QUESTION);
    box.setText(BaseMessages.getString(PKG, "ReferencedFileOpener.SaveChanges.Title"));
    box.setMessage(BaseMessages.getString(PKG, "ReferencedFileOpener.SaveChanges.Message"));
    return box.open();
  }

  private static void openResolved(String filename, Shell dialogShell) {
    try {
      HopGui hopGui = HopGui.getInstance();
      ExplorerPerspective perspective = HopGui.getExplorerPerspective();
      if (perspective != null) {
        IHopFileTypeHandler existing = perspective.findFileTypeHandlerByFilename(filename);
        if (existing != null) {
          perspective.setActiveFileTypeHandler(existing);
          perspective.activate();
          return;
        }
      }
      hopGui.fileDelegate.fileOpen(filename);
    } catch (Exception e) {
      Shell parent = parentShell(dialogShell);
      if (parent == null) {
        return;
      }
      new ErrorDialog(
          parent,
          BaseMessages.getString(PKG, "ReferencedFileOpener.ErrorOpening.Title"),
          BaseMessages.getString(PKG, "ReferencedFileOpener.ErrorOpening.Message", filename),
          e);
    }
  }

  private static Shell parentShell(Shell dialogShell) {
    if (dialogShell != null && !dialogShell.isDisposed()) {
      return dialogShell;
    }
    HopGui hopGui = HopGui.getInstance();
    if (hopGui == null || hopGui.getShell() == null || hopGui.getShell().isDisposed()) {
      return null;
    }
    return hopGui.getShell();
  }
}
