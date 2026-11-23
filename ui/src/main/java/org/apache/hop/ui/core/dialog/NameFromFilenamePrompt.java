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

package org.apache.hop.ui.core.dialog;

import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

/**
 * After a file is picked in a transform or action dialog (pipeline or workflow executor, pipeline
 * or workflow action), offer to give the transform or action the name of that file.
 *
 * <p>Nothing is asked when the name is already the file name, or when the question was turned off
 * (with "Don't ask again", or in the Configuration perspective).
 */
public final class NameFromFilenamePrompt {

  private static final Class<?> PKG = BaseDialog.class;

  private NameFromFilenamePrompt() {}

  /**
   * Ask whether {@code nameField} should get the base name of {@code filename}, and set it if so.
   *
   * @param shell the dialog shell
   * @param variables to resolve {@code filename}
   * @param filename the picked file, may contain variables
   * @param nameField the name field of the transform or action dialog
   * @param action true for an action, false for a transform
   */
  public static void askToReplaceName(
      Shell shell, IVariables variables, String filename, Text nameField, boolean action) {
    if (Utils.isEmpty(filename) || nameField == null || nameField.isDisposed()) {
      return;
    }
    PropsUi props = PropsUi.getInstance();
    if (!props.isAskingToRenameFromFilename()) {
      return;
    }

    String baseName;
    try {
      baseName =
          HopVfs.getFileObject(variables.resolve(filename), variables).getName().getBaseName();
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "NameFromFilenamePrompt.Error.Title"),
          BaseMessages.getString(PKG, "NameFromFilenamePrompt.Error.Message", filename),
          e);
      return;
    }
    if (Utils.isEmpty(baseName) || baseName.equals(nameField.getText())) {
      return;
    }

    MessageDialogWithToggle dialog =
        new MessageDialogWithToggle(
            shell,
            BaseMessages.getString(PKG, "NameFromFilenamePrompt.Title"),
            BaseMessages.getString(
                PKG,
                action
                    ? "NameFromFilenamePrompt.Action.Message"
                    : "NameFromFilenamePrompt.Transform.Message",
                baseName),
            SWT.ICON_QUESTION,
            new String[] {
              BaseMessages.getString(PKG, "System.Button.Yes"),
              BaseMessages.getString(PKG, "System.Button.No")
            },
            BaseMessages.getString(PKG, "NameFromFilenamePrompt.DontAskAgain"),
            false);
    int answer = dialog.open();
    if (dialog.getToggleState()) {
      props.setAskingToRenameFromFilename(false);
    }
    if (answer == 0) {
      nameField.setText(baseName);
    }
  }
}
