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

package org.apache.hop.setup.gui;

import org.apache.hop.core.Const;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.setup.HopEnvironmentDefaults;
import org.apache.hop.setup.HopEnvironmentSnapshot;
import org.apache.hop.setup.HopSetupVariables;
import org.apache.hop.setup.OsFamily;
import org.apache.hop.setup.UserPaths;
import org.apache.hop.setup.persist.HopVfsFiles;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.welcome.WelcomeDialog;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.SWT;

@ExtensionPoint(
    id = "HopGuiStartSetupPrompt",
    description = "Offer to configure user-level Hop folders on first start",
    extensionPointId = "HopGuiStart")
public class HopGuiStartSetupPrompt implements IExtensionPoint {

  private static final Class<?> PKG = SetupGuiPlugin.class;

  @Override
  public void callExtensionPoint(ILogChannel log, IVariables variables, Object hopGuiObject)
      throws HopException {
    if (!shouldPrompt(variables)) {
      return;
    }
    HopGui hopGui = HopGui.getInstance();
    MessageBox box = new MessageBox(hopGui.getShell(), SWT.YES | SWT.NO | SWT.ICON_QUESTION);
    box.setText(BaseMessages.getString(PKG, "SetupPrompt.Header"));
    box.setMessage(BaseMessages.getString(PKG, "SetupPrompt.Message"));
    int answer = box.open();
    if (answer == SWT.YES) {
      new SetupDialog(hopGui.getShell(), hopGui.getVariables(), true).open();
    } else {
      HopConfig.getInstance().saveOption(HopSetupVariables.CONFIG_OPTION_DO_NOT_SHOW, true);
    }
  }

  static boolean shouldPrompt(IVariables variables) {
    if (EnvironmentUtils.getInstance().isWeb()) {
      return false;
    }
    if (HopEnvironmentSnapshot.configFolderSetInEnvironment()) {
      return false;
    }
    if (Const.toBoolean(System.getenv(HopSetupVariables.NO_SETUP_DIALOG))) {
      return false;
    }
    if (Const.toBoolean(System.getProperty(HopSetupVariables.NO_SETUP_DIALOG))) {
      return false;
    }
    if (variables != null
        && Const.toBoolean(variables.getVariable(WelcomeDialog.VARIABLE_HOP_NO_WELCOME_DIALOG))) {
      return false;
    }
    if (HopConfig.readOptionBoolean(HopSetupVariables.CONFIG_OPTION_DO_NOT_SHOW, false)) {
      return false;
    }
    try {
      String envFile =
          HopEnvironmentDefaults.wellKnownEnvFile(OsFamily.detect(), UserPaths.system());
      if (HopVfsFiles.exists(envFile)) {
        return false;
      }
    } catch (Exception e) {
      // If we cannot check the file, still offer the prompt.
    }
    return true;
  }
}
