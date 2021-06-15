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

/*
 * Hop : The Hop Orchestration Platform
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.git.model;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hop.ui.core.dialog.EnterStringDialog;
import org.apache.hop.ui.hopgui.perspective.HopPerspectivePlugin;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;

public abstract class VCS {

  protected static final Class<?> PKG = HopPerspectivePlugin.class; // For Translator

  public static final String WORKINGTREE = "WORKINGTREE";
  public static final String INDEX = "INDEX";
  public static final String TYPE_TAG = "tag";
  public static final String TYPE_BRANCH = "branch";
  public static final String TYPE_COMMIT = "commit";

  protected Shell shell;
  protected String directory;

  @VisibleForTesting
  void showMessageBox(String title, String message) {
    MessageBox messageBox = new MessageBox(shell, SWT.OK);
    messageBox.setText(title);
    messageBox.setMessage(message == null ? "" : message);
    messageBox.open();
  }

  /**
   * Prompt the user to set username and password
   *
   * @return true on success
   */
  protected boolean promptUsernamePassword() {
    EnterStringDialog userDialog =
        new EnterStringDialog(shell, "", "Username?", "Enter the git username to use");
    String username = userDialog.open();
    if (username == null) {
      return false;
    }

    EnterStringDialog passDialog =
        new EnterStringDialog(shell, "", "Password?", "Enter the git password to use");
    passDialog.setEchoChar('*');
    String password = passDialog.open();
    if (password == null) {
      return false;
    }

    setCredential(username, password);
    return true;
  }

  public abstract void setCredential(String username, String password);
}
