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

package org.apache.hop.git.model;

import org.apache.hop.ui.core.dialog.EnterUsernamePasswordDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;

public abstract class VCS {

  protected static final Class<?> PKG = UIGit.class;

  public static final String WORKINGTREE = "WORKINGTREE";
  public static final String INDEX = "INDEX";
  public static final String TYPE_TAG = "tag";
  public static final String TYPE_BRANCH = "branch";
  public static final String TYPE_COMMIT = "commit";

  protected String directory;

  void showMessageBox(String title, String message) {
    MessageBox messageBox = new MessageBox(HopGui.getInstance().getShell(), SWT.OK);
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
    EnterUsernamePasswordDialog userDialog =
        new EnterUsernamePasswordDialog(HopGui.getInstance().getShell());
    String[] usernamePassword = userDialog.open();
    if (usernamePassword == null) {
      return false;
    }

    setCredential(usernamePassword[0], usernamePassword[1]);
    return true;
  }

  public abstract void setCredential(String username, String password);
}
