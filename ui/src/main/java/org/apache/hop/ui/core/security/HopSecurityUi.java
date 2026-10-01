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

package org.apache.hop.ui.core.security;

import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.security.HopSecurity;
import org.apache.hop.core.security.HopSecurityContext;
import org.apache.hop.core.security.Permission;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;

/**
 * UI helpers for authorization failures (dialogs). Core remains free of SWT; call sites in Hop GUI
 * use this when a mutation or run is blocked.
 */
public final class HopSecurityUi {

  private static final Class<?> PKG = HopSecurityUi.class;

  private HopSecurityUi() {
    // utility
  }

  /**
   * Check a permission; if denied, log and show a warning dialog.
   *
   * @param permission required permission
   * @return true if allowed
   */
  public static boolean check(Permission permission) {
    if (HopSecurity.allows(permission)) {
      return true;
    }
    deny(permission);
    return false;
  }

  /**
   * Show an access-denied dialog for the given permission (and log at basic level).
   *
   * @param permission denied permission
   */
  public static void deny(Permission permission) {
    HopSecurityContext ctx = HopSecurity.getContext();
    String user = ctx != null ? ctx.getUsername() : "?";
    String permId = permission != null ? permission.getId() : "?";
    LogChannel.UI.logBasic("Permission denied for user ''{0}'': {1}", user, permId);

    Shell shell = null;
    try {
      HopGui hopGui = HopGui.getInstance();
      if (hopGui != null) {
        shell = hopGui.getShell();
      }
    } catch (Exception ignored) {
      // no GUI
    }
    if (shell == null || shell.isDisposed()) {
      return;
    }
    MessageBox box = new MessageBox(shell, SWT.ICON_WARNING | SWT.OK);
    box.setText(BaseMessages.getString(PKG, "HopSecurityUi.AccessDenied.Title"));
    box.setMessage(BaseMessages.getString(PKG, "HopSecurityUi.AccessDenied.Message", user, permId));
    box.open();
  }
}
