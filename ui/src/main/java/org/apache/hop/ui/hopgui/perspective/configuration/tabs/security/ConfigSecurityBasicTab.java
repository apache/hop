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

package org.apache.hop.ui.hopgui.perspective.configuration.tabs.security;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hop.core.Const;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.tab.GuiTab;
import org.apache.hop.core.security.HopRole;
import org.apache.hop.core.security.HopSecurityConfig;
import org.apache.hop.core.security.HopUser;
import org.apache.hop.core.security.HopUserStore;
import org.apache.hop.core.security.PasswordHasher;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.hopgui.perspective.configuration.tabs.ConfigSecurityTab;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.TableItem;

/** BASIC authentication users (users.json). */
@GuiPlugin
public class ConfigSecurityBasicTab implements ISecurityConfigSection {

  private static final Class<?> PKG = ConfigSecurityTab.class;

  private TableView wUsers;

  public ConfigSecurityBasicTab() {
    // Instantiated by ConfigSecurityTab / @GuiTab system
  }

  @GuiTab(
      id = "10304-security-basic",
      parentId = ConfigSecurityTab.SECURITY_CONFIG_TABS,
      description = "BASIC authentication users")
  public void addBasicTab(CTabFolder wTabFolder) {
    int margin = PropsUi.getMargin();
    Composite content =
        SecurityConfigUi.createTabContent(wTabFolder, "ConfigSecurityTab.Basic.Tab");

    Control last = SecurityConfigUi.addHint(content, null, "ConfigSecurityTab.Users.Hint", margin);

    ColumnInfo[] userColumns = {
      new ColumnInfo(
          BaseMessages.getString(PKG, "ConfigSecurityTab.Users.Username"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          false),
      new ColumnInfo(
          BaseMessages.getString(PKG, "ConfigSecurityTab.Users.Roles"),
          ColumnInfo.COLUMN_TYPE_CCOMBO,
          SecurityConfigUi.HOP_ROLE_IDS,
          false),
      new ColumnInfo(
          BaseMessages.getString(PKG, "ConfigSecurityTab.Users.Enabled"),
          ColumnInfo.COLUMN_TYPE_CCOMBO,
          SecurityConfigUi.YES_NO,
          false),
      new ColumnInfo(
          BaseMessages.getString(PKG, "ConfigSecurityTab.Users.NewPassword"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          false),
    };
    userColumns[3].setPasswordField(true);

    wUsers =
        new TableView(
            Variables.getADefaultVariableSpace(),
            content,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.H_SCROLL | SWT.V_SCROLL,
            userColumns,
            0,
            null,
            PropsUi.getInstance());
    FormData fdUserTable = new FormData();
    fdUserTable.left = new FormAttachment(0, 0);
    fdUserTable.top = new FormAttachment(last, margin);
    fdUserTable.right = new FormAttachment(100, 0);
    fdUserTable.bottom = new FormAttachment(100, 0);
    wUsers.setLayoutData(fdUserTable);

    SecurityConfigUi.finishTabLayout(content);
  }

  @Override
  public void loadFrom(HopSecurityConfig config, HopUserStore store) {
    if (wUsers == null || wUsers.isDisposed()) {
      return;
    }
    wUsers.clearAll(false);
    for (HopUser user : store.listUsers()) {
      TableItem item = new TableItem(wUsers.table, SWT.NONE);
      item.setText(1, Const.NVL(user.getUsername(), ""));
      String roles =
          user.getRoles() == null ? "" : user.getRoles().stream().collect(Collectors.joining(","));
      String primary =
          user.getRoles() != null && !user.getRoles().isEmpty()
              ? user.getRoles().get(0)
              : HopRole.USER.getId();
      item.setText(2, primary);
      item.setText(3, user.isEnabled() ? "Y" : "N");
      item.setText(4, "");
      item.setData("roles", roles);
    }
    wUsers.optimizeTableView();
  }

  @Override
  public void applyTo(HopSecurityConfig config) {
    // Users are persisted separately in persistSecondary when mode is BASIC
  }

  @Override
  public void persistSecondary(HopSecurityConfig config) throws Exception {
    if (config.getAuthMode() != HopSecurityConfig.AuthMode.BASIC) {
      return;
    }
    if (wUsers == null || wUsers.isDisposed()) {
      return;
    }
    HopUserStore store = HopUserStore.getInstance();
    Map<String, HopUser> existing = new LinkedHashMap<>();
    for (HopUser u : store.listUsers()) {
      if (u.getUsername() != null) {
        existing.put(u.getUsername().toLowerCase(), u);
      }
    }

    List<HopUser> next = new ArrayList<>();
    for (int i = 0; i < wUsers.nrNonEmpty(); i++) {
      TableItem item = wUsers.getNonEmpty(i);
      String username = item.getText(1).trim();
      if (username.isEmpty()) {
        continue;
      }
      String roleId = item.getText(2).trim();
      if (roleId.isEmpty()) {
        roleId = HopRole.USER.getId();
      }
      HopRole hopRole = HopRole.fromIdOrAlias(roleId);
      if (hopRole == null) {
        throw new IllegalArgumentException("Unknown role: " + roleId);
      }
      boolean enabled = !"N".equalsIgnoreCase(item.getText(3).trim());
      String newPassword = item.getText(4);

      HopUser prev = existing.get(username.toLowerCase());
      String hash;
      if (newPassword != null && !newPassword.isEmpty()) {
        hash = PasswordHasher.hash(newPassword);
      } else if (prev != null && prev.getPasswordHash() != null) {
        hash = prev.getPasswordHash();
      } else {
        throw new IllegalArgumentException(
            BaseMessages.getString(PKG, "ConfigSecurityTab.Users.PasswordRequired", username));
      }

      HopUser user = new HopUser(username, hash, List.of(hopRole.getId()));
      user.setEnabled(enabled);
      next.add(user);
    }

    if (next.isEmpty()) {
      throw new IllegalArgumentException(
          BaseMessages.getString(PKG, "ConfigSecurityTab.Users.EmptyStore"));
    }

    store.replaceAllUsers(next);
  }
}
