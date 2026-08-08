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

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.hop.core.Const;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.tab.GuiTab;
import org.apache.hop.core.security.HopSecurityConfig;
import org.apache.hop.core.security.HopUserStore;
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
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.TableItem;

/**
 * EXTERNAL (servlet container / reverse proxy / LDAP groups exposed as roles) and shared
 * container/IdP role → Hop role mappings (also used by OAUTH2 claim groups).
 */
@GuiPlugin
public class ConfigSecurityExternalTab implements ISecurityConfigSection {

  private static final Class<?> PKG = ConfigSecurityTab.class;

  private TableView wRoleMappings;

  public ConfigSecurityExternalTab() {
    // Instantiated by ConfigSecurityTab / @GuiTab system
  }

  @GuiTab(
      id = "10303-security-external",
      parentId = ConfigSecurityTab.SECURITY_CONFIG_TABS,
      description = "External / container role mappings")
  public void addExternalTab(CTabFolder wTabFolder) {
    int margin = PropsUi.getMargin();
    Composite content =
        SecurityConfigUi.createTabContent(wTabFolder, "ConfigSecurityTab.External.Tab");

    Control last =
        SecurityConfigUi.addHint(content, null, "ConfigSecurityTab.External.Hint", margin);

    Label wlTable = new Label(content, SWT.LEFT);
    PropsUi.setLook(wlTable);
    wlTable.setText(BaseMessages.getString(PKG, "ConfigSecurityTab.RoleMappings.Group"));
    FormData fdlTable = new FormData();
    fdlTable.left = new FormAttachment(0, 0);
    fdlTable.top = new FormAttachment(last, margin * 2);
    fdlTable.right = new FormAttachment(100, 0);
    wlTable.setLayoutData(fdlTable);

    ColumnInfo[] mapColumns = {
      new ColumnInfo(
          BaseMessages.getString(PKG, "ConfigSecurityTab.RoleMappings.ContainerRole"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          false),
      new ColumnInfo(
          BaseMessages.getString(PKG, "ConfigSecurityTab.RoleMappings.HopRole"),
          ColumnInfo.COLUMN_TYPE_CCOMBO,
          SecurityConfigUi.HOP_ROLE_IDS,
          false),
    };
    wRoleMappings =
        new TableView(
            Variables.getADefaultVariableSpace(),
            content,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.H_SCROLL | SWT.V_SCROLL,
            mapColumns,
            0,
            null,
            PropsUi.getInstance());
    FormData fdMapTable = new FormData();
    fdMapTable.left = new FormAttachment(0, 0);
    fdMapTable.top = new FormAttachment(wlTable, margin);
    fdMapTable.right = new FormAttachment(100, 0);
    fdMapTable.bottom = new FormAttachment(100, 0);
    wRoleMappings.setLayoutData(fdMapTable);

    SecurityConfigUi.finishTabLayout(content);
  }

  @Override
  public void loadFrom(HopSecurityConfig config, HopUserStore store) {
    if (wRoleMappings == null || wRoleMappings.isDisposed()) {
      return;
    }
    wRoleMappings.clearAll(false);
    Map<String, String> mappings = config.getRoleMappings();
    if (mappings != null) {
      for (Map.Entry<String, String> entry : mappings.entrySet()) {
        TableItem item = new TableItem(wRoleMappings.table, SWT.NONE);
        item.setText(1, Const.NVL(entry.getKey(), ""));
        item.setText(2, Const.NVL(entry.getValue(), ""));
      }
    }
    wRoleMappings.optimizeTableView();
  }

  @Override
  public void applyTo(HopSecurityConfig config) {
    if (wRoleMappings == null || wRoleMappings.isDisposed()) {
      return;
    }
    Map<String, String> mappings = new LinkedHashMap<>();
    for (int i = 0; i < wRoleMappings.nrNonEmpty(); i++) {
      TableItem item = wRoleMappings.getNonEmpty(i);
      String containerRole = item.getText(1).trim();
      String hopRole = item.getText(2).trim();
      if (!containerRole.isEmpty() && !hopRole.isEmpty()) {
        mappings.put(containerRole, hopRole);
      }
    }
    config.setRoleMappings(mappings);
  }
}
