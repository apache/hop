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

import org.apache.hop.core.Const;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.tab.GuiTab;
import org.apache.hop.core.security.HopSecurityConfig;
import org.apache.hop.core.security.HopUserStore;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.hopgui.perspective.configuration.tabs.ConfigSecurityTab;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Text;

/** Authentication mode and login welcome message. */
@GuiPlugin
public class ConfigSecurityGeneralTab implements ISecurityConfigSection {

  private static final Class<?> PKG = ConfigSecurityTab.class;

  private Combo wMode;
  private Text wWelcome;

  public ConfigSecurityGeneralTab() {
    // Instantiated by ConfigSecurityTab / @GuiTab system
  }

  @GuiTab(
      id = "10301-security-general",
      parentId = ConfigSecurityTab.SECURITY_CONFIG_TABS,
      description = "General security settings")
  public void addGeneralTab(CTabFolder wTabFolder) {
    int margin = PropsUi.getMargin();
    int mid = PropsUi.getInstance().getMiddlePct();
    Composite content =
        SecurityConfigUi.createTabContent(wTabFolder, "ConfigSecurityTab.General.Tab");

    Label wlMode = new Label(content, SWT.RIGHT);
    wlMode.setText(BaseMessages.getString(PKG, "ConfigSecurityTab.Mode.Label"));
    PropsUi.setLook(wlMode);
    FormData fdlMode = new FormData();
    fdlMode.left = new FormAttachment(0, 0);
    fdlMode.top = new FormAttachment(0, 0);
    fdlMode.right = new FormAttachment(mid, 0);
    wlMode.setLayoutData(fdlMode);

    wMode = new Combo(content, SWT.BORDER | SWT.READ_ONLY);
    PropsUi.setLook(wMode);
    wMode.setItems(SecurityConfigUi.AUTH_MODES);
    FormData fdMode = new FormData();
    fdMode.left = new FormAttachment(mid, margin);
    fdMode.top = new FormAttachment(0, 0);
    fdMode.right = new FormAttachment(100, 0);
    wMode.setLayoutData(fdMode);
    Control last = wMode;

    last = SecurityConfigUi.addHint(content, last, "ConfigSecurityTab.Mode.Hint", margin);

    Label wlWelcome = new Label(content, SWT.RIGHT);
    wlWelcome.setText(BaseMessages.getString(PKG, "ConfigSecurityTab.Welcome.Label"));
    PropsUi.setLook(wlWelcome);
    FormData fdlWelcome = new FormData();
    fdlWelcome.left = new FormAttachment(0, 0);
    fdlWelcome.top = new FormAttachment(last, margin * 2);
    fdlWelcome.right = new FormAttachment(mid, 0);
    wlWelcome.setLayoutData(fdlWelcome);

    wWelcome = new Text(content, SWT.BORDER | SWT.MULTI | SWT.WRAP | SWT.V_SCROLL);
    PropsUi.setLook(wWelcome);
    wWelcome.setToolTipText(BaseMessages.getString(PKG, "ConfigSecurityTab.Welcome.Tooltip"));
    FormData fdWelcome = new FormData();
    fdWelcome.left = new FormAttachment(mid, margin);
    fdWelcome.top = new FormAttachment(last, margin * 2);
    fdWelcome.right = new FormAttachment(100, 0);
    fdWelcome.height = 80;
    wWelcome.setLayoutData(fdWelcome);
    last = wWelcome;

    SecurityConfigUi.addHint(content, last, "ConfigSecurityTab.Welcome.Hint", margin);
    SecurityConfigUi.finishTabLayout(content);
  }

  @Override
  public void loadFrom(HopSecurityConfig config, HopUserStore store) {
    if (wMode != null && !wMode.isDisposed()) {
      wMode.setText(config.getAuthMode().name());
    }
    if (wWelcome != null && !wWelcome.isDisposed()) {
      wWelcome.setText(Const.NVL(config.getWelcomeMessage(), ""));
      wWelcome.setMessage(HopSecurityConfig.DEFAULT_WELCOME_MESSAGE);
    }
  }

  @Override
  public void applyTo(HopSecurityConfig config) {
    if (wMode != null && !wMode.isDisposed()) {
      config.setAuthMode(HopSecurityConfig.AuthMode.fromString(wMode.getText()));
    }
    if (wWelcome != null && !wWelcome.isDisposed()) {
      config.setWelcomeMessage(wWelcome.getText());
    }
  }

  public String getSelectedMode() {
    return wMode != null && !wMode.isDisposed() ? wMode.getText() : "";
  }
}
