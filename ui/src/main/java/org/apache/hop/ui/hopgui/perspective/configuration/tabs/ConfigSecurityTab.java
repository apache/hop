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

package org.apache.hop.ui.hopgui.perspective.configuration.tabs;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.tab.GuiTab;
import org.apache.hop.core.gui.plugin.tab.GuiTabItem;
import org.apache.hop.core.security.HopSecurity;
import org.apache.hop.core.security.HopSecurityConfig;
import org.apache.hop.core.security.HopSecurityContext;
import org.apache.hop.core.security.HopUserStore;
import org.apache.hop.core.security.Permission;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.configuration.ConfigurationPerspective;
import org.apache.hop.ui.hopgui.perspective.configuration.tabs.security.ISecurityConfigSection;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;

/**
 * Configuration Perspective tab for Hop Web security.
 *
 * <p>Hosts a nested tab folder with built-in sections (General, OAuth, External, Basic) and any
 * plugin-contributed tabs registered under {@link #SECURITY_CONFIG_TABS}.
 *
 * <p>Plugins add a tab with:
 *
 * <pre>
 * &#64;GuiPlugin
 * public class MySecurityTab implements ISecurityConfigSection {
 *   &#64;GuiTab(id = "10390-security-my", parentId = ConfigSecurityTab.SECURITY_CONFIG_TABS, ...)
 *   public void addTab(CTabFolder folder) { ... }
 *   // loadFrom / applyTo / persistSecondary
 * }
 * </pre>
 */
@GuiPlugin
public class ConfigSecurityTab {

  private static final Class<?> PKG = ConfigSecurityTab.class;

  /**
   * Parent id for nested Security sub-tabs. Plugins use this as {@code @GuiTab(parentId = ...)}.
   */
  public static final String SECURITY_CONFIG_TABS = "ConfigSecurityTab.Security.Tabs";

  private Label wlStatus;
  private Button wbSave;
  private Button wbReload;
  private CTabFolder wInnerTabs;
  private final List<ISecurityConfigSection> sections = new ArrayList<>();
  private boolean canManage;

  public ConfigSecurityTab() {
    // Instantiated by the GuiPlugin / @GuiTab system
  }

  @GuiTab(
      id = "10300-config-perspective-security-tab",
      parentId = ConfigurationPerspective.CONFIG_PERSPECTIVE_TABS,
      description = "Security mode, roles and users")
  public void addSecurityTab(CTabFolder wTabFolder) {
    int margin = PropsUi.getMargin();

    CTabItem wTab = new CTabItem(wTabFolder, SWT.NONE);
    wTab.setFont(GuiResource.getInstance().getFontDefault());
    wTab.setText(BaseMessages.getString(PKG, "ConfigSecurityTab.Tab.Name"));
    wTab.setImage(GuiResource.getInstance().getImageLocked());

    Composite outer = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(outer);
    FormLayout layout = new FormLayout();
    layout.marginWidth = PropsUi.getFormMargin();
    layout.marginHeight = PropsUi.getFormMargin();
    outer.setLayout(layout);

    canManage = HopSecurity.allows(Permission.SECURITY_MANAGE);

    wlStatus = new Label(outer, SWT.LEFT | SWT.WRAP);
    PropsUi.setLook(wlStatus);
    FormData fdStatus = new FormData();
    fdStatus.left = new FormAttachment(0, 0);
    fdStatus.top = new FormAttachment(0, 0);
    fdStatus.right = new FormAttachment(100, 0);
    wlStatus.setLayoutData(fdStatus);
    updateStatusLabel();

    if (!canManage) {
      Label wlDenied = new Label(outer, SWT.LEFT | SWT.WRAP);
      PropsUi.setLook(wlDenied);
      wlDenied.setText(BaseMessages.getString(PKG, "ConfigSecurityTab.NoPermission.Message"));
      FormData fdDenied = new FormData();
      fdDenied.left = new FormAttachment(0, 0);
      fdDenied.top = new FormAttachment(wlStatus, margin * 2);
      fdDenied.right = new FormAttachment(100, 0);
      wlDenied.setLayoutData(fdDenied);
      wTab.setControl(outer);
      return;
    }

    wbSave = new Button(outer, SWT.PUSH);
    wbSave.setText(BaseMessages.getString(PKG, "ConfigSecurityTab.Button.Save"));
    PropsUi.setLook(wbSave);
    wbSave.addListener(SWT.Selection, this::save);

    wbReload = new Button(outer, SWT.PUSH);
    wbReload.setText(BaseMessages.getString(PKG, "ConfigSecurityTab.Button.Reload"));
    PropsUi.setLook(wbReload);
    wbReload.addListener(SWT.Selection, e -> reloadFromDisk());

    BaseTransformDialog.positionBottomButtons(outer, new Button[] {wbSave, wbReload}, margin, null);

    wInnerTabs = new CTabFolder(outer, SWT.BORDER);
    PropsUi.setLook(wInnerTabs);
    FormData fdTabs = new FormData();
    fdTabs.left = new FormAttachment(0, 0);
    fdTabs.top = new FormAttachment(wlStatus, margin * 2);
    fdTabs.right = new FormAttachment(100, 0);
    fdTabs.bottom = new FormAttachment(wbSave, -2 * margin);
    wInnerTabs.setLayoutData(fdTabs);

    loadSecuritySubTabs(wInnerTabs);
    reloadFromDisk();

    if (wInnerTabs.getItemCount() > 0) {
      wInnerTabs.setSelection(0);
    }

    wTab.setControl(outer);
  }

  /**
   * Discover and create nested security tabs (built-in + plugins) registered under {@link
   * #SECURITY_CONFIG_TABS}.
   */
  private void loadSecuritySubTabs(CTabFolder folder) {
    sections.clear();
    List<GuiTabItem> tabsList = GuiRegistry.getInstance().findGuiTabItems(SECURITY_CONFIG_TABS);
    if (tabsList == null || tabsList.isEmpty()) {
      return;
    }
    List<GuiTabItem> sorted = new ArrayList<>(tabsList);
    sorted.sort(Comparator.comparing(GuiTabItem::getId));

    for (GuiTabItem tabItem : sorted) {
      try {
        Object object = tabItem.getMethod().getDeclaringClass().getConstructor().newInstance();
        tabItem.getMethod().invoke(object, folder);
        if (object instanceof ISecurityConfigSection section) {
          sections.add(section);
        }
      } catch (Exception e) {
        new ErrorDialog(
            HopGui.getInstance().getShell(),
            BaseMessages.getString(PKG, "ConfigSecurityTab.Error.Title"),
            "Unable to create security sub-tab "
                + tabItem.getId()
                + " via "
                + tabItem.getMethod().getName(),
            e);
      }
    }
  }

  /** Called by ConfigurationPerspective when the perspective is re-activated. */
  public void reloadValues() {
    if (canManage) {
      reloadFromDisk();
    } else {
      updateStatusLabel();
    }
  }

  private void updateStatusLabel() {
    if (wlStatus == null || wlStatus.isDisposed()) {
      return;
    }
    HopSecurityContext ctx = HopSecurity.getContext();
    String user = ctx.getUsername();
    String roles =
        ctx.getRoleIds() == null || ctx.getRoleIds().isEmpty()
            ? "-"
            : String.join(", ", ctx.getRoleIds());
    String mode = HopSecurityConfig.load().getAuthMode().name();
    if (ctx.isUnrestricted()) {
      wlStatus.setText(BaseMessages.getString(PKG, "ConfigSecurityTab.Status.Unrestricted", mode));
    } else {
      wlStatus.setText(
          BaseMessages.getString(PKG, "ConfigSecurityTab.Status.User", user, roles, mode));
    }
  }

  private void reloadFromDisk() {
    try {
      HopSecurityConfig.clearCache();
      HopUserStore.reset();
      HopSecurityConfig config = HopSecurityConfig.load();
      HopUserStore store = HopUserStore.getInstance();
      for (ISecurityConfigSection section : sections) {
        section.loadFrom(config, store);
      }
      updateStatusLabel();
    } catch (Exception e) {
      new ErrorDialog(
          HopGui.getInstance().getShell(),
          BaseMessages.getString(PKG, "ConfigSecurityTab.Error.Title"),
          BaseMessages.getString(PKG, "ConfigSecurityTab.Error.Reload"),
          e);
    }
  }

  private void save(Event event) {
    try {
      if (!HopSecurity.allows(Permission.SECURITY_MANAGE)) {
        MessageBox box = new MessageBox(HopGui.getInstance().getShell(), SWT.ICON_WARNING | SWT.OK);
        box.setText(BaseMessages.getString(PKG, "ConfigSecurityTab.Error.Title"));
        box.setMessage(BaseMessages.getString(PKG, "ConfigSecurityTab.NoPermission.Message"));
        box.open();
        return;
      }

      HopSecurityConfig config = HopSecurityConfig.load();
      for (ISecurityConfigSection section : sections) {
        section.applyTo(config);
      }
      HopSecurityConfig.save(config);
      HopSecurityConfig.clearCache();

      // Re-load after save so secondary persist sees the final mode
      config = HopSecurityConfig.load();
      for (ISecurityConfigSection section : sections) {
        section.persistSecondary(config);
      }

      updateStatusLabel();

      MessageBox box =
          new MessageBox(HopGui.getInstance().getShell(), SWT.ICON_INFORMATION | SWT.OK);
      box.setText(BaseMessages.getString(PKG, "ConfigSecurityTab.Saved.Title"));
      box.setMessage(
          BaseMessages.getString(
              PKG, "ConfigSecurityTab.Saved.Message", config.getAuthMode().name()));
      box.open();
    } catch (Exception e) {
      new ErrorDialog(
          HopGui.getInstance().getShell(),
          BaseMessages.getString(PKG, "ConfigSecurityTab.Error.Title"),
          BaseMessages.getString(PKG, "ConfigSecurityTab.Error.Save"),
          e);
    }
  }
}
