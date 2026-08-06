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

package org.apache.hop.projects.security;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.tab.GuiTab;
import org.apache.hop.core.security.HopSecurityConfig;
import org.apache.hop.core.security.HopUserStore;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.hopgui.perspective.configuration.tabs.ConfigSecurityTab;
import org.apache.hop.ui.hopgui.perspective.configuration.tabs.security.ISecurityConfigSection;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.TableItem;

/**
 * Security sub-tab (Configuration → Security → Projects) to assign projects to users, Hop roles, or
 * container/LDAP groups.
 */
@GuiPlugin
public class ProjectsSecurityTab implements ISecurityConfigSection {

  private static final Class<?> PKG = ProjectsSecurityTab.class;

  private static final String[] SUBJECT_TYPES = {
    ProjectsAccessRule.TYPE_USER, ProjectsAccessRule.TYPE_ROLE, ProjectsAccessRule.TYPE_GROUP
  };
  private static final String[] YES_NO = {"Y", "N"};

  private Button wEnabled;
  private Button wDefaultAllowAll;
  private TableView wRules;

  public ProjectsSecurityTab() {
    // Instantiated by ConfigSecurityTab / @GuiTab system
  }

  @GuiTab(
      id = "10350-security-projects",
      parentId = ConfigSecurityTab.SECURITY_CONFIG_TABS,
      description = "Project access by user, role, or LDAP group")
  public void addProjectsSecurityTab(CTabFolder wTabFolder) {
    int margin = PropsUi.getMargin();

    CTabItem wTab = new CTabItem(wTabFolder, SWT.NONE);
    wTab.setFont(GuiResource.getInstance().getFontDefault());
    wTab.setText(BaseMessages.getString(PKG, "ProjectsSecurityTab.Tab.Name"));
    wTab.setImage(GuiResource.getInstance().getImage("project.svg", PKG.getClassLoader(), 16, 16));

    ScrolledComposite scrolled = new ScrolledComposite(wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL);
    scrolled.setLayout(new FillLayout());

    Composite content = new Composite(scrolled, SWT.NONE);
    PropsUi.setLook(content);
    FormLayout layout = new FormLayout();
    layout.marginWidth = PropsUi.getFormMargin();
    layout.marginHeight = PropsUi.getFormMargin();
    content.setLayout(layout);

    Label wlHint = new Label(content, SWT.LEFT | SWT.WRAP);
    PropsUi.setLook(wlHint);
    wlHint.setText(BaseMessages.getString(PKG, "ProjectsSecurityTab.Hint"));
    FormData fdHint = new FormData();
    fdHint.left = new FormAttachment(0, 0);
    fdHint.top = new FormAttachment(0, 0);
    fdHint.right = new FormAttachment(100, 0);
    wlHint.setLayoutData(fdHint);
    Control last = wlHint;

    wEnabled = new Button(content, SWT.CHECK);
    PropsUi.setLook(wEnabled);
    wEnabled.setText(BaseMessages.getString(PKG, "ProjectsSecurityTab.Enabled"));
    FormData fdEnabled = new FormData();
    fdEnabled.left = new FormAttachment(0, 0);
    fdEnabled.top = new FormAttachment(last, margin * 2);
    wEnabled.setLayoutData(fdEnabled);
    last = wEnabled;

    wDefaultAllowAll = new Button(content, SWT.CHECK);
    PropsUi.setLook(wDefaultAllowAll);
    wDefaultAllowAll.setText(BaseMessages.getString(PKG, "ProjectsSecurityTab.DefaultAllowAll"));
    FormData fdDefault = new FormData();
    fdDefault.left = new FormAttachment(0, 0);
    fdDefault.top = new FormAttachment(last, margin);
    wDefaultAllowAll.setLayoutData(fdDefault);
    last = wDefaultAllowAll;

    Label wlRules = new Label(content, SWT.LEFT);
    PropsUi.setLook(wlRules);
    wlRules.setText(BaseMessages.getString(PKG, "ProjectsSecurityTab.Rules.Label"));
    FormData fdlRules = new FormData();
    fdlRules.left = new FormAttachment(0, 0);
    fdlRules.top = new FormAttachment(last, margin * 2);
    fdlRules.right = new FormAttachment(100, 0);
    wlRules.setLayoutData(fdlRules);
    last = wlRules;

    // Project name combo from registered projects
    String[] projectNames =
        ProjectsConfigSingleton.getConfig().listProjectConfigNames().toArray(new String[0]);

    ColumnInfo[] columns = {
      new ColumnInfo(
          BaseMessages.getString(PKG, "ProjectsSecurityTab.Col.Type"),
          ColumnInfo.COLUMN_TYPE_CCOMBO,
          SUBJECT_TYPES,
          false),
      new ColumnInfo(
          BaseMessages.getString(PKG, "ProjectsSecurityTab.Col.Subject"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          false),
      new ColumnInfo(
          BaseMessages.getString(PKG, "ProjectsSecurityTab.Col.AllProjects"),
          ColumnInfo.COLUMN_TYPE_CCOMBO,
          YES_NO,
          false),
      new ColumnInfo(
          BaseMessages.getString(PKG, "ProjectsSecurityTab.Col.Projects"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          false),
    };
    columns[3].setToolTip(BaseMessages.getString(PKG, "ProjectsSecurityTab.Col.Projects.Tooltip"));
    if (projectNames.length > 0) {
      // Helpful combo values still allow free text (comma-separated list)
      columns[3].setComboValues(projectNames);
    }

    wRules =
        new TableView(
            Variables.getADefaultVariableSpace(),
            content,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.H_SCROLL | SWT.V_SCROLL,
            columns,
            0,
            null,
            PropsUi.getInstance());
    FormData fdRules = new FormData();
    fdRules.left = new FormAttachment(0, 0);
    fdRules.top = new FormAttachment(last, margin);
    fdRules.right = new FormAttachment(100, 0);
    fdRules.bottom = new FormAttachment(100, 0);
    wRules.setLayoutData(fdRules);

    scrolled.setContent(content);
    scrolled.setExpandHorizontal(true);
    scrolled.setExpandVertical(true);
    content.pack();
    scrolled.setMinSize(content.computeSize(SWT.DEFAULT, SWT.DEFAULT));
    wTab.setControl(scrolled);
  }

  @Override
  public void loadFrom(HopSecurityConfig config, HopUserStore store) {
    // projects-access.json is independent of security-config.json
    ProjectsAccessConfig access = ProjectsAccessConfig.load();
    if (wEnabled != null && !wEnabled.isDisposed()) {
      wEnabled.setSelection(access.isEnabled());
    }
    if (wDefaultAllowAll != null && !wDefaultAllowAll.isDisposed()) {
      wDefaultAllowAll.setSelection(access.isDefaultAllowAll());
    }
    if (wRules == null || wRules.isDisposed()) {
      return;
    }
    wRules.clearAll(false);
    List<ProjectsAccessRule> rules = access.getRules();
    if (rules != null) {
      for (ProjectsAccessRule rule : rules) {
        if (rule == null) {
          continue;
        }
        TableItem item = new TableItem(wRules.table, SWT.NONE);
        item.setText(1, Const.NVL(rule.normalizedType(), ProjectsAccessRule.TYPE_USER));
        item.setText(2, Const.NVL(rule.getSubject(), ""));
        item.setText(3, rule.isAllProjects() ? "Y" : "N");
        String projects =
            rule.getProjects() == null
                ? ""
                : rule.getProjects().stream()
                    .filter(StringUtils::isNotEmpty)
                    .collect(Collectors.joining(", "));
        item.setText(4, projects);
      }
    }
    wRules.optimizeTableView();
  }

  @Override
  public void applyTo(HopSecurityConfig config) {
    // Saved in persistSecondary so we always write projects-access.json on Save
  }

  @Override
  public void persistSecondary(HopSecurityConfig config) {
    if (wEnabled == null || wEnabled.isDisposed()) {
      return;
    }
    ProjectsAccessConfig access = new ProjectsAccessConfig();
    access.setEnabled(wEnabled.getSelection());
    access.setDefaultAllowAll(wDefaultAllowAll.getSelection());

    List<ProjectsAccessRule> rules = new ArrayList<>();
    for (int i = 0; i < wRules.nrNonEmpty(); i++) {
      TableItem item = wRules.getNonEmpty(i);
      String type = item.getText(1).trim().toLowerCase(Locale.ROOT);
      if (type.isEmpty()) {
        type = ProjectsAccessRule.TYPE_USER;
      }
      if (!Arrays.asList(SUBJECT_TYPES).contains(type)) {
        throw new IllegalArgumentException(
            BaseMessages.getString(PKG, "ProjectsSecurityTab.Error.BadType", type));
      }
      String subject = item.getText(2).trim();
      if (subject.isEmpty()) {
        continue;
      }
      boolean all = "Y".equalsIgnoreCase(item.getText(3).trim());
      List<String> projects = new ArrayList<>();
      if (!all) {
        String raw = item.getText(4);
        if (StringUtils.isNotEmpty(raw)) {
          for (String part : raw.split("[,;]")) {
            String p = part.trim();
            if (!p.isEmpty()) {
              projects.add(p);
            }
          }
        }
      }
      rules.add(new ProjectsAccessRule(type, subject, all, projects));
    }
    access.setRules(rules);
    ProjectsAccessConfig.save(access);
    ProjectsAccessConfig.clearCache();
    // Re-load so in-memory cache matches disk
    ProjectsAccessConfig.load();
  }
}
