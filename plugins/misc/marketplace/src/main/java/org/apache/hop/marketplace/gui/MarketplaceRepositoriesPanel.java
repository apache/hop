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

package org.apache.hop.marketplace.gui;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.marketplace.catalog.OptionalPluginInfo;
import org.apache.hop.marketplace.config.MarketplaceConfig;
import org.apache.hop.marketplace.config.MarketplaceRepository;
import org.apache.hop.marketplace.config.MarketplaceRepositoryDefinition;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

/**
 * Marketplace repository list editor hosted as a panel (e.g. Marketplace dialog Repositories tab).
 * Settings are saved to hop-config.json via {@link MarketplaceConfig#save()}.
 *
 * <p>Plugin metadata for a repository is edited in the Add/Edit repository dialog (Plugin metadata
 * tab). Import/Export shareable hop-marketplace-repo YAML definitions from this panel.
 */
public class MarketplaceRepositoriesPanel {

  private static final Class<?> PKG = MarketplaceGuiPlugin.class;

  private MarketplaceConfig config;
  private final Shell shell;
  private final TableView wTable;
  private boolean dirty;

  /**
   * Build the repositories UI into {@code parent} (must use {@link FormLayout}).
   *
   * @param parent tab composite
   * @param config shared marketplace config (mutated in place; call Save to persist)
   */
  public MarketplaceRepositoriesPanel(Composite parent, MarketplaceConfig config) {
    PropsUi props = PropsUi.getInstance();
    this.config = config;
    this.shell = parent.getShell();
    this.dirty = false;

    Label wlHelp = new Label(parent, SWT.LEFT | SWT.WRAP);
    PropsUi.setLook(wlHelp);
    wlHelp.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Help"));
    FormData fdlHelp = new FormData();
    fdlHelp.left = new FormAttachment(0, 0);
    fdlHelp.top = new FormAttachment(0, 0);
    fdlHelp.right = new FormAttachment(100, 0);
    wlHelp.setLayoutData(fdlHelp);

    Button wSave = createRightButton(parent, "ManageRepositoriesDialog.Button.Save");
    wSave.addListener(SWT.Selection, e -> save());
    Button wReset = createRightButton(parent, "ManageRepositoriesDialog.Button.ResetDefaults");
    wReset.addListener(SWT.Selection, e -> resetDefaults());
    Button wDown = createRightButton(parent, "ManageRepositoriesDialog.Button.MoveDown");
    wDown.addListener(SWT.Selection, e -> moveSelected(1));
    Button wUp = createRightButton(parent, "ManageRepositoriesDialog.Button.MoveUp");
    wUp.addListener(SWT.Selection, e -> moveSelected(-1));
    Button wPrimary = createRightButton(parent, "ManageRepositoriesDialog.Button.SetPrimary");
    wPrimary.addListener(SWT.Selection, e -> setPrimarySelected());
    Button wExport = createRightButton(parent, "ManageRepositoriesDialog.Button.Export");
    wExport.addListener(SWT.Selection, e -> exportDefinition());
    Button wImport = createRightButton(parent, "ManageRepositoriesDialog.Button.Import");
    wImport.addListener(SWT.Selection, e -> importDefinition());
    Button wRemove = createRightButton(parent, "ManageRepositoriesDialog.Button.Remove");
    wRemove.addListener(SWT.Selection, e -> removeSelected());
    Button wEdit = createRightButton(parent, "ManageRepositoriesDialog.Button.Edit");
    wEdit.addListener(SWT.Selection, e -> editSelected());
    Button wAdd = createRightButton(parent, "ManageRepositoriesDialog.Button.Add");
    wAdd.addListener(SWT.Selection, e -> addRepository());

    Button[] rightButtons = {
      wAdd, wEdit, wRemove, wImport, wExport, wPrimary, wUp, wDown, wReset, wSave
    };
    layoutRightButtons(rightButtons, wlHelp);

    ColumnInfo[] columns = {
      new ColumnInfo(
          BaseMessages.getString(PKG, "ManageRepositoriesDialog.Column.Primary"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          true),
      new ColumnInfo(
          BaseMessages.getString(PKG, "ManageRepositoriesDialog.Column.Enabled"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          true),
      new ColumnInfo(
          BaseMessages.getString(PKG, "ManageRepositoriesDialog.Column.Browse"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          true),
      new ColumnInfo(
          BaseMessages.getString(PKG, "ManageRepositoriesDialog.Column.Id"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          true),
      new ColumnInfo(
          BaseMessages.getString(PKG, "ManageRepositoriesDialog.Column.Name"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          true),
      new ColumnInfo(
          BaseMessages.getString(PKG, "ManageRepositoriesDialog.Column.Url"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          true),
      new ColumnInfo(
          BaseMessages.getString(PKG, "ManageRepositoriesDialog.Column.Auth"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          true),
    };
    wTable =
        new TableView(
            Variables.getADefaultVariableSpace(),
            parent,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.SINGLE | SWT.V_SCROLL,
            columns,
            1,
            true,
            null,
            props,
            false);
    PropsUi.setLook(wTable);
    FormData fdTable = new FormData();
    fdTable.left = new FormAttachment(0, 0);
    fdTable.top = new FormAttachment(wlHelp, PropsUi.getMargin() * 2);
    fdTable.right = new FormAttachment(wAdd, -PropsUi.getMargin());
    fdTable.bottom = new FormAttachment(100, 0);
    wTable.setLayoutData(fdTable);

    refreshTable();
  }

  private static Button createRightButton(Composite parent, String messageKey) {
    Button b = new Button(parent, SWT.PUSH);
    b.setText(BaseMessages.getString(PKG, messageKey));
    PropsUi.setLook(b);
    return b;
  }

  private void layoutRightButtons(Button[] buttons, Label wlHelp) {
    int margin = PropsUi.getMargin();
    int maxW = 80;
    for (Button b : buttons) {
      maxW = Math.max(maxW, b.computeSize(SWT.DEFAULT, SWT.DEFAULT).x);
    }
    Button prev = null;
    for (Button b : buttons) {
      FormData fd = new FormData();
      fd.right = new FormAttachment(100, 0);
      fd.left = new FormAttachment(100, -maxW - 8);
      if (prev == null) {
        fd.top = new FormAttachment(wlHelp, margin * 2);
      } else {
        fd.top = new FormAttachment(prev, margin);
      }
      b.setLayoutData(fd);
      prev = b;
    }
  }

  public boolean isDirty() {
    return dirty;
  }

  public boolean saveChanges(boolean showSuccessDialog) {
    try {
      config.ensureValidPrimary();
      config.save();
      dirty = false;
      if (showSuccessDialog) {
        MessageBox box = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
        box.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Save.Done.Header"));
        box.setMessage(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Save.Done.Message"));
        box.open();
      }
      return true;
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "ManageRepositoriesDialog.Error.Header"),
          BaseMessages.getString(PKG, "ManageRepositoriesDialog.Error.Save"),
          e);
      return false;
    }
  }

  public void setConfig(MarketplaceConfig config) {
    this.config = config;
    this.dirty = false;
    refreshTable();
  }

  public MarketplaceConfig getConfig() {
    return config;
  }

  private void markDirty() {
    dirty = true;
  }

  private void refreshTable() {
    wTable.table.removeAll();
    if (config.getRepositories() == null) {
      wTable.optimizeTableView();
      return;
    }
    for (MarketplaceRepository repo : config.getRepositories()) {
      if (repo == null) {
        continue;
      }
      TableItem item = new TableItem(wTable.table, SWT.NONE);
      item.setText(1, repo.isPrimary() ? "*" : "");
      item.setText(2, repo.isEnabled() ? "Y" : "N");
      item.setText(3, repo.isBrowse() ? "Y" : "N");
      item.setText(4, Const.NVL(repo.getId(), ""));
      item.setText(5, Const.NVL(repo.displayName(), ""));
      item.setText(6, Const.NVL(repo.getUrl(), ""));
      item.setText(
          7, repo.hasCredentials() || StringUtils.isNotBlank(repo.getUsername()) ? "Y" : "");
      item.setData(repo);
    }
    wTable.optimizeTableView();
  }

  private MarketplaceRepository selected() {
    TableItem[] items = wTable.table.getSelection();
    if (items == null || items.length == 0) {
      return null;
    }
    return (MarketplaceRepository) items[0].getData();
  }

  private void addRepository() {
    MarketplaceRepository repo = new MarketplaceRepository();
    repo.setId("");
    repo.setName("");
    repo.setUrl("");
    repo.setEnabled(true);
    if (editRepository(repo, true)) {
      try {
        config.addRepository(repo);
        markDirty();
        refreshTable();
        selectRepoId(repo.getId());
      } catch (Exception e) {
        new ErrorDialog(
            shell,
            BaseMessages.getString(PKG, "ManageRepositoriesDialog.Error.Header"),
            e.getMessage(),
            e);
      }
    }
  }

  private void editSelected() {
    MarketplaceRepository repo = selected();
    if (repo == null) {
      return;
    }
    if (editRepository(repo, false)) {
      config.ensureValidPrimary();
      markDirty();
      refreshTable();
      selectRepoId(repo.getId());
    }
  }

  private void selectRepoId(String id) {
    if (StringUtils.isBlank(id) || config.getRepositories() == null) {
      return;
    }
    for (int i = 0; i < wTable.table.getItemCount(); i++) {
      MarketplaceRepository r = (MarketplaceRepository) wTable.table.getItem(i).getData();
      if (r != null && id.equals(r.getId())) {
        wTable.table.setSelection(i);
        break;
      }
    }
  }

  /**
   * Add/Edit repository dialog: General connection fields + Plugin metadata table (for this repo
   * only). Mutations applied only on OK.
   */
  private boolean editRepository(MarketplaceRepository repo, boolean isNew) {
    Shell dialog = new Shell(shell, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.RESIZE | SWT.MAX);
    PropsUi props = PropsUi.getInstance();
    PropsUi.setLook(dialog);
    dialog.setText(
        BaseMessages.getString(
            PKG,
            isNew
                ? "ManageRepositoriesDialog.Edit.Title.Add"
                : "ManageRepositoriesDialog.Edit.Title.Edit"));
    FormLayout layout = new FormLayout();
    layout.marginWidth = PropsUi.getFormMargin();
    layout.marginHeight = PropsUi.getFormMargin();
    dialog.setLayout(layout);

    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();

    final boolean[] ok = {false};

    Button wOk = new Button(dialog, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Button.Ok"));
    Button wCancel = new Button(dialog, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Button.Cancel"));
    BaseTransformDialog.positionBottomButtons(dialog, new Button[] {wOk, wCancel}, margin, null);

    CTabFolder tabs = new CTabFolder(dialog, SWT.BORDER);
    PropsUi.setLook(tabs);
    FormData fdTabs = new FormData();
    fdTabs.left = new FormAttachment(0, 0);
    fdTabs.top = new FormAttachment(0, 0);
    fdTabs.right = new FormAttachment(100, 0);
    fdTabs.bottom = new FormAttachment(wOk, -margin);
    tabs.setLayoutData(fdTabs);

    // ----- General -----
    CTabItem generalTab = new CTabItem(tabs, SWT.NONE);
    generalTab.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Edit.Tab.General"));
    Composite general = new Composite(tabs, SWT.NONE);
    PropsUi.setLook(general);
    general.setLayout(new FormLayout());
    generalTab.setControl(general);

    Label wlId = new Label(general, SWT.RIGHT);
    PropsUi.setLook(wlId);
    wlId.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Edit.Id"));
    FormData fdlId = new FormData();
    fdlId.left = new FormAttachment(0, 0);
    fdlId.top = new FormAttachment(0, margin);
    fdlId.right = new FormAttachment(middle, -margin);
    wlId.setLayoutData(fdlId);
    Text wId = new Text(general, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wId);
    wId.setText(Const.NVL(repo.getId(), ""));
    wId.setEditable(isNew);
    FormData fdId = new FormData();
    fdId.left = new FormAttachment(middle, margin);
    fdId.top = new FormAttachment(wlId, 0, SWT.CENTER);
    fdId.right = new FormAttachment(100, 0);
    wId.setLayoutData(fdId);

    Label wlName = new Label(general, SWT.RIGHT);
    PropsUi.setLook(wlName);
    wlName.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Edit.Name"));
    FormData fdlName = new FormData();
    fdlName.left = new FormAttachment(0, 0);
    fdlName.top = new FormAttachment(wId, margin);
    fdlName.right = new FormAttachment(middle, -margin);
    wlName.setLayoutData(fdlName);
    Text wName = new Text(general, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wName);
    wName.setText(Const.NVL(repo.getName(), ""));
    FormData fdName = new FormData();
    fdName.left = new FormAttachment(middle, margin);
    fdName.top = new FormAttachment(wlName, 0, SWT.CENTER);
    fdName.right = new FormAttachment(100, 0);
    wName.setLayoutData(fdName);

    Label wlUrl = new Label(general, SWT.RIGHT);
    PropsUi.setLook(wlUrl);
    wlUrl.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Edit.Url"));
    FormData fdlUrl = new FormData();
    fdlUrl.left = new FormAttachment(0, 0);
    fdlUrl.top = new FormAttachment(wName, margin);
    fdlUrl.right = new FormAttachment(middle, -margin);
    wlUrl.setLayoutData(fdlUrl);
    Text wUrl = new Text(general, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wUrl);
    wUrl.setText(Const.NVL(repo.getUrl(), ""));
    FormData fdUrl = new FormData();
    fdUrl.left = new FormAttachment(middle, margin);
    fdUrl.top = new FormAttachment(wlUrl, 0, SWT.CENTER);
    fdUrl.right = new FormAttachment(100, 0);
    wUrl.setLayoutData(fdUrl);

    Label wlUser = new Label(general, SWT.RIGHT);
    PropsUi.setLook(wlUser);
    wlUser.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Edit.Username"));
    FormData fdlUser = new FormData();
    fdlUser.left = new FormAttachment(0, 0);
    fdlUser.top = new FormAttachment(wUrl, margin);
    fdlUser.right = new FormAttachment(middle, -margin);
    wlUser.setLayoutData(fdlUser);
    Text wUser = new Text(general, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wUser);
    wUser.setText(Const.NVL(repo.getUsername(), ""));
    FormData fdUser = new FormData();
    fdUser.left = new FormAttachment(middle, margin);
    fdUser.top = new FormAttachment(wlUser, 0, SWT.CENTER);
    fdUser.right = new FormAttachment(100, 0);
    wUser.setLayoutData(fdUser);

    Label wlPass = new Label(general, SWT.RIGHT);
    PropsUi.setLook(wlPass);
    wlPass.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Edit.Password"));
    FormData fdlPass = new FormData();
    fdlPass.left = new FormAttachment(0, 0);
    fdlPass.top = new FormAttachment(wUser, margin);
    fdlPass.right = new FormAttachment(middle, -margin);
    wlPass.setLayoutData(fdlPass);
    Text wPass = new Text(general, SWT.SINGLE | SWT.LEFT | SWT.BORDER | SWT.PASSWORD);
    PropsUi.setLook(wPass);
    FormData fdPass = new FormData();
    fdPass.left = new FormAttachment(middle, margin);
    fdPass.top = new FormAttachment(wlPass, 0, SWT.CENTER);
    fdPass.right = new FormAttachment(100, 0);
    wPass.setLayoutData(fdPass);

    Button wEnabled = new Button(general, SWT.CHECK);
    PropsUi.setLook(wEnabled);
    wEnabled.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Edit.Enabled"));
    wEnabled.setSelection(repo.isEnabled());
    FormData fdEnabled = new FormData();
    fdEnabled.left = new FormAttachment(middle, margin);
    fdEnabled.top = new FormAttachment(wPass, margin);
    wEnabled.setLayoutData(fdEnabled);

    Button wPrimary = new Button(general, SWT.CHECK);
    PropsUi.setLook(wPrimary);
    wPrimary.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Edit.Primary"));
    wPrimary.setSelection(repo.isPrimary());
    FormData fdPrimary = new FormData();
    fdPrimary.left = new FormAttachment(middle, margin);
    fdPrimary.top = new FormAttachment(wEnabled, margin);
    wPrimary.setLayoutData(fdPrimary);

    Button wBrowse = new Button(general, SWT.CHECK);
    PropsUi.setLook(wBrowse);
    wBrowse.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Edit.Browse"));
    wBrowse.setToolTipText(
        BaseMessages.getString(PKG, "ManageRepositoriesDialog.Edit.Browse.Tooltip"));
    wBrowse.setSelection(repo.isBrowse());
    FormData fdBrowse = new FormData();
    fdBrowse.left = new FormAttachment(middle, margin);
    fdBrowse.top = new FormAttachment(wPrimary, margin);
    wBrowse.setLayoutData(fdBrowse);

    Label wlCatalog = new Label(general, SWT.RIGHT);
    PropsUi.setLook(wlCatalog);
    wlCatalog.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Edit.CatalogUrl"));
    FormData fdlCatalog = new FormData();
    fdlCatalog.left = new FormAttachment(0, 0);
    fdlCatalog.top = new FormAttachment(wBrowse, margin);
    fdlCatalog.right = new FormAttachment(middle, -margin);
    wlCatalog.setLayoutData(fdlCatalog);
    Text wCatalog = new Text(general, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wCatalog);
    wCatalog.setText(Const.NVL(repo.getCatalogUrl(), ""));
    FormData fdCatalog = new FormData();
    fdCatalog.left = new FormAttachment(middle, margin);
    fdCatalog.top = new FormAttachment(wlCatalog, 0, SWT.CENTER);
    fdCatalog.right = new FormAttachment(100, 0);
    wCatalog.setLayoutData(fdCatalog);

    Label wlSearch = new Label(general, SWT.RIGHT);
    PropsUi.setLook(wlSearch);
    wlSearch.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Edit.SearchQuery"));
    FormData fdlSearch = new FormData();
    fdlSearch.left = new FormAttachment(0, 0);
    fdlSearch.top = new FormAttachment(wCatalog, margin);
    fdlSearch.right = new FormAttachment(middle, -margin);
    wlSearch.setLayoutData(fdlSearch);
    Text wSearch = new Text(general, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSearch);
    wSearch.setText(Const.NVL(repo.getSearchQuery(), ""));
    FormData fdSearch = new FormData();
    fdSearch.left = new FormAttachment(middle, margin);
    fdSearch.top = new FormAttachment(wlSearch, 0, SWT.CENTER);
    fdSearch.right = new FormAttachment(100, 0);
    wSearch.setLayoutData(fdSearch);

    Label wlGroup = new Label(general, SWT.RIGHT);
    PropsUi.setLook(wlGroup);
    wlGroup.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Edit.GroupIdFilter"));
    FormData fdlGroup = new FormData();
    fdlGroup.left = new FormAttachment(0, 0);
    fdlGroup.top = new FormAttachment(wSearch, margin);
    fdlGroup.right = new FormAttachment(middle, -margin);
    wlGroup.setLayoutData(fdlGroup);
    Text wGroup = new Text(general, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wGroup);
    wGroup.setText(Const.NVL(repo.getGroupIdFilter(), ""));
    FormData fdGroup = new FormData();
    fdGroup.left = new FormAttachment(middle, margin);
    fdGroup.top = new FormAttachment(wlGroup, 0, SWT.CENTER);
    fdGroup.right = new FormAttachment(100, 0);
    wGroup.setLayoutData(fdGroup);

    Button wSnapshots = new Button(general, SWT.CHECK);
    PropsUi.setLook(wSnapshots);
    wSnapshots.setText(
        BaseMessages.getString(PKG, "ManageRepositoriesDialog.Edit.IncludeSnapshots"));
    wSnapshots.setSelection(repo.isIncludeSnapshots());
    FormData fdSnapshots = new FormData();
    fdSnapshots.left = new FormAttachment(middle, margin);
    fdSnapshots.top = new FormAttachment(wGroup, margin);
    wSnapshots.setLayoutData(fdSnapshots);

    // ----- Plugin metadata -----
    CTabItem metaTab = new CTabItem(tabs, SWT.NONE);
    metaTab.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Tab.PluginMetadata"));
    Composite meta = new Composite(tabs, SWT.NONE);
    PropsUi.setLook(meta);
    meta.setLayout(new FormLayout());
    metaTab.setControl(meta);

    Label wlMetaHelp = new Label(meta, SWT.LEFT | SWT.WRAP);
    PropsUi.setLook(wlMetaHelp);
    wlMetaHelp.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Plugins.Hint"));
    FormData fdlMetaHelp = new FormData();
    fdlMetaHelp.left = new FormAttachment(0, 0);
    fdlMetaHelp.top = new FormAttachment(0, 0);
    fdlMetaHelp.right = new FormAttachment(100, 0);
    wlMetaHelp.setLayoutData(fdlMetaHelp);

    Button wAddPlugin = new Button(meta, SWT.PUSH);
    wAddPlugin.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Button.AddPlugin"));
    Button wRemovePlugin = new Button(meta, SWT.PUSH);
    wRemovePlugin.setText(
        BaseMessages.getString(PKG, "ManageRepositoriesDialog.Button.RemovePlugin"));
    BaseTransformDialog.positionBottomButtons(
        meta, new Button[] {wAddPlugin, wRemovePlugin}, margin, null);

    ColumnInfo[] pluginCols = {
      new ColumnInfo(
          BaseMessages.getString(PKG, "ManageRepositoriesDialog.Plugins.Column.Group"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          false),
      new ColumnInfo(
          BaseMessages.getString(PKG, "ManageRepositoriesDialog.Plugins.Column.Artifact"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          false),
      new ColumnInfo(
          BaseMessages.getString(PKG, "ManageRepositoriesDialog.Plugins.Column.Version"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          false),
      new ColumnInfo(
          BaseMessages.getString(PKG, "ManageRepositoriesDialog.Plugins.Column.Name"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          false),
      new ColumnInfo(
          BaseMessages.getString(PKG, "ManageRepositoriesDialog.Plugins.Column.Category"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          false),
      new ColumnInfo(
          BaseMessages.getString(PKG, "ManageRepositoriesDialog.Plugins.Column.Description"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          false),
      new ColumnInfo(
          BaseMessages.getString(PKG, "ManageRepositoriesDialog.Plugins.Column.MinHop"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          false),
      new ColumnInfo(
          BaseMessages.getString(PKG, "ManageRepositoriesDialog.Plugins.Column.MaxHop"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          false),
    };
    TableView wPluginsTable =
        new TableView(
            Variables.getADefaultVariableSpace(),
            meta,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL,
            pluginCols,
            1,
            true,
            null,
            props,
            true);
    PropsUi.setLook(wPluginsTable);
    FormData fdPlugins = new FormData();
    fdPlugins.left = new FormAttachment(0, 0);
    fdPlugins.top = new FormAttachment(wlMetaHelp, margin);
    fdPlugins.right = new FormAttachment(100, 0);
    fdPlugins.bottom = new FormAttachment(wAddPlugin, -margin);
    wPluginsTable.setLayoutData(fdPlugins);

    // Load existing plugins into the table (do not mutate repo until OK)
    if (repo.getPlugins() != null) {
      for (OptionalPluginInfo p : repo.getPlugins()) {
        if (p == null) {
          continue;
        }
        TableItem item = new TableItem(wPluginsTable.table, SWT.NONE);
        item.setText(1, Const.NVL(p.getGroupId(), ""));
        item.setText(2, Const.NVL(p.getArtifactId(), ""));
        item.setText(3, Const.NVL(p.getVersion(), ""));
        item.setText(4, Const.NVL(p.getName(), ""));
        item.setText(5, Const.NVL(p.getCategory(), ""));
        item.setText(6, Const.NVL(p.getDescription(), ""));
        item.setText(7, Const.NVL(p.getMinHopVersion(), ""));
        item.setText(8, Const.NVL(p.getMaxHopVersion(), ""));
      }
    }
    wPluginsTable.optimizeTableView();

    wAddPlugin.addListener(
        SWT.Selection,
        e -> {
          TableItem item = new TableItem(wPluginsTable.table, SWT.NONE);
          item.setText(1, "org.apache.hop");
          item.setText(5, "Community");
          wPluginsTable.optimizeTableView();
        });
    wRemovePlugin.addListener(
        SWT.Selection,
        e -> {
          int[] indices = wPluginsTable.table.getSelectionIndices();
          if (indices == null || indices.length == 0) {
            return;
          }
          for (int i = indices.length - 1; i >= 0; i--) {
            wPluginsTable.table.remove(indices[i]);
          }
        });

    wOk.addListener(
        SWT.Selection,
        e -> {
          if (StringUtils.isBlank(wId.getText()) || StringUtils.isBlank(wUrl.getText())) {
            MessageBox box = new MessageBox(dialog, SWT.OK | SWT.ICON_WARNING);
            box.setMessage(
                BaseMessages.getString(PKG, "ManageRepositoriesDialog.Edit.IdUrlRequired"));
            box.open();
            return;
          }
          repo.setId(wId.getText().trim());
          repo.setName(wName.getText().trim());
          repo.setUrl(wUrl.getText().trim());
          repo.setUsername(StringUtils.trimToNull(wUser.getText()));
          if (StringUtils.isNotBlank(wPass.getText())) {
            repo.setPassword(wPass.getText());
          }
          repo.setEnabled(wEnabled.getSelection());
          repo.setPrimary(wPrimary.getSelection());
          repo.setBrowse(wBrowse.getSelection());
          repo.setCatalogUrl(StringUtils.trimToNull(wCatalog.getText()));
          repo.setSearchQuery(StringUtils.trimToNull(wSearch.getText()));
          repo.setGroupIdFilter(StringUtils.trimToNull(wGroup.getText()));
          repo.setIncludeSnapshots(wSnapshots.getSelection());
          repo.setPlugins(readPluginsFromTable(wPluginsTable, repo.getId()));
          ok[0] = true;
          PropsUi.getInstance().setScreen(new WindowProperty(dialog));
          dialog.dispose();
        });
    wCancel.addListener(SWT.Selection, e -> dialog.dispose());

    tabs.setSelection(0);
    BaseTransformDialog.setSize(dialog);
    // Prefer a taller default so the metadata table is usable
    if (dialog.getSize().y < 480) {
      dialog.setSize(Math.max(dialog.getSize().x, 640), 520);
    }
    dialog.open();
    Display display = shell.getDisplay();
    while (!dialog.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
    if (ok[0] && repo.isPrimary()) {
      for (MarketplaceRepository other : config.getRepositories()) {
        if (other != null && other != repo) {
          other.setPrimary(false);
        }
      }
    }
    return ok[0];
  }

  private static List<OptionalPluginInfo> readPluginsFromTable(TableView table, String repoId) {
    List<OptionalPluginInfo> plugins = new ArrayList<>();
    for (int i = 0; i < table.table.getItemCount(); i++) {
      TableItem item = table.table.getItem(i);
      String artifact = item.getText(2).trim();
      if (StringUtils.isBlank(artifact)) {
        continue;
      }
      OptionalPluginInfo p = new OptionalPluginInfo();
      p.setGroupId(StringUtils.trimToNull(item.getText(1)));
      p.setArtifactId(artifact);
      p.setVersion(StringUtils.trimToNull(item.getText(3)));
      p.setName(StringUtils.trimToNull(item.getText(4)));
      p.setCategory(StringUtils.trimToNull(item.getText(5)));
      p.setDescription(StringUtils.trimToNull(item.getText(6)));
      p.setMinHopVersion(StringUtils.trimToNull(item.getText(7)));
      p.setMaxHopVersion(StringUtils.trimToNull(item.getText(8)));
      p.setSource(repoId);
      plugins.add(p);
    }
    return plugins;
  }

  private void removeSelected() {
    MarketplaceRepository repo = selected();
    if (repo == null) {
      return;
    }
    try {
      config.removeRepository(repo.getId());
      markDirty();
      refreshTable();
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "ManageRepositoriesDialog.Error.Header"),
          e.getMessage(),
          e);
    }
  }

  private void importDefinition() {
    try {
      String path =
          BaseDialog.presentFileDialog(
              false,
              shell,
              new String[] {"*.yaml;*.yml;*.json", "*.*"},
              new String[] {
                BaseMessages.getString(PKG, "ManageRepositoriesDialog.Filter.RepoDef"),
                BaseMessages.getString(PKG, "MarketplaceDialog.EnvFile.Filter.All")
              },
              false);
      if (StringUtils.isBlank(path)) {
        return;
      }
      MarketplaceRepository imported = MarketplaceRepositoryDefinition.load(Path.of(path.trim()));
      MarketplaceRepositoryDefinition.applyToConfig(config, imported, false);
      markDirty();
      refreshTable();
      selectRepoId(imported.getId());
      MessageBox box = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
      box.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Import.Done.Header"));
      box.setMessage(
          BaseMessages.getString(
              PKG, "ManageRepositoriesDialog.Import.Done.Message", imported.getId()));
      box.open();
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "ManageRepositoriesDialog.Error.Header"),
          BaseMessages.getString(PKG, "ManageRepositoriesDialog.Import.Error"),
          e);
    }
  }

  private void exportDefinition() {
    MarketplaceRepository repo = selected();
    if (repo == null) {
      MessageBox box = new MessageBox(shell, SWT.OK | SWT.ICON_WARNING);
      box.setMessage(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Export.Select"));
      box.open();
      return;
    }
    try {
      String path =
          BaseDialog.presentFileDialog(
              true,
              shell,
              new String[] {"*.yaml;*.yml", "*.json", "*.*"},
              new String[] {
                BaseMessages.getString(PKG, "ManageRepositoriesDialog.Filter.Yaml"),
                BaseMessages.getString(PKG, "ManageRepositoriesDialog.Filter.Json"),
                BaseMessages.getString(PKG, "MarketplaceDialog.EnvFile.Filter.All")
              },
              false);
      if (StringUtils.isBlank(path)) {
        return;
      }
      Path out = Path.of(path.trim());
      String name = out.getFileName().toString().toLowerCase();
      if (!name.endsWith(".yaml") && !name.endsWith(".yml") && !name.endsWith(".json")) {
        out = out.resolveSibling(out.getFileName().toString() + ".yaml");
      }
      MarketplaceRepositoryDefinition.save(out, repo);
      MessageBox box = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
      box.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Export.Done.Header"));
      box.setMessage(
          BaseMessages.getString(
              PKG, "ManageRepositoriesDialog.Export.Done.Message", out.toString()));
      box.open();
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "ManageRepositoriesDialog.Error.Header"),
          BaseMessages.getString(PKG, "ManageRepositoriesDialog.Export.Error"),
          e);
    }
  }

  private void setPrimarySelected() {
    MarketplaceRepository repo = selected();
    if (repo == null) {
      return;
    }
    try {
      config.setPrimary(repo.getId());
      markDirty();
      refreshTable();
      selectRepoId(repo.getId());
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "ManageRepositoriesDialog.Error.Header"),
          e.getMessage(),
          e);
    }
  }

  private void moveSelected(int delta) {
    MarketplaceRepository repo = selected();
    if (repo == null || config.getRepositories() == null) {
      return;
    }
    List<MarketplaceRepository> list = config.getRepositories();
    int idx = list.indexOf(repo);
    int target = idx + delta;
    if (idx < 0 || target < 0 || target >= list.size()) {
      return;
    }
    list.remove(idx);
    list.add(target, repo);
    markDirty();
    refreshTable();
    wTable.table.setSelection(target);
  }

  private void resetDefaults() {
    MessageBox confirm = new MessageBox(shell, SWT.YES | SWT.NO | SWT.ICON_QUESTION);
    confirm.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Reset.Header"));
    confirm.setMessage(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Reset.Message"));
    if (confirm.open() != SWT.YES) {
      return;
    }
    config.setRepositories(new ArrayList<>(MarketplaceConfig.defaultRepositories()));
    markDirty();
    refreshTable();
  }

  private void save() {
    saveChanges(true);
  }
}
