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

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.marketplace.config.MarketplaceConfig;
import org.apache.hop.marketplace.config.MarketplaceRepository;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
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
 */
public class MarketplaceRepositoriesPanel {

  private static final Class<?> PKG = MarketplaceGuiPlugin.class;

  private final MarketplaceConfig config;
  private final Shell shell;
  private final TableView wTable;

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

    Label wlHelp = new Label(parent, SWT.LEFT | SWT.WRAP);
    PropsUi.setLook(wlHelp);
    wlHelp.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Help"));
    FormData fdlHelp = new FormData();
    fdlHelp.left = new FormAttachment(0, 0);
    fdlHelp.top = new FormAttachment(0, 0);
    fdlHelp.right = new FormAttachment(100, 0);
    wlHelp.setLayoutData(fdlHelp);

    Label wHelpSep = new Label(parent, SWT.HORIZONTAL | SWT.SEPARATOR);
    FormData fdHelpSep = new FormData();
    fdHelpSep.left = new FormAttachment(0, 0);
    fdHelpSep.right = new FormAttachment(100, 0);
    fdHelpSep.top = new FormAttachment(wlHelp, PropsUi.getMargin());
    wHelpSep.setLayoutData(fdHelpSep);

    Button wSave = new Button(parent, SWT.PUSH);
    wSave.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Button.Save"));
    wSave.addListener(SWT.Selection, e -> save());

    Button wReset = new Button(parent, SWT.PUSH);
    wReset.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Button.ResetDefaults"));
    wReset.addListener(SWT.Selection, e -> resetDefaults());

    Button wRemove = new Button(parent, SWT.PUSH);
    wRemove.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Button.Remove"));
    wRemove.addListener(SWT.Selection, e -> removeSelected());

    Button wEdit = new Button(parent, SWT.PUSH);
    wEdit.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Button.Edit"));
    wEdit.addListener(SWT.Selection, e -> editSelected());

    Button wAdd = new Button(parent, SWT.PUSH);
    wAdd.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Button.Add"));
    wAdd.addListener(SWT.Selection, e -> addRepository());

    Button wPrimary = new Button(parent, SWT.PUSH);
    wPrimary.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Button.SetPrimary"));
    wPrimary.addListener(SWT.Selection, e -> setPrimarySelected());

    Button wUp = new Button(parent, SWT.PUSH);
    wUp.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Button.MoveUp"));
    wUp.addListener(SWT.Selection, e -> moveSelected(-1));

    Button wDown = new Button(parent, SWT.PUSH);
    wDown.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Button.MoveDown"));
    wDown.addListener(SWT.Selection, e -> moveSelected(1));

    BaseTransformDialog.positionBottomButtons(
        parent,
        new Button[] {wAdd, wEdit, wRemove, wPrimary, wUp, wDown, wReset, wSave},
        PropsUi.getMargin(),
        wlHelp);

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
    fdTable.top = new FormAttachment(wSave, PropsUi.getMargin());
    fdTable.right = new FormAttachment(100, 0);
    fdTable.bottom = new FormAttachment(100, 0);
    wTable.setLayoutData(fdTable);

    refreshTable();
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
      item.setText(3, Const.NVL(repo.getId(), ""));
      item.setText(4, Const.NVL(repo.displayName(), ""));
      item.setText(5, Const.NVL(repo.getUrl(), ""));
      item.setText(
          6, repo.hasCredentials() || StringUtils.isNotBlank(repo.getUsername()) ? "Y" : "");
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
        refreshTable();
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
      refreshTable();
    }
  }

  private boolean editRepository(MarketplaceRepository repo, boolean isNew) {
    Shell dialog = new Shell(shell, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.RESIZE);
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

    int middle = PropsUi.getInstance().getMiddlePct();
    int margin = PropsUi.getMargin();

    Label wlId = new Label(dialog, SWT.RIGHT);
    PropsUi.setLook(wlId);
    wlId.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Edit.Id"));
    FormData fdlId = new FormData();
    fdlId.left = new FormAttachment(0, 0);
    fdlId.top = new FormAttachment(0, margin);
    fdlId.right = new FormAttachment(middle, -margin);
    wlId.setLayoutData(fdlId);
    Text wId = new Text(dialog, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wId);
    wId.setText(Const.NVL(repo.getId(), ""));
    wId.setEditable(isNew);
    FormData fdId = new FormData();
    fdId.left = new FormAttachment(middle, margin);
    fdId.top = new FormAttachment(wlId, 0, SWT.CENTER);
    fdId.right = new FormAttachment(100, 0);
    wId.setLayoutData(fdId);

    Label wlName = new Label(dialog, SWT.RIGHT);
    PropsUi.setLook(wlName);
    wlName.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Edit.Name"));
    FormData fdlName = new FormData();
    fdlName.left = new FormAttachment(0, 0);
    fdlName.top = new FormAttachment(wId, margin);
    fdlName.right = new FormAttachment(middle, -margin);
    wlName.setLayoutData(fdlName);
    Text wName = new Text(dialog, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wName);
    wName.setText(Const.NVL(repo.getName(), ""));
    FormData fdName = new FormData();
    fdName.left = new FormAttachment(middle, margin);
    fdName.top = new FormAttachment(wlName, 0, SWT.CENTER);
    fdName.right = new FormAttachment(100, 0);
    wName.setLayoutData(fdName);

    Label wlUrl = new Label(dialog, SWT.RIGHT);
    PropsUi.setLook(wlUrl);
    wlUrl.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Edit.Url"));
    FormData fdlUrl = new FormData();
    fdlUrl.left = new FormAttachment(0, 0);
    fdlUrl.top = new FormAttachment(wName, margin);
    fdlUrl.right = new FormAttachment(middle, -margin);
    wlUrl.setLayoutData(fdlUrl);
    Text wUrl = new Text(dialog, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wUrl);
    wUrl.setText(Const.NVL(repo.getUrl(), ""));
    FormData fdUrl = new FormData();
    fdUrl.left = new FormAttachment(middle, margin);
    fdUrl.top = new FormAttachment(wlUrl, 0, SWT.CENTER);
    fdUrl.right = new FormAttachment(100, 0);
    wUrl.setLayoutData(fdUrl);

    Label wlUser = new Label(dialog, SWT.RIGHT);
    PropsUi.setLook(wlUser);
    wlUser.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Edit.Username"));
    FormData fdlUser = new FormData();
    fdlUser.left = new FormAttachment(0, 0);
    fdlUser.top = new FormAttachment(wUrl, margin);
    fdlUser.right = new FormAttachment(middle, -margin);
    wlUser.setLayoutData(fdlUser);
    Text wUser = new Text(dialog, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wUser);
    wUser.setText(Const.NVL(repo.getUsername(), ""));
    FormData fdUser = new FormData();
    fdUser.left = new FormAttachment(middle, margin);
    fdUser.top = new FormAttachment(wlUser, 0, SWT.CENTER);
    fdUser.right = new FormAttachment(100, 0);
    wUser.setLayoutData(fdUser);

    Label wlPass = new Label(dialog, SWT.RIGHT);
    PropsUi.setLook(wlPass);
    wlPass.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Edit.Password"));
    FormData fdlPass = new FormData();
    fdlPass.left = new FormAttachment(0, 0);
    fdlPass.top = new FormAttachment(wUser, margin);
    fdlPass.right = new FormAttachment(middle, -margin);
    wlPass.setLayoutData(fdlPass);
    Text wPass = new Text(dialog, SWT.SINGLE | SWT.LEFT | SWT.BORDER | SWT.PASSWORD);
    PropsUi.setLook(wPass);
    FormData fdPass = new FormData();
    fdPass.left = new FormAttachment(middle, margin);
    fdPass.top = new FormAttachment(wlPass, 0, SWT.CENTER);
    fdPass.right = new FormAttachment(100, 0);
    wPass.setLayoutData(fdPass);

    Button wEnabled = new Button(dialog, SWT.CHECK);
    PropsUi.setLook(wEnabled);
    wEnabled.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Edit.Enabled"));
    wEnabled.setSelection(repo.isEnabled());
    FormData fdEnabled = new FormData();
    fdEnabled.left = new FormAttachment(middle, margin);
    fdEnabled.top = new FormAttachment(wPass, margin);
    wEnabled.setLayoutData(fdEnabled);

    Button wPrimary = new Button(dialog, SWT.CHECK);
    PropsUi.setLook(wPrimary);
    wPrimary.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Edit.Primary"));
    wPrimary.setSelection(repo.isPrimary());
    FormData fdPrimary = new FormData();
    fdPrimary.left = new FormAttachment(middle, margin);
    fdPrimary.top = new FormAttachment(wEnabled, margin);
    wPrimary.setLayoutData(fdPrimary);

    final boolean[] ok = {false};
    Button wOk = new Button(dialog, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Button.Ok"));
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
          ok[0] = true;
          PropsUi.getInstance().setScreen(new WindowProperty(dialog));
          dialog.dispose();
        });
    Button wCancel = new Button(dialog, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> dialog.dispose());
    BaseTransformDialog.positionBottomButtons(
        dialog, new Button[] {wOk, wCancel}, margin, wPrimary);

    BaseTransformDialog.setSize(dialog);
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

  private void removeSelected() {
    MarketplaceRepository repo = selected();
    if (repo == null) {
      return;
    }
    try {
      config.removeRepository(repo.getId());
      refreshTable();
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "ManageRepositoriesDialog.Error.Header"),
          e.getMessage(),
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
      refreshTable();
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
    refreshTable();
  }

  private void save() {
    try {
      config.ensureValidPrimary();
      config.save();
      MessageBox box = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
      box.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Save.Done.Header"));
      box.setMessage(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Save.Done.Message"));
      box.open();
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "ManageRepositoriesDialog.Error.Header"),
          BaseMessages.getString(PKG, "ManageRepositoriesDialog.Error.Save"),
          e);
    }
  }
}
