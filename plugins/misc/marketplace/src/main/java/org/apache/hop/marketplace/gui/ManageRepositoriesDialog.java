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
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.marketplace.config.MarketplaceConfig;
import org.apache.hop.marketplace.config.MarketplaceRepository;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

/** Edit marketplace repository list (saved to hop-config.json). */
public class ManageRepositoriesDialog extends Dialog {

  private static final Class<?> PKG = MarketplaceGuiPlugin.class;

  private final PropsUi props;
  private final MarketplaceConfig config;
  private boolean saved;

  private Shell shell;
  private Table wTable;

  public ManageRepositoriesDialog(Shell parent, MarketplaceConfig config) {
    super(parent, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.RESIZE | SWT.MAX);
    this.props = PropsUi.getInstance();
    this.config = config;
  }

  /**
   * @return true if the user saved changes
   */
  public boolean open() {
    Shell parent = getParent();
    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.RESIZE | SWT.MAX);
    props.setLook(shell);
    shell.setImage(GuiResource.getInstance().getImageHopUi());
    shell.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Shell.Title"));

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;
    shell.setLayout(formLayout);

    Label wlHelp = new Label(shell, SWT.LEFT | SWT.WRAP);
    props.setLook(wlHelp);
    wlHelp.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Help"));
    FormData fdlHelp = new FormData();
    fdlHelp.left = new FormAttachment(0, 0);
    fdlHelp.top = new FormAttachment(0, 0);
    fdlHelp.right = new FormAttachment(100, 0);
    wlHelp.setLayoutData(fdlHelp);

    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> shell.dispose());

    Button wSave = new Button(shell, SWT.PUSH);
    wSave.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Button.Save"));
    wSave.addListener(SWT.Selection, e -> save());

    Button wReset = new Button(shell, SWT.PUSH);
    wReset.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Button.ResetDefaults"));
    wReset.addListener(SWT.Selection, e -> resetDefaults());

    Button wRemove = new Button(shell, SWT.PUSH);
    wRemove.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Button.Remove"));
    wRemove.addListener(SWT.Selection, e -> removeSelected());

    Button wEdit = new Button(shell, SWT.PUSH);
    wEdit.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Button.Edit"));
    wEdit.addListener(SWT.Selection, e -> editSelected());

    Button wAdd = new Button(shell, SWT.PUSH);
    wAdd.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Button.Add"));
    wAdd.addListener(SWT.Selection, e -> addRepository());

    Button wPrimary = new Button(shell, SWT.PUSH);
    wPrimary.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Button.SetPrimary"));
    wPrimary.addListener(SWT.Selection, e -> setPrimarySelected());

    Button wUp = new Button(shell, SWT.PUSH);
    wUp.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Button.MoveUp"));
    wUp.addListener(SWT.Selection, e -> moveSelected(-1));

    Button wDown = new Button(shell, SWT.PUSH);
    wDown.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Button.MoveDown"));
    wDown.addListener(SWT.Selection, e -> moveSelected(1));

    BaseTransformDialog.positionBottomButtons(
        shell,
        new Button[] {wAdd, wEdit, wRemove, wPrimary, wUp, wDown, wReset, wSave, wCancel},
        Const.MARGIN,
        null);

    wTable = new Table(shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.SINGLE | SWT.V_SCROLL);
    props.setLook(wTable);
    wTable.setHeaderVisible(true);
    wTable.setLinesVisible(true);
    addColumn(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Column.Primary"), 60);
    addColumn(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Column.Enabled"), 60);
    addColumn(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Column.Id"), 100);
    addColumn(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Column.Name"), 160);
    addColumn(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Column.Url"), 360);
    addColumn(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Column.Auth"), 50);

    FormData fdTable = new FormData();
    fdTable.left = new FormAttachment(0, 0);
    fdTable.top = new FormAttachment(wlHelp, Const.MARGIN * 2);
    fdTable.right = new FormAttachment(100, 0);
    fdTable.bottom = new FormAttachment(wSave, -Const.MARGIN * 2);
    wTable.setLayoutData(fdTable);

    refreshTable();

    BaseTransformDialog.setSize(shell);
    shell.open();
    Display display = parent.getDisplay();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
    return saved;
  }

  private void addColumn(String title, int width) {
    TableColumn col = new TableColumn(wTable, SWT.LEFT);
    col.setText(title);
    col.setWidth(width);
  }

  private void refreshTable() {
    wTable.removeAll();
    if (config.getRepositories() == null) {
      return;
    }
    for (MarketplaceRepository repo : config.getRepositories()) {
      if (repo == null) {
        continue;
      }
      TableItem item = new TableItem(wTable, SWT.NONE);
      item.setText(0, repo.isPrimary() ? "*" : "");
      item.setText(1, repo.isEnabled() ? "Y" : "N");
      item.setText(2, Const.NVL(repo.getId(), ""));
      item.setText(3, Const.NVL(repo.displayName(), ""));
      item.setText(4, Const.NVL(repo.getUrl(), ""));
      item.setText(
          5, repo.hasCredentials() || StringUtils.isNotBlank(repo.getUsername()) ? "Y" : "");
      item.setData(repo);
    }
  }

  private MarketplaceRepository selected() {
    TableItem[] items = wTable.getSelection();
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
    props.setLook(dialog);
    dialog.setText(
        BaseMessages.getString(
            PKG,
            isNew
                ? "ManageRepositoriesDialog.Edit.Title.Add"
                : "ManageRepositoriesDialog.Edit.Title.Edit"));
    FormLayout layout = new FormLayout();
    layout.marginWidth = Const.FORM_MARGIN;
    layout.marginHeight = Const.FORM_MARGIN;
    dialog.setLayout(layout);

    int middle = 25;
    int margin = Const.MARGIN;

    Label wlId = new Label(dialog, SWT.RIGHT);
    props.setLook(wlId);
    wlId.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Edit.Id"));
    FormData fdlId = new FormData();
    fdlId.left = new FormAttachment(0, 0);
    fdlId.top = new FormAttachment(0, margin);
    fdlId.right = new FormAttachment(middle, -margin);
    wlId.setLayoutData(fdlId);
    Text wId = new Text(dialog, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wId);
    wId.setText(Const.NVL(repo.getId(), ""));
    wId.setEditable(isNew);
    FormData fdId = new FormData();
    fdId.left = new FormAttachment(middle, 0);
    fdId.top = new FormAttachment(wlId, 0, SWT.CENTER);
    fdId.right = new FormAttachment(100, 0);
    wId.setLayoutData(fdId);

    Label wlName = new Label(dialog, SWT.RIGHT);
    props.setLook(wlName);
    wlName.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Edit.Name"));
    FormData fdlName = new FormData();
    fdlName.left = new FormAttachment(0, 0);
    fdlName.top = new FormAttachment(wId, margin);
    fdlName.right = new FormAttachment(middle, -margin);
    wlName.setLayoutData(fdlName);
    Text wName = new Text(dialog, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wName);
    wName.setText(Const.NVL(repo.getName(), ""));
    FormData fdName = new FormData();
    fdName.left = new FormAttachment(middle, 0);
    fdName.top = new FormAttachment(wlName, 0, SWT.CENTER);
    fdName.right = new FormAttachment(100, 0);
    wName.setLayoutData(fdName);

    Label wlUrl = new Label(dialog, SWT.RIGHT);
    props.setLook(wlUrl);
    wlUrl.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Edit.Url"));
    FormData fdlUrl = new FormData();
    fdlUrl.left = new FormAttachment(0, 0);
    fdlUrl.top = new FormAttachment(wName, margin);
    fdlUrl.right = new FormAttachment(middle, -margin);
    wlUrl.setLayoutData(fdlUrl);
    Text wUrl = new Text(dialog, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wUrl);
    wUrl.setText(Const.NVL(repo.getUrl(), ""));
    FormData fdUrl = new FormData();
    fdUrl.left = new FormAttachment(middle, 0);
    fdUrl.top = new FormAttachment(wlUrl, 0, SWT.CENTER);
    fdUrl.right = new FormAttachment(100, 0);
    wUrl.setLayoutData(fdUrl);

    Label wlUser = new Label(dialog, SWT.RIGHT);
    props.setLook(wlUser);
    wlUser.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Edit.Username"));
    FormData fdlUser = new FormData();
    fdlUser.left = new FormAttachment(0, 0);
    fdlUser.top = new FormAttachment(wUrl, margin);
    fdlUser.right = new FormAttachment(middle, -margin);
    wlUser.setLayoutData(fdlUser);
    Text wUser = new Text(dialog, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wUser);
    wUser.setText(Const.NVL(repo.getUsername(), ""));
    FormData fdUser = new FormData();
    fdUser.left = new FormAttachment(middle, 0);
    fdUser.top = new FormAttachment(wlUser, 0, SWT.CENTER);
    fdUser.right = new FormAttachment(100, 0);
    wUser.setLayoutData(fdUser);

    Label wlPass = new Label(dialog, SWT.RIGHT);
    props.setLook(wlPass);
    wlPass.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Edit.Password"));
    FormData fdlPass = new FormData();
    fdlPass.left = new FormAttachment(0, 0);
    fdlPass.top = new FormAttachment(wUser, margin);
    fdlPass.right = new FormAttachment(middle, -margin);
    wlPass.setLayoutData(fdlPass);
    Text wPass = new Text(dialog, SWT.SINGLE | SWT.LEFT | SWT.BORDER | SWT.PASSWORD);
    props.setLook(wPass);
    FormData fdPass = new FormData();
    fdPass.left = new FormAttachment(middle, 0);
    fdPass.top = new FormAttachment(wlPass, 0, SWT.CENTER);
    fdPass.right = new FormAttachment(100, 0);
    wPass.setLayoutData(fdPass);

    Button wEnabled = new Button(dialog, SWT.CHECK);
    props.setLook(wEnabled);
    wEnabled.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Edit.Enabled"));
    wEnabled.setSelection(repo.isEnabled());
    FormData fdEnabled = new FormData();
    fdEnabled.left = new FormAttachment(middle, 0);
    fdEnabled.top = new FormAttachment(wPass, margin);
    wEnabled.setLayoutData(fdEnabled);

    Button wPrimary = new Button(dialog, SWT.CHECK);
    props.setLook(wPrimary);
    wPrimary.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Edit.Primary"));
    wPrimary.setSelection(repo.isPrimary());
    FormData fdPrimary = new FormData();
    fdPrimary.left = new FormAttachment(middle, 0);
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
          dialog.dispose();
        });
    Button wCancel = new Button(dialog, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "ManageRepositoriesDialog.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> dialog.dispose());
    BaseTransformDialog.positionBottomButtons(
        dialog, new Button[] {wOk, wCancel}, margin, wPrimary);

    dialog.setSize(560, 360);
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
    wTable.setSelection(target);
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
      saved = true;
      shell.dispose();
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "ManageRepositoriesDialog.Error.Header"),
          BaseMessages.getString(PKG, "ManageRepositoriesDialog.Error.Save"),
          e);
    }
  }
}
