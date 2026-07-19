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
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.marketplace.catalog.OptionalPluginCatalog;
import org.apache.hop.marketplace.catalog.OptionalPluginInfo;
import org.apache.hop.marketplace.command.MarketplaceCommand;
import org.apache.hop.marketplace.config.MarketplaceConfig;
import org.apache.hop.marketplace.install.HopHome;
import org.apache.hop.marketplace.install.InstallReceipt;
import org.apache.hop.marketplace.install.PluginInstaller;
import org.apache.hop.marketplace.install.PluginUninstaller;
import org.apache.hop.marketplace.resolve.MavenCoordinates;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
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

/** Tools → Marketplace dialog: list optional plugins and install/uninstall from a Maven repo. */
public class MarketplaceDialog extends Dialog {

  private static final Class<?> PKG = MarketplaceGuiPlugin.class;

  private final PropsUi props;
  private final ILogChannel log = new LogChannel("Marketplace");

  private Shell shell;
  private Table wTable;
  private Text wRepo;
  private Label wStatus;
  private Path hopHome;
  private MarketplaceConfig config;

  public MarketplaceDialog(Shell parent) {
    super(parent, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.RESIZE | SWT.MAX);
    this.props = PropsUi.getInstance();
  }

  public void open() {
    Shell parent = getParent();
    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.RESIZE | SWT.MAX);
    props.setLook(shell);
    shell.setImage(GuiResource.getInstance().getImageHopUi());
    shell.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Shell.Title"));

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;
    shell.setLayout(formLayout);

    try {
      hopHome = HopHome.resolve();
      config = MarketplaceConfig.load();
    } catch (Exception e) {
      new ErrorDialog(
          parent,
          BaseMessages.getString(PKG, "MarketplaceDialog.Error.Header"),
          BaseMessages.getString(PKG, "MarketplaceDialog.Error.ResolveHome"),
          e);
      return;
    }

    // Repository URL
    Label wlRepo = new Label(shell, SWT.RIGHT);
    props.setLook(wlRepo);
    wlRepo.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Repository.Label"));
    FormData fdlRepo = new FormData();
    fdlRepo.left = new FormAttachment(0, 0);
    fdlRepo.top = new FormAttachment(0, 0);
    wlRepo.setLayoutData(fdlRepo);

    wRepo = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wRepo);
    wRepo.setText(config.primaryRepositoryUrl());
    wRepo.setEditable(false);
    FormData fdRepo = new FormData();
    fdRepo.left = new FormAttachment(wlRepo, Const.MARGIN);
    fdRepo.top = new FormAttachment(wlRepo, 0, SWT.CENTER);
    fdRepo.right = new FormAttachment(100, 0);
    wRepo.setLayoutData(fdRepo);

    // Buttons (bottom)
    Button wClose = new Button(shell, SWT.PUSH);
    wClose.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Button.Close"));
    wClose.addListener(SWT.Selection, e -> close());

    Button wUninstall = new Button(shell, SWT.PUSH);
    wUninstall.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Button.Uninstall"));
    wUninstall.addListener(SWT.Selection, e -> uninstallSelected());

    Button wInstall = new Button(shell, SWT.PUSH);
    wInstall.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Button.Install"));
    wInstall.addListener(SWT.Selection, e -> installSelected());

    Button wRefresh = new Button(shell, SWT.PUSH);
    wRefresh.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Button.Refresh"));
    wRefresh.addListener(SWT.Selection, e -> refreshTable());

    BaseTransformDialog.positionBottomButtons(
        shell, new Button[] {wInstall, wUninstall, wRefresh, wClose}, Const.MARGIN, null);

    // Status
    wStatus = new Label(shell, SWT.LEFT);
    props.setLook(wStatus);
    wStatus.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Status.RestartHint"));
    FormData fdStatus = new FormData();
    fdStatus.left = new FormAttachment(0, 0);
    fdStatus.right = new FormAttachment(100, 0);
    fdStatus.bottom = new FormAttachment(wClose, -Const.MARGIN * 2);
    wStatus.setLayoutData(fdStatus);

    // Table
    wTable = new Table(shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.SINGLE | SWT.V_SCROLL);
    props.setLook(wTable);
    wTable.setHeaderVisible(true);
    wTable.setLinesVisible(true);

    TableColumn colName = new TableColumn(wTable, SWT.LEFT);
    colName.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Column.Name"));
    colName.setWidth(180);
    TableColumn colArtifact = new TableColumn(wTable, SWT.LEFT);
    colArtifact.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Column.Artifact"));
    colArtifact.setWidth(200);
    TableColumn colCategory = new TableColumn(wTable, SWT.LEFT);
    colCategory.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Column.Category"));
    colCategory.setWidth(100);
    TableColumn colStatus = new TableColumn(wTable, SWT.LEFT);
    colStatus.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Column.Status"));
    colStatus.setWidth(100);
    TableColumn colDesc = new TableColumn(wTable, SWT.LEFT);
    colDesc.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Column.Description"));
    colDesc.setWidth(320);

    FormData fdTable = new FormData();
    fdTable.left = new FormAttachment(0, 0);
    fdTable.top = new FormAttachment(wRepo, Const.MARGIN * 2);
    fdTable.right = new FormAttachment(100, 0);
    fdTable.bottom = new FormAttachment(wStatus, -Const.MARGIN);
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
  }

  private void refreshTable() {
    wTable.removeAll();
    List<OptionalPluginInfo> plugins = OptionalPluginCatalog.listWave1();
    for (OptionalPluginInfo info : plugins) {
      TableItem item = new TableItem(wTable, SWT.NONE);
      item.setText(0, Const.NVL(info.getName(), ""));
      item.setText(1, Const.NVL(info.getArtifactId(), ""));
      item.setText(2, Const.NVL(info.getCategory(), ""));
      boolean onDisk = OptionalPluginCatalog.isInstalledOnDisk(hopHome, info);
      InstallReceipt receipt = null;
      try {
        receipt = PluginInstaller.readReceipt(hopHome, info.getArtifactId());
      } catch (Exception e) {
        log.logError("Unable to read receipt for " + info.getArtifactId(), e);
      }
      String status;
      if (onDisk && receipt != null) {
        status =
            BaseMessages.getString(PKG, "MarketplaceDialog.Status.Installed")
                + " ("
                + receipt.getVersion()
                + ")";
      } else if (onDisk) {
        status = BaseMessages.getString(PKG, "MarketplaceDialog.Status.Present");
      } else {
        status = BaseMessages.getString(PKG, "MarketplaceDialog.Status.NotInstalled");
      }
      item.setText(3, status);
      item.setText(4, Const.NVL(info.getDescription(), ""));
      item.setData(info);
    }
  }

  private OptionalPluginInfo selected() {
    TableItem[] items = wTable.getSelection();
    if (items == null || items.length == 0) {
      return null;
    }
    return (OptionalPluginInfo) items[0].getData();
  }

  private void installSelected() {
    OptionalPluginInfo info = selected();
    if (info == null) {
      return;
    }
    try {
      // Re-read repo URL in case config changed
      config = MarketplaceConfig.load();
      String version = MarketplaceCommand.resolveDefaultVersion(config);
      MavenCoordinates coords =
          new MavenCoordinates(config.getGroupId(), info.getArtifactId(), version);
      wStatus.setText(
          BaseMessages.getString(PKG, "MarketplaceDialog.Status.Installing", coords.gav()));
      shell.update();
      new PluginInstaller(log, hopHome, config).install(coords, true);
      MessageBox box = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
      box.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Install.Done.Header"));
      box.setMessage(
          BaseMessages.getString(PKG, "MarketplaceDialog.Install.Done.Message", coords.gav()));
      box.open();
      refreshTable();
      wStatus.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Status.RestartHint"));
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "MarketplaceDialog.Error.Header"),
          BaseMessages.getString(PKG, "MarketplaceDialog.Install.Error", info.getArtifactId()),
          e);
    }
  }

  private void uninstallSelected() {
    OptionalPluginInfo info = selected();
    if (info == null) {
      return;
    }
    try {
      InstallReceipt receipt = PluginInstaller.readReceipt(hopHome, info.getArtifactId());
      if (receipt == null) {
        MessageBox box = new MessageBox(shell, SWT.OK | SWT.ICON_WARNING);
        box.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Uninstall.NoReceipt.Header"));
        box.setMessage(
            BaseMessages.getString(
                PKG, "MarketplaceDialog.Uninstall.NoReceipt.Message", info.getArtifactId()));
        box.open();
        return;
      }
      MessageBox confirm = new MessageBox(shell, SWT.YES | SWT.NO | SWT.ICON_QUESTION);
      confirm.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Uninstall.Confirm.Header"));
      confirm.setMessage(
          BaseMessages.getString(
              PKG, "MarketplaceDialog.Uninstall.Confirm.Message", info.getArtifactId()));
      if (confirm.open() != SWT.YES) {
        return;
      }
      new PluginUninstaller(log, hopHome).uninstall(info.getArtifactId());
      refreshTable();
      wStatus.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Status.RestartHint"));
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "MarketplaceDialog.Error.Header"),
          BaseMessages.getString(PKG, "MarketplaceDialog.Uninstall.Error", info.getArtifactId()),
          e);
    }
  }

  private void close() {
    props.setScreen(new WindowProperty(shell));
    shell.dispose();
  }
}
