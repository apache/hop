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

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.marketplace.catalog.OptionalPluginCatalog;
import org.apache.hop.marketplace.catalog.OptionalPluginInfo;
import org.apache.hop.marketplace.command.MarketplaceCommand;
import org.apache.hop.marketplace.config.MarketplaceConfig;
import org.apache.hop.marketplace.config.MarketplaceRepository;
import org.apache.hop.marketplace.env.EnvironmentApplier;
import org.apache.hop.marketplace.env.EnvironmentDrift;
import org.apache.hop.marketplace.env.HopEnvironmentLoader;
import org.apache.hop.marketplace.env.HopEnvironmentSpec;
import org.apache.hop.marketplace.install.HopHome;
import org.apache.hop.marketplace.install.InstallReceipt;
import org.apache.hop.marketplace.install.PluginInstaller;
import org.apache.hop.marketplace.install.PluginUninstaller;
import org.apache.hop.marketplace.resolve.MavenCoordinates;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
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
import org.eclipse.swt.widgets.Combo;
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
  private Combo wRepo;
  private final java.util.List<String> repoComboIds = new java.util.ArrayList<>();
  private Text wEnvFile;
  private Button wPrune;
  private Button wStrict;
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

    // Primary repository (install still uses full fallback chain with this first)
    Label wlRepo = new Label(shell, SWT.RIGHT);
    props.setLook(wlRepo);
    wlRepo.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Repository.Label"));
    FormData fdlRepo = new FormData();
    fdlRepo.left = new FormAttachment(0, 0);
    fdlRepo.top = new FormAttachment(0, 0);
    wlRepo.setLayoutData(fdlRepo);

    Button wManageRepos = new Button(shell, SWT.PUSH);
    wManageRepos.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Button.ManageRepos"));
    wManageRepos.addListener(SWT.Selection, e -> manageRepositories());
    FormData fdManage = new FormData();
    fdManage.right = new FormAttachment(100, 0);
    fdManage.top = new FormAttachment(0, 0);
    wManageRepos.setLayoutData(fdManage);

    wRepo = new Combo(shell, SWT.READ_ONLY | SWT.BORDER);
    props.setLook(wRepo);
    FormData fdRepo = new FormData();
    fdRepo.left = new FormAttachment(wlRepo, Const.MARGIN);
    fdRepo.top = new FormAttachment(wlRepo, 0, SWT.CENTER);
    fdRepo.right = new FormAttachment(wManageRepos, -Const.MARGIN);
    wRepo.setLayoutData(fdRepo);
    wRepo.addListener(SWT.Selection, e -> onPrimaryRepoSelected());
    fillRepoCombo();

    // Environment file (hop-env / full-client-env) — Validate / Apply
    Label wlEnv = new Label(shell, SWT.RIGHT);
    props.setLook(wlEnv);
    wlEnv.setText(BaseMessages.getString(PKG, "MarketplaceDialog.EnvFile.Label"));
    FormData fdlEnv = new FormData();
    fdlEnv.left = new FormAttachment(0, 0);
    fdlEnv.top = new FormAttachment(wRepo, Const.MARGIN * 2);
    wlEnv.setLayoutData(fdlEnv);

    Button wApplyEnv = new Button(shell, SWT.PUSH);
    wApplyEnv.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Button.ApplyEnv"));
    wApplyEnv.setToolTipText(
        BaseMessages.getString(PKG, "MarketplaceDialog.Button.ApplyEnv.Tooltip"));
    wApplyEnv.addListener(SWT.Selection, e -> applyEnvironment());
    FormData fdApplyEnv = new FormData();
    fdApplyEnv.right = new FormAttachment(100, 0);
    fdApplyEnv.top = new FormAttachment(wlEnv, 0, SWT.CENTER);
    wApplyEnv.setLayoutData(fdApplyEnv);

    Button wValidateEnv = new Button(shell, SWT.PUSH);
    wValidateEnv.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Button.ValidateEnv"));
    wValidateEnv.setToolTipText(
        BaseMessages.getString(PKG, "MarketplaceDialog.Button.ValidateEnv.Tooltip"));
    wValidateEnv.addListener(SWT.Selection, e -> validateEnvironment());
    FormData fdValidateEnv = new FormData();
    fdValidateEnv.right = new FormAttachment(wApplyEnv, -Const.MARGIN);
    fdValidateEnv.top = new FormAttachment(wlEnv, 0, SWT.CENTER);
    wValidateEnv.setLayoutData(fdValidateEnv);

    Button wBrowseEnv = new Button(shell, SWT.PUSH);
    wBrowseEnv.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Button.BrowseEnv"));
    wBrowseEnv.addListener(SWT.Selection, e -> browseEnvironmentFile());
    FormData fdBrowseEnv = new FormData();
    fdBrowseEnv.right = new FormAttachment(wValidateEnv, -Const.MARGIN);
    fdBrowseEnv.top = new FormAttachment(wlEnv, 0, SWT.CENTER);
    wBrowseEnv.setLayoutData(fdBrowseEnv);

    wEnvFile = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wEnvFile);
    FormData fdEnvFile = new FormData();
    fdEnvFile.left = new FormAttachment(wlEnv, Const.MARGIN);
    fdEnvFile.top = new FormAttachment(wlEnv, 0, SWT.CENTER);
    fdEnvFile.right = new FormAttachment(wBrowseEnv, -Const.MARGIN);
    wEnvFile.setLayoutData(fdEnvFile);
    suggestDefaultEnvFile();

    wPrune = new Button(shell, SWT.CHECK);
    props.setLook(wPrune);
    wPrune.setText(BaseMessages.getString(PKG, "MarketplaceDialog.EnvFile.Prune"));
    wPrune.setToolTipText(BaseMessages.getString(PKG, "MarketplaceDialog.EnvFile.Prune.Tooltip"));
    FormData fdPrune = new FormData();
    fdPrune.left = new FormAttachment(wlEnv, Const.MARGIN);
    fdPrune.top = new FormAttachment(wEnvFile, Const.MARGIN);
    wPrune.setLayoutData(fdPrune);

    wStrict = new Button(shell, SWT.CHECK);
    props.setLook(wStrict);
    wStrict.setText(BaseMessages.getString(PKG, "MarketplaceDialog.EnvFile.Strict"));
    wStrict.setToolTipText(BaseMessages.getString(PKG, "MarketplaceDialog.EnvFile.Strict.Tooltip"));
    FormData fdStrict = new FormData();
    fdStrict.left = new FormAttachment(wPrune, Const.MARGIN * 3);
    fdStrict.top = new FormAttachment(wEnvFile, Const.MARGIN);
    wStrict.setLayoutData(fdStrict);

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
    fdTable.top = new FormAttachment(wPrune, Const.MARGIN * 2);
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

  private void fillRepoCombo() {
    wRepo.removeAll();
    repoComboIds.clear();
    MarketplaceRepository primary = config.primaryRepository();
    int select = 0;
    int i = 0;
    for (MarketplaceRepository repo : config.getRepositories()) {
      if (repo == null || !repo.isEnabled()) {
        continue;
      }
      wRepo.add(repo.displayName() + " — " + repo.normalizedUrl());
      repoComboIds.add(repo.getId());
      if (primary != null && primary.getId() != null && primary.getId().equals(repo.getId())) {
        select = i;
      }
      i++;
    }
    if (i > 0) {
      wRepo.select(select);
    }
  }

  private void onPrimaryRepoSelected() {
    int idx = wRepo.getSelectionIndex();
    if (idx < 0 || idx >= repoComboIds.size()) {
      return;
    }
    try {
      config.setPrimary(repoComboIds.get(idx));
      config.save();
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "MarketplaceDialog.Error.Header"),
          BaseMessages.getString(PKG, "MarketplaceDialog.Repository.SetPrimary.Error"),
          e);
    }
  }

  private void manageRepositories() {
    ManageRepositoriesDialog dialog = new ManageRepositoriesDialog(shell, config);
    if (dialog.open()) {
      config = MarketplaceConfig.load();
      fillRepoCombo();
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

  private void suggestDefaultEnvFile() {
    Path fullClient = hopHome.resolve("full-client-env.yaml");
    if (Files.isRegularFile(fullClient)) {
      wEnvFile.setText(fullClient.toString());
      return;
    }
    Path discovered = EnvironmentApplier.resolveEnvironmentFile(hopHome, null);
    if (discovered != null) {
      wEnvFile.setText(discovered.toString());
    }
  }

  private void browseEnvironmentFile() {
    String path =
        BaseDialog.presentFileDialog(
            false,
            shell,
            new String[] {"*.yaml;*.yml;*.json", "*.*"},
            new String[] {
              BaseMessages.getString(PKG, "MarketplaceDialog.EnvFile.Filter.Env"),
              BaseMessages.getString(PKG, "MarketplaceDialog.EnvFile.Filter.All")
            },
            false);
    if (StringUtils.isNotBlank(path)) {
      wEnvFile.setText(path);
    }
  }

  private Path requireEnvFilePath() {
    String text = wEnvFile.getText();
    if (StringUtils.isBlank(text)) {
      MessageBox box = new MessageBox(shell, SWT.OK | SWT.ICON_WARNING);
      box.setText(BaseMessages.getString(PKG, "MarketplaceDialog.EnvFile.Missing.Header"));
      box.setMessage(BaseMessages.getString(PKG, "MarketplaceDialog.EnvFile.Missing.Message"));
      box.open();
      return null;
    }
    Path path = Path.of(text.trim()).toAbsolutePath().normalize();
    if (!Files.isRegularFile(path)) {
      MessageBox box = new MessageBox(shell, SWT.OK | SWT.ICON_WARNING);
      box.setText(BaseMessages.getString(PKG, "MarketplaceDialog.EnvFile.NotFound.Header"));
      box.setMessage(
          BaseMessages.getString(
              PKG, "MarketplaceDialog.EnvFile.NotFound.Message", path.toString()));
      box.open();
      return null;
    }
    return path;
  }

  private void validateEnvironment() {
    Path envPath = requireEnvFilePath();
    if (envPath == null) {
      return;
    }
    try {
      wStatus.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Status.Validating"));
      shell.update();
      HopEnvironmentSpec env = HopEnvironmentLoader.load(envPath);
      EnvironmentApplier applier = new EnvironmentApplier(log, hopHome, MarketplaceConfig.load());
      EnvironmentDrift drift = applier.validate(env);
      if (wStrict.getSelection()) {
        populateExtraPlugins(env, drift);
      }
      boolean hard =
          !drift.getMissingPlugins().isEmpty()
              || !drift.getVersionMismatches().isEmpty()
              || !drift.getMissingDependencies().isEmpty()
              || (wStrict.getSelection() && !drift.getExtraMarketplacePlugins().isEmpty());
      if (!hard) {
        MessageBox box = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
        box.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Validate.Ok.Header"));
        box.setMessage(
            BaseMessages.getString(
                PKG, "MarketplaceDialog.Validate.Ok.Message", envPath.toString()));
        box.open();
        wStatus.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Status.RestartHint"));
        return;
      }
      MessageBox box = new MessageBox(shell, SWT.OK | SWT.ICON_WARNING);
      box.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Validate.Drift.Header"));
      box.setMessage(
          BaseMessages.getString(PKG, "MarketplaceDialog.Validate.Drift.Message")
              + "\n\n"
              + drift.formatReport()
              + "\n"
              + BaseMessages.getString(PKG, "MarketplaceDialog.Validate.Drift.Hint"));
      box.open();
      wStatus.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Status.Drift"));
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "MarketplaceDialog.Error.Header"),
          BaseMessages.getString(PKG, "MarketplaceDialog.Validate.Error"),
          e);
    }
  }

  private void applyEnvironment() {
    Path envPath = requireEnvFilePath();
    if (envPath == null) {
      return;
    }
    boolean prune = wPrune.getSelection();
    if (prune) {
      MessageBox confirm = new MessageBox(shell, SWT.YES | SWT.NO | SWT.ICON_QUESTION);
      confirm.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Apply.PruneConfirm.Header"));
      confirm.setMessage(
          BaseMessages.getString(PKG, "MarketplaceDialog.Apply.PruneConfirm.Message"));
      if (confirm.open() != SWT.YES) {
        return;
      }
    }
    try {
      config = MarketplaceConfig.load();
      if (!config.isEnabled()) {
        MessageBox box = new MessageBox(shell, SWT.OK | SWT.ICON_WARNING);
        box.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Error.Header"));
        box.setMessage(BaseMessages.getString(PKG, "MarketplaceDialog.Apply.Disabled"));
        box.open();
        return;
      }
      wStatus.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Status.Applying"));
      shell.update();
      HopEnvironmentSpec env = HopEnvironmentLoader.load(envPath);
      new EnvironmentApplier(log, hopHome, config).apply(env, prune);
      MessageBox box = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
      box.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Apply.Done.Header"));
      box.setMessage(
          BaseMessages.getString(PKG, "MarketplaceDialog.Apply.Done.Message", envPath.toString()));
      box.open();
      refreshTable();
      wStatus.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Status.RestartHint"));
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "MarketplaceDialog.Error.Header"),
          BaseMessages.getString(PKG, "MarketplaceDialog.Apply.Error"),
          e);
    }
  }

  private void populateExtraPlugins(HopEnvironmentSpec env, EnvironmentDrift drift)
      throws Exception {
    Set<String> desired = new HashSet<>();
    if (env.getPlugins() != null) {
      for (HopEnvironmentSpec.PluginRef ref : env.getPlugins()) {
        if (ref.getArtifactId() != null) {
          desired.add(ref.getArtifactId());
        }
      }
    }
    Path receipts = hopHome.resolve(PluginInstaller.RECEIPTS_DIR);
    if (!Files.isDirectory(receipts)) {
      return;
    }
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(receipts, "*.json")) {
      for (Path f : stream) {
        String name = f.getFileName().toString();
        String id = name.substring(0, name.length() - ".json".length());
        if (!desired.contains(id)) {
          drift.getExtraMarketplacePlugins().add(id);
        }
      }
    }
  }

  private void close() {
    props.setScreen(new WindowProperty(shell));
    shell.dispose();
  }
}
