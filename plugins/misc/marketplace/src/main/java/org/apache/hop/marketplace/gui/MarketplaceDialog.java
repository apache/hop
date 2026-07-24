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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.marketplace.catalog.OptionalPluginCatalog;
import org.apache.hop.marketplace.catalog.OptionalPluginInfo;
import org.apache.hop.marketplace.catalog.PluginDiscovery;
import org.apache.hop.marketplace.command.MarketplaceCommand;
import org.apache.hop.marketplace.config.MarketplaceConfig;
import org.apache.hop.marketplace.config.MarketplaceRepository;
import org.apache.hop.marketplace.env.EnvironmentApplier;
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
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

/** Tools → Marketplace dialog: list optional plugins and install/uninstall from a Maven repo. */
public class MarketplaceDialog extends Dialog {

  private static final Class<?> PKG = MarketplaceGuiPlugin.class;

  private final PropsUi props;
  private final ILogChannel log = new LogChannel("Marketplace");

  private Shell shell;
  private TableView wTable;
  private Text wSearch;
  private Label wStatus;
  private Path hopHome;
  private MarketplaceConfig config;
  private MarketplaceRepositoriesPanel repositoriesPanel;

  public MarketplaceDialog(Shell parent) {
    super(parent, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.RESIZE | SWT.MAX);
    this.props = PropsUi.getInstance();
  }

  public void open() {
    Shell parent = getParent();
    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.RESIZE | SWT.MAX);
    PropsUi.setLook(shell);
    shell.setImage(GuiResource.getInstance().getImageHopUi());
    shell.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Shell.Title"));

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();
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

    // Shell-level Close only; Install/Uninstall/Refresh live on the Plugins tab.
    Button wClose = new Button(shell, SWT.PUSH);
    wClose.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Button.Close"));
    // shell.close() → ShellAdapter.shellClosed (single dirty check for button and window X)
    wClose.addListener(SWT.Selection, e -> shell.close());
    BaseTransformDialog.positionBottomButtons(
        shell, new Button[] {wClose}, PropsUi.getMargin(), null);

    // Status (shared across tabs)
    wStatus = new Label(shell, SWT.LEFT);
    PropsUi.setLook(wStatus);
    wStatus.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Status.RestartHint"));
    FormData fdStatus = new FormData();
    fdStatus.left = new FormAttachment(0, 0);
    fdStatus.right = new FormAttachment(100, 0);
    fdStatus.bottom = new FormAttachment(wClose, -PropsUi.getMargin() * 2);
    wStatus.setLayoutData(fdStatus);

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    PropsUi.setLook(wTabFolder);
    FormData fdTabs = new FormData();
    fdTabs.left = new FormAttachment(0, 0);
    fdTabs.top = new FormAttachment(0, 0);
    fdTabs.right = new FormAttachment(100, 0);
    fdTabs.bottom = new FormAttachment(wStatus, -PropsUi.getMargin());
    wTabFolder.setLayoutData(fdTabs);

    createPluginsTab(wTabFolder);
    createRepositoriesTab(wTabFolder);
    createEnvironmentTab(wTabFolder);
    wTabFolder.setSelection(0);

    shell.addShellListener(
        new ShellAdapter() {
          @Override
          public void shellClosed(ShellEvent e) {
            // X / Alt+F4 — same dirty prompt as Close button
            if (!confirmClose()) {
              e.doit = false;
            } else {
              props.setScreen(new WindowProperty(shell));
            }
          }
        });

    refreshTable(false);

    BaseTransformDialog.setSize(shell);
    shell.open();
    Display display = parent.getDisplay();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
  }

  private void createPluginsTab(CTabFolder folder) {
    CTabItem tab = new CTabItem(folder, SWT.NONE);
    tab.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Tab.Plugins"));
    tab.setImage(GuiResource.getInstance().getImagePlugin());
    Composite comp = new Composite(folder, SWT.NONE);
    PropsUi.setLook(comp);
    FormLayout layout = new FormLayout();
    layout.marginWidth = PropsUi.getFormMargin();
    layout.marginHeight = PropsUi.getFormMargin();
    comp.setLayout(layout);
    tab.setControl(comp);

    Button wUninstall = new Button(comp, SWT.PUSH);
    wUninstall.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Button.Uninstall"));
    wUninstall.addListener(SWT.Selection, e -> uninstallSelected());

    Button wInstall = new Button(comp, SWT.PUSH);
    wInstall.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Button.Install"));
    wInstall.addListener(SWT.Selection, e -> installSelected());

    Button wRefresh = new Button(comp, SWT.PUSH);
    wRefresh.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Button.Refresh"));
    wRefresh.addListener(SWT.Selection, e -> refreshTable(true));

    BaseTransformDialog.positionBottomButtons(
        comp, new Button[] {wInstall, wUninstall, wRefresh}, PropsUi.getMargin(), null);

    // Search / filter above the plugin list
    Label wlSearch = new Label(comp, SWT.RIGHT);
    PropsUi.setLook(wlSearch);
    wlSearch.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Search.Label"));
    FormData fdlSearch = new FormData();
    fdlSearch.left = new FormAttachment(0, 0);
    fdlSearch.top = new FormAttachment(0, 0);
    wlSearch.setLayoutData(fdlSearch);

    Button wClearSearch = new Button(comp, SWT.PUSH);
    wClearSearch.setImage(GuiResource.getInstance().getImageClear());
    wClearSearch.setToolTipText(BaseMessages.getString(PKG, "MarketplaceDialog.Search.Clear"));
    wClearSearch.addListener(SWT.Selection, e -> clearSearch());
    FormData fdClearSearch = new FormData();
    fdClearSearch.right = new FormAttachment(100, 0);
    fdClearSearch.top = new FormAttachment(wlSearch, 0, SWT.CENTER);
    wClearSearch.setLayoutData(fdClearSearch);

    wSearch = new Text(comp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSearch);
    wSearch.setMessage(BaseMessages.getString(PKG, "MarketplaceDialog.Search.Placeholder"));
    FormData fdSearch = new FormData();
    fdSearch.left = new FormAttachment(wlSearch, PropsUi.getMargin());
    fdSearch.top = new FormAttachment(wlSearch, 0, SWT.CENTER);
    fdSearch.right = new FormAttachment(wClearSearch, -PropsUi.getMargin());
    wSearch.setLayoutData(fdSearch);
    wSearch.addListener(SWT.Modify, e -> refreshTable(false));

    Label wSearchSep = new Label(comp, SWT.HORIZONTAL | SWT.SEPARATOR);
    FormData fdSearchSep = new FormData();
    fdSearchSep.left = new FormAttachment(0, 0);
    fdSearchSep.right = new FormAttachment(100, 0);
    fdSearchSep.top = new FormAttachment(wSearch, PropsUi.getMargin());
    wSearchSep.setLayoutData(fdSearchSep);

    ColumnInfo[] columns = {
      new ColumnInfo(
          BaseMessages.getString(PKG, "MarketplaceDialog.Column.Name"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          true),
      new ColumnInfo(
          BaseMessages.getString(PKG, "MarketplaceDialog.Column.Version"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          true),
      new ColumnInfo(
          BaseMessages.getString(PKG, "MarketplaceDialog.Column.Artifact"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          true),
      new ColumnInfo(
          BaseMessages.getString(PKG, "MarketplaceDialog.Column.Category"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          true),
      new ColumnInfo(
          BaseMessages.getString(PKG, "MarketplaceDialog.Column.Repository"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          true),
      new ColumnInfo(
          BaseMessages.getString(PKG, "MarketplaceDialog.Column.Status"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          true),
      new ColumnInfo(
          BaseMessages.getString(PKG, "MarketplaceDialog.Column.Description"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          true),
    };
    wTable =
        new TableView(
            Variables.getADefaultVariableSpace(),
            comp,
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
    fdTable.top = new FormAttachment(wSearchSep, PropsUi.getMargin());
    fdTable.right = new FormAttachment(100, 0);
    fdTable.bottom = new FormAttachment(wInstall, -PropsUi.getMargin() * 2);
    wTable.setLayoutData(fdTable);
  }

  private void createEnvironmentTab(CTabFolder folder) {
    CTabItem tab = new CTabItem(folder, SWT.NONE);
    tab.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Tab.Environment"));
    tab.setImage(GuiResource.getInstance().getImageFile());
    Composite comp = new Composite(folder, SWT.NONE);
    PropsUi.setLook(comp);
    FormLayout layout = new FormLayout();
    layout.marginWidth = PropsUi.getFormMargin();
    layout.marginHeight = PropsUi.getFormMargin();
    comp.setLayout(layout);
    tab.setControl(comp);

    // Full hop-env editor (formerly HopEnvironmentDialog modal)
    HopEnvironmentDialog.embed(
        comp,
        resolveDefaultEnvPath(),
        msg -> {
          if (wStatus != null && !wStatus.isDisposed()) {
            wStatus.setText(msg);
          }
        });
  }

  private Path resolveDefaultEnvPath() {
    // Last file edited in the marketplace environment editor (audit list, most-recent first)
    String last =
        org.apache.hop.ui.hopgui.shared.AuditManagerGuiUtil.getLastUsedValue(
            HopEnvironmentDialog.AUDIT_TYPE_ENV_FILES);
    if (StringUtils.isNotBlank(last)) {
      Path lastPath = Path.of(last.trim()).toAbsolutePath().normalize();
      if (Files.isRegularFile(lastPath)) {
        return lastPath;
      }
    }
    Path fullClient = hopHome.resolve("full-client-env.yaml");
    if (Files.isRegularFile(fullClient)) {
      return fullClient;
    }
    return EnvironmentApplier.resolveEnvironmentFile(hopHome, null);
  }

  private void createRepositoriesTab(CTabFolder folder) {
    CTabItem tab = new CTabItem(folder, SWT.NONE);
    tab.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Tab.Repositories"));
    tab.setImage(GuiResource.getInstance().getImageServer());
    Composite comp = new Composite(folder, SWT.NONE);
    PropsUi.setLook(comp);
    FormLayout layout = new FormLayout();
    layout.marginWidth = PropsUi.getFormMargin();
    layout.marginHeight = PropsUi.getFormMargin();
    comp.setLayout(layout);
    tab.setControl(comp);

    // Full repository management UI (formerly a nested ManageRepositoriesDialog)
    repositoriesPanel = new MarketplaceRepositoriesPanel(comp, config);
  }

  private void clearSearch() {
    if (wSearch != null && !wSearch.isDisposed()) {
      wSearch.setText("");
      wSearch.setFocus();
    }
  }

  /**
   * Reload the plugins list.
   *
   * @param userRefresh when true (Refresh button), prompt to save dirty repositories and show a
   *     wait cursor; search filter updates pass false.
   */
  private void refreshTable(boolean userRefresh) {
    if (userRefresh && !ensureRepositoriesSavedForRefresh()) {
      return;
    }

    Cursor waitCursor = null;
    if (userRefresh && shell != null && !shell.isDisposed()) {
      waitCursor = shell.getDisplay().getSystemCursor(SWT.CURSOR_WAIT);
      shell.setCursor(waitCursor);
    }
    try {
      // Keep shared in-memory config (after optional save/discard). Do not reload from disk on
      // every search keystroke — that would drop unsaved repository edits.
      if (repositoriesPanel != null) {
        config = repositoriesPanel.getConfig();
      }
      wTable.table.removeAll();
      // Filter only when more than 2 characters are typed (same as explorer/metadata search).
      String searchText = wSearch != null ? wSearch.getText() : "";
      String filter =
          searchText != null && searchText.trim().length() > 2 ? searchText.trim() : null;
      // Apache optional catalog + live list from every browse=true repository
      List<OptionalPluginInfo> plugins = PluginDiscovery.query(filter, null, config, log);
      for (OptionalPluginInfo info : plugins) {
        // Column 0 is the (hidden) index column; data columns are 1-based.
        TableItem item = new TableItem(wTable.table, SWT.NONE);
        String name = Const.NVL(info.getName(), info.getArtifactId());
        item.setText(1, Const.NVL(name, ""));
        item.setText(2, Const.NVL(info.getVersion(), ""));
        item.setText(3, Const.NVL(info.getArtifactId(), ""));
        item.setText(4, Const.NVL(info.getCategory(), ""));
        item.setText(5, repositoryDisplayName(info.getSource()));
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
        item.setText(6, status);
        item.setText(7, Const.NVL(info.getDescription(), ""));
        item.setData(info);
      }
      wTable.optimizeTableView();
    } finally {
      if (userRefresh && shell != null && !shell.isDisposed()) {
        shell.setCursor(null);
      }
    }
  }

  /**
   * If repository edits are unsaved, ask the user to save before refreshing plugins (discovery uses
   * the config that is saved / in memory).
   *
   * @return false if the user cancelled
   */
  private boolean ensureRepositoriesSavedForRefresh() {
    if (repositoriesPanel == null || !repositoriesPanel.isDirty()) {
      return true;
    }
    MessageBox box = new MessageBox(shell, SWT.YES | SWT.NO | SWT.CANCEL | SWT.ICON_WARNING);
    box.setText(BaseMessages.getString(PKG, "MarketplaceDialog.ReposDirty.Header"));
    box.setMessage(BaseMessages.getString(PKG, "MarketplaceDialog.ReposDirty.Refresh.Message"));
    int answer = box.open();
    if (answer == SWT.CANCEL) {
      return false;
    }
    if (answer == SWT.YES) {
      return repositoriesPanel.saveChanges(false);
    }
    // No — discard in-memory edits and reload hop-config so refresh matches disk
    try {
      MarketplaceConfig loaded = MarketplaceConfig.load();
      config = loaded;
      repositoriesPanel.setConfig(loaded);
      return true;
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "MarketplaceDialog.Error.Header"),
          BaseMessages.getString(PKG, "MarketplaceDialog.ReposDirty.Reload.Error"),
          e);
      return false;
    }
  }

  /**
   * @return true if the dialog may close (saved, discarded, or clean)
   */
  private boolean confirmClose() {
    if (repositoriesPanel == null || !repositoriesPanel.isDirty()) {
      return true;
    }
    MessageBox box = new MessageBox(shell, SWT.YES | SWT.NO | SWT.CANCEL | SWT.ICON_WARNING);
    box.setText(BaseMessages.getString(PKG, "MarketplaceDialog.ReposDirty.Header"));
    box.setMessage(BaseMessages.getString(PKG, "MarketplaceDialog.ReposDirty.Close.Message"));
    int answer = box.open();
    if (answer == SWT.CANCEL) {
      return false;
    }
    if (answer == SWT.YES) {
      return repositoriesPanel.saveChanges(false);
    }
    // No — discard
    return true;
  }

  private OptionalPluginInfo selected() {
    TableItem[] items = wTable.table.getSelection();
    if (items == null || items.length == 0) {
      return null;
    }
    return (OptionalPluginInfo) items[0].getData();
  }

  /**
   * Human-readable origin for a discovery hit: repository display name when {@code source} is a
   * known repo id, otherwise the source string (e.g. {@code apache}).
   */
  private String repositoryDisplayName(String source) {
    if (StringUtils.isBlank(source)) {
      return "";
    }
    if ("apache".equalsIgnoreCase(source)) {
      return BaseMessages.getString(PKG, "MarketplaceDialog.Repository.Apache");
    }
    if (config != null) {
      MarketplaceRepository repo = config.findRepository(source);
      if (repo != null) {
        return Const.NVL(repo.displayName(), source);
      }
    }
    return source;
  }

  private void installSelected() {
    OptionalPluginInfo info = selected();
    if (info == null) {
      return;
    }
    try {
      if (repositoriesPanel != null) {
        config = repositoriesPanel.getConfig();
      }
      String groupId =
          StringUtils.isNotBlank(info.getGroupId()) ? info.getGroupId() : config.getGroupId();
      String version =
          StringUtils.isNotBlank(info.getVersion())
              ? info.getVersion()
              : MarketplaceCommand.resolveDefaultVersion(config);
      MavenCoordinates coords = new MavenCoordinates(groupId, info.getArtifactId(), version);
      String preferredRepo =
          StringUtils.isNotBlank(info.getSource()) && !"apache".equalsIgnoreCase(info.getSource())
              ? info.getSource()
              : null;
      wStatus.setText(
          BaseMessages.getString(PKG, "MarketplaceDialog.Status.Installing", coords.gav()));
      shell.update();
      new PluginInstaller(log, hopHome, config).install(coords, true, null, preferredRepo);
      MessageBox box = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
      box.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Install.Done.Header"));
      box.setMessage(
          BaseMessages.getString(PKG, "MarketplaceDialog.Install.Done.Message", coords.gav()));
      box.open();
      refreshTable(false);
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
      refreshTable(false);
      wStatus.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Status.RestartHint"));
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "MarketplaceDialog.Error.Header"),
          BaseMessages.getString(PKG, "MarketplaceDialog.Uninstall.Error", info.getArtifactId()),
          e);
    }
  }
}
