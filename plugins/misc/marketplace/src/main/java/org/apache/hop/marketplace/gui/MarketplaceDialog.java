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

import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.IRunnableWithProgress;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.marketplace.catalog.OptionalPluginCatalog;
import org.apache.hop.marketplace.catalog.OptionalPluginInfo;
import org.apache.hop.marketplace.catalog.PluginDiscovery;
import org.apache.hop.marketplace.catalog.PluginUpdates;
import org.apache.hop.marketplace.command.MarketplaceCommand;
import org.apache.hop.marketplace.config.MarketplaceConfig;
import org.apache.hop.marketplace.config.MarketplaceRepository;
import org.apache.hop.marketplace.env.EnvironmentApplier;
import org.apache.hop.marketplace.env.HopInstallSpecFiles;
import org.apache.hop.marketplace.install.HopHome;
import org.apache.hop.marketplace.install.InstallReceipt;
import org.apache.hop.marketplace.install.PendingChanges;
import org.apache.hop.marketplace.install.PluginInstaller;
import org.apache.hop.marketplace.install.PluginUninstaller;
import org.apache.hop.marketplace.resolve.MavenCoordinates;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.dialog.ProgressMonitorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
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
  /** Message key prefix of the install results; rollbacks use their own. */
  private static final String PREFIX_INSTALL = "MarketplaceDialog.Install";

  private static final String CONST_MARKETPLACE_DIALOG_ERROR_HEADER =
      "MarketplaceDialog.Error.Header";
  private static final String CONST_MARKETPLACE_DIALOG_STATUS_RESTART_HINT =
      "MarketplaceDialog.Status.RestartHint";

  private static final Class<?> PKG = MarketplaceGuiPlugin.class;
  private static final int MAX_PENDING_CHANGES_SHOWN = 15;

  /** Why the plugin table is being reloaded, which decides how the wait is presented. */
  private enum RefreshReason {
    /** Refresh button — the user is waiting, so show a cancellable progress dialog. */
    USER_REFRESH,
    /** Search box — background query, "Searching…" in the status line, no modal. */
    SEARCH,
    /** Dialog open, or after an install/uninstall — background query, status line untouched. */
    AFTER_CHANGE
  }

  private final PropsUi props;
  private final ILogChannel log = new LogChannel("Marketplace");

  private Shell shell;
  private TableView wTable;
  private Text wSearch;
  private Label wStatus;
  private Button wInstall;
  private Button wUpdate;
  private Button wRollback;
  private Button wUninstall;

  /** The plugins the table shows: discovery results, which also tell whether there is an update. */
  private List<OptionalPluginInfo> shownPlugins = List.of();

  private Path hopHome;
  private MarketplaceConfig config;
  private MarketplaceRepositoriesPanel repositoriesPanel;

  /** Incremented per discovery request so a slow, superseded answer can be discarded. */
  private int queryGeneration;

  private boolean searchPending;
  private long searchRequestedAt;

  /** Pre-filled search text, e.g. the plugin id from the missing plugins dialog. */
  private final String initialSearch;

  public MarketplaceDialog(Shell parent) {
    this(parent, null);
  }

  /**
   * @param initialSearch search box content on open, or null/blank to list everything
   */
  public MarketplaceDialog(Shell parent, String initialSearch) {
    super(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX);
    this.props = PropsUi.getInstance();
    this.initialSearch = initialSearch;
  }

  public void open() {
    Shell parent = getParent();
    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX);
    PropsUi.setLook(shell);
    shell.setImage(GuiResource.getInstance().getImageMarketplace());
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
          BaseMessages.getString(PKG, CONST_MARKETPLACE_DIALOG_ERROR_HEADER),
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
    wStatus.setText(BaseMessages.getString(PKG, CONST_MARKETPLACE_DIALOG_STATUS_RESTART_HINT));
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

    refreshTable(RefreshReason.AFTER_CHANGE);

    BaseTransformDialog.setSize(shell);
    shell.open();
    if (StringUtils.isNotBlank(initialSearch) && !wSearch.isDisposed()) {
      // Selected, so typing replaces the suggested plugin id when it turns up nothing.
      wSearch.setFocus();
      wSearch.selectAll();
    }
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

    wUninstall = new Button(comp, SWT.PUSH);
    wUninstall.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Button.Uninstall"));
    wUninstall.addListener(SWT.Selection, e -> uninstallSelected());

    wInstall = new Button(comp, SWT.PUSH);
    wInstall.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Button.Install"));
    wInstall.addListener(SWT.Selection, e -> installSelected());

    wUpdate = new Button(comp, SWT.PUSH);
    wUpdate.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Button.Update"));
    wUpdate.addListener(SWT.Selection, e -> updateSelected());

    wRollback = new Button(comp, SWT.PUSH);
    wRollback.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Button.Rollback"));
    wRollback.addListener(SWT.Selection, e -> rollbackSelected());

    Button wRefresh = new Button(comp, SWT.PUSH);
    wRefresh.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Button.Refresh"));
    wRefresh.addListener(SWT.Selection, e -> refreshTable(RefreshReason.USER_REFRESH));

    BaseTransformDialog.positionBottomButtons(
        comp,
        new Button[] {wInstall, wUpdate, wRollback, wUninstall, wRefresh},
        PropsUi.getMargin(),
        null);

    applyPluginManagePermissions();

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
    wSearch.setToolTipText(BaseMessages.getString(PKG, "MarketplaceDialog.Search.Tooltip"));
    FormData fdSearch = new FormData();
    fdSearch.left = new FormAttachment(wlSearch, PropsUi.getMargin());
    fdSearch.top = new FormAttachment(wlSearch, 0, SWT.CENTER);
    fdSearch.right = new FormAttachment(wClearSearch, -PropsUi.getMargin());
    wSearch.setLayoutData(fdSearch);
    // Set before the listener is added: the first refreshTable() below already picks this up, a
    // Modify event here would only schedule a second, identical query.
    if (StringUtils.isNotBlank(initialSearch)) {
      wSearch.setText(initialSearch.trim());
    }
    wSearch.addListener(SWT.Modify, e -> scheduleSearch());

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
          BaseMessages.getString(PKG, "MarketplaceDialog.Column.UpdateAvailable"),
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
            // No SWT.SINGLE: TableView adds SWT.MULTI, so several plugins can be installed or
            // removed in one go.
            SWT.BORDER | SWT.FULL_SELECTION | SWT.V_SCROLL,
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
    tab.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Tab.InstallSpec"));
    tab.setImage(GuiResource.getInstance().getImageMarketplace());
    Composite comp = new Composite(folder, SWT.NONE);
    PropsUi.setLook(comp);
    FormLayout layout = new FormLayout();
    layout.marginWidth = PropsUi.getFormMargin();
    layout.marginHeight = PropsUi.getFormMargin();
    comp.setLayout(layout);
    tab.setControl(comp);

    HopInstallSpecEditor.embed(
        comp,
        resolveDefaultEnvPath(),
        msg -> {
          if (wStatus != null && !wStatus.isDisposed()) {
            wStatus.setText(msg);
          }
        });
  }

  private String resolveDefaultEnvPath() {
    String last =
        org.apache.hop.ui.hopgui.shared.AuditManagerGuiUtil.getLastUsedValue(
            HopInstallSpecEditor.AUDIT_TYPE_ENV_FILES);
    if (HopInstallSpecFiles.exists(last, hopGuiVariables())) {
      return last;
    }
    if (hopHome != null) {
      String fullClient = hopHome.resolve("full-client-env.yaml").toString();
      if (HopInstallSpecFiles.exists(fullClient, hopGuiVariables())) {
        return fullClient;
      }
    }
    Path discovered = EnvironmentApplier.resolveEnvironmentFile(hopHome, null);
    return discovered == null ? null : discovered.toString();
  }

  private org.apache.hop.core.variables.IVariables hopGuiVariables() {
    try {
      HopGui hopGui = HopGui.getInstance();
      if (hopGui != null && hopGui.getVariables() != null) {
        return hopGui.getVariables();
      }
    } catch (Exception ignored) {
      // dialog constructed outside Hop Gui
    }
    return Variables.getADefaultVariableSpace();
  }

  private void createRepositoriesTab(CTabFolder folder) {
    CTabItem tab = new CTabItem(folder, SWT.NONE);
    tab.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Tab.Repositories"));
    tab.setImage(GuiResource.getInstance().getImageRepository());
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
   * <p>Discovery is a live Nexus search over every browse-enabled repository, so it never runs on
   * the UI thread: {@link RefreshReason#USER_REFRESH} gets a cancellable progress dialog, the other
   * reasons run quietly in the background and repopulate the table when they land.
   */
  private void refreshTable(RefreshReason reason) {
    if (reason == RefreshReason.USER_REFRESH && !ensureRepositoriesSavedForRefresh()) {
      return;
    }
    // Keep shared in-memory config (after optional save/discard). Do not reload from disk on
    // every search keystroke — that would drop unsaved repository edits.
    if (repositoriesPanel != null) {
      config = repositoriesPanel.getConfig();
    }
    // Filter only when more than 2 characters are typed (same as explorer/metadata search).
    String searchText = wSearch != null ? wSearch.getText() : "";
    String filter = searchText != null && searchText.trim().length() > 2 ? searchText.trim() : null;
    MarketplaceConfig snapshot = config;

    if (reason == RefreshReason.USER_REFRESH) {
      queryWithProgress(filter, snapshot);
    } else {
      queryInBackground(filter, snapshot, reason == RefreshReason.SEARCH);
    }
  }

  /** Refresh button: modal, cancellable, because the user explicitly asked and is waiting. */
  private void queryWithProgress(String filter, MarketplaceConfig snapshot) {
    List<OptionalPluginInfo> found = new ArrayList<>();
    IRunnableWithProgress operation =
        monitor -> {
          // Discovery cannot report intermediate progress, so this is a one-step task: the bar
          // shows "busy", the label says what for.
          monitor.beginTask(BaseMessages.getString(PKG, "MarketplaceDialog.Progress.Searching"), 1);
          try {
            found.addAll(PluginDiscovery.query(filter, null, snapshot, log));
          } catch (Exception e) {
            // No monitor.done() before throwing — see the note in runInstall().
            throw new InvocationTargetException(e, e.getMessage());
          }
          monitor.worked(1);
          monitor.done();
        };
    try {
      new ProgressMonitorDialog(shell).run(true, operation);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return;
    } catch (InvocationTargetException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, CONST_MARKETPLACE_DIALOG_ERROR_HEADER),
          BaseMessages.getString(PKG, "MarketplaceDialog.Refresh.Error"),
          e.getCause() == null ? e : e.getCause());
      return;
    }
    populateTable(found);
  }

  /**
   * Search box and post-install refreshes: a modal dialog per keystroke would be worse than the
   * freeze it replaces, so query on a worker thread and swap the rows in when the answer arrives.
   *
   * <p>Results from a superseded query are dropped — with typing debounced there is rarely more
   * than one in flight, but a slow repository could still finish after a newer query.
   */
  private void queryInBackground(String filter, MarketplaceConfig snapshot, boolean showSearching) {
    Display display = shell.getDisplay();
    final int generation = ++queryGeneration;
    if (showSearching && wStatus != null && !wStatus.isDisposed()) {
      wStatus.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Progress.Searching"));
    }
    Thread worker =
        new Thread(
            () -> {
              List<OptionalPluginInfo> found;
              try {
                found = PluginDiscovery.query(filter, null, snapshot, log);
              } catch (Exception e) {
                log.logError("Marketplace discovery failed", e);
                found = List.of();
              }
              List<OptionalPluginInfo> result = found;
              display.asyncExec(
                  () -> {
                    // Stale answer, or the dialog closed while we were waiting on the network.
                    if (generation != queryGeneration || shell.isDisposed()) {
                      return;
                    }
                    populateTable(result);
                    if (showSearching && !wStatus.isDisposed()) {
                      wStatus.setText(
                          BaseMessages.getString(
                              PKG, CONST_MARKETPLACE_DIALOG_STATUS_RESTART_HINT));
                    }
                  });
            },
            "Hop-Marketplace-Discovery");
    worker.setDaemon(true);
    worker.start();
  }

  /** Fill the table from discovery results. UI thread only. */
  private void populateTable(List<OptionalPluginInfo> plugins) {
    wTable.table.removeAll();
    shownPlugins = List.copyOf(plugins);
    for (OptionalPluginInfo info : plugins) {
      // Column 0 is the (hidden) index column; data columns are 1-based.
      TableItem item = new TableItem(wTable.table, SWT.NONE);
      item.setText(1, Const.NVL(displayName(info), ""));
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
      if (onDisk && receipt != null && !receipt.isUninstallPending()) {
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
      OptionalPluginInfo newer = PluginUpdates.newerVersion(receipt, plugins);
      item.setText(7, newer == null ? "" : newer.getVersion());
      item.setText(8, Const.NVL(info.getDescription(), ""));
      item.setData(info);
    }
    wTable.optimizeTableView();
  }

  /**
   * Coalesce keystrokes into one discovery call. Without this, every character typed in the search
   * box fires a full Nexus search (paged, 60s per request) against every repository.
   */
  private void scheduleSearch() {
    if (shell == null || shell.isDisposed()) {
      return;
    }
    searchRequestedAt = System.currentTimeMillis();
    if (searchPending) {
      return;
    }
    searchPending = true;
    shell.getDisplay().timerExec(ConstUi.FILTER_DEBOUNCE_MS, this::fireDebouncedSearch);
  }

  private void fireDebouncedSearch() {
    if (shell == null || shell.isDisposed()) {
      searchPending = false;
      return;
    }
    long quietFor = System.currentTimeMillis() - searchRequestedAt;
    if (quietFor < ConstUi.FILTER_DEBOUNCE_MS) {
      // Still typing — check again once the remaining quiet period has elapsed.
      shell
          .getDisplay()
          .timerExec((int) (ConstUi.FILTER_DEBOUNCE_MS - quietFor), this::fireDebouncedSearch);
      return;
    }
    searchPending = false;
    refreshTable(RefreshReason.SEARCH);
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
          BaseMessages.getString(PKG, CONST_MARKETPLACE_DIALOG_ERROR_HEADER),
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

  /** The plugins highlighted in the table, in table order. Empty when nothing is selected. */
  private List<OptionalPluginInfo> selected() {
    TableItem[] items = wTable.table.getSelection();
    if (items == null) {
      return List.of();
    }
    List<OptionalPluginInfo> selection = new ArrayList<>();
    for (TableItem item : items) {
      if (item.getData() instanceof OptionalPluginInfo info) {
        selection.add(info);
      }
    }
    return selection;
  }

  /** Plugin names as the table shows them, one per line, for confirmations and summaries. */
  private static String nameList(List<String> names) {
    return names.stream().map(name -> "- " + name).collect(Collectors.joining("\n"));
  }

  private static String displayName(OptionalPluginInfo info) {
    return Const.NVL(info.getName(), info.getArtifactId());
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

  /**
   * One plugin ready to install: where to get it, and what to call it while it downloads. A
   * rollback puts back the version that the installed one replaced instead.
   */
  private record InstallRequest(
      MavenCoordinates coordinates, String preferredRepo, String displayName, boolean rollback) {}

  /** What a batch install did: what went in, what didn't, and whether the user stopped it. */
  private record InstallOutcome(
      List<String> installed, Map<String, Exception> failures, boolean cancelled) {}

  /** Coordinates, repository and display name for one selected row. */
  private InstallRequest toInstallRequest(OptionalPluginInfo info) {
    String groupId =
        StringUtils.isNotBlank(info.getGroupId()) ? info.getGroupId() : config.getGroupId();
    String version =
        StringUtils.isNotBlank(info.getVersion())
            ? info.getVersion()
            : MarketplaceCommand.resolveDefaultVersion(config);
    String preferredRepo =
        StringUtils.isNotBlank(info.getSource()) && !"apache".equalsIgnoreCase(info.getSource())
            ? info.getSource()
            : null;
    return new InstallRequest(
        new MavenCoordinates(groupId, info.getArtifactId(), version),
        preferredRepo,
        // Same name the table shows, not the Maven coordinate.
        displayName(info),
        false);
  }

  /** Enable Install/Update/Rollback/Uninstall only when plugin manage permission is allowed. */
  private void applyPluginManagePermissions() {
    boolean canManage = MarketplaceSecurity.canManagePlugins();
    if (wInstall != null && !wInstall.isDisposed()) {
      wInstall.setEnabled(canManage);
      wInstall.setToolTipText(
          canManage
              ? ""
              : BaseMessages.getString(PKG, "MarketplaceDialog.Button.Install.RequiresAdmin"));
    }
    for (Button button : new Button[] {wUpdate, wRollback}) {
      if (button != null && !button.isDisposed()) {
        button.setEnabled(canManage);
        button.setToolTipText(
            BaseMessages.getString(
                PKG,
                canManage
                    ? (button == wUpdate
                        ? "MarketplaceDialog.Button.Update.Tooltip"
                        : "MarketplaceDialog.Button.Rollback.Tooltip")
                    : "MarketplaceDialog.Button.Install.RequiresAdmin"));
      }
    }
    if (wUninstall != null && !wUninstall.isDisposed()) {
      wUninstall.setEnabled(canManage);
      wUninstall.setToolTipText(
          canManage
              ? ""
              : BaseMessages.getString(PKG, "MarketplaceDialog.Button.Uninstall.RequiresAdmin"));
    }
  }

  private void installSelected() {
    if (!MarketplaceSecurity.checkManagePlugins()) {
      return;
    }
    List<OptionalPluginInfo> selection = selected();
    if (selection.isEmpty()) {
      return;
    }
    if (repositoriesPanel != null) {
      config = repositoriesPanel.getConfig();
    }
    installRequests(selection.stream().map(this::toInstallRequest).toList());
  }

  /** Install the newest version of the selected plugins that have one. */
  private void updateSelected() {
    if (!MarketplaceSecurity.checkManagePlugins()) {
      return;
    }
    List<OptionalPluginInfo> selection = selected();
    if (selection.isEmpty()) {
      return;
    }
    if (repositoriesPanel != null) {
      config = repositoriesPanel.getConfig();
    }
    List<InstallRequest> requests = new ArrayList<>();
    Set<String> seen = new HashSet<>();
    for (OptionalPluginInfo info : selection) {
      InstallReceipt receipt = readReceiptOrLog(info.getArtifactId());
      OptionalPluginInfo newer = PluginUpdates.newerVersion(receipt, shownPlugins);
      if (newer == null || !seen.add(receipt.getArtifactId())) {
        continue;
      }
      InstallRequest request = toInstallRequest(newer);
      String preferred =
          PluginUpdates.preferredRepository(new PluginUpdates.Update(receipt, newer), config);
      requests.add(
          new InstallRequest(request.coordinates(), preferred, request.displayName(), false));
    }
    if (requests.isEmpty()) {
      MessageBox box = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
      box.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Update.None.Header"));
      box.setMessage(BaseMessages.getString(PKG, "MarketplaceDialog.Update.None.Message"));
      box.open();
      return;
    }
    installRequests(requests);
  }

  /** Put back the version that the installed version of the selected plugins replaced. */
  private void rollbackSelected() {
    if (!MarketplaceSecurity.checkManagePlugins()) {
      return;
    }
    List<OptionalPluginInfo> selection = selected();
    if (selection.isEmpty()) {
      return;
    }
    List<InstallRequest> requests = new ArrayList<>();
    List<String> versions = new ArrayList<>();
    Set<String> seen = new HashSet<>();
    for (OptionalPluginInfo info : selection) {
      InstallReceipt receipt = readReceiptOrLog(info.getArtifactId());
      String earlier = PluginInstaller.rollbackVersion(hopHome, receipt);
      if (earlier == null || !seen.add(receipt.getArtifactId())) {
        continue;
      }
      requests.add(
          new InstallRequest(
              new MavenCoordinates(
                  Const.NVL(receipt.getGroupId(), config.getGroupId()),
                  receipt.getArtifactId(),
                  earlier),
              null,
              displayName(info),
              true));
      versions.add(
          BaseMessages.getString(
              PKG,
              "MarketplaceDialog.Rollback.Confirm.Version",
              displayName(info),
              receipt.getVersion(),
              earlier));
    }
    if (requests.isEmpty()) {
      MessageBox box = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
      box.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Rollback.None.Header"));
      box.setMessage(BaseMessages.getString(PKG, "MarketplaceDialog.Rollback.None.Message"));
      box.open();
      return;
    }
    MessageBox confirm = new MessageBox(shell, SWT.YES | SWT.NO | SWT.ICON_QUESTION);
    confirm.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Rollback.Confirm.Header"));
    confirm.setMessage(
        BaseMessages.getString(
            PKG, "MarketplaceDialog.Rollback.Confirm.Message", nameList(versions)));
    if (confirm.open() != SWT.YES) {
      return;
    }
    installRequests(requests);
  }

  /** The receipt of a plugin, or null when there is none or it cannot be read. */
  private InstallReceipt readReceiptOrLog(String artifactId) {
    try {
      return PluginInstaller.readReceipt(hopHome, artifactId);
    } catch (Exception e) {
      log.logError("Unable to read receipt for " + artifactId, e);
      return null;
    }
  }

  /**
   * Install or roll back the requested plugins, then report the outcome. All requests are of one
   * kind.
   */
  private void installRequests(List<InstallRequest> requests) {
    // The messages say "installed" or "rolled back".
    String messages = requests.get(0).rollback() ? "MarketplaceDialog.Rollback" : PREFIX_INSTALL;
    InstallOutcome outcome;
    try {
      wStatus.setText(installingStatus(requests));
      shell.update();
      outcome = runInstall(requests);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      showInstallError(requests, e, messages);
      return;
    } catch (Exception e) {
      showInstallError(requests, e, messages);
      return;
    }

    // The table reflects what is on disk either way: a batch can be part installed.
    refreshTable(RefreshReason.AFTER_CHANGE);
    if (!outcome.failures().isEmpty()) {
      showInstallFailures(outcome, messages);
      wStatus.setText(BaseMessages.getString(PKG, CONST_MARKETPLACE_DIALOG_STATUS_RESTART_HINT));
      return;
    }
    if (outcome.cancelled()) {
      // Cancelled by the user — no success popup.
      wStatus.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Status.Cancelled"));
      return;
    }
    boolean single = outcome.installed().size() == 1;
    MessageBox box = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
    box.setText(
        BaseMessages.getString(PKG, messages + (single ? ".Done.Header" : ".Done.HeaderMultiple")));
    box.setMessage(
        (single
                ? BaseMessages.getString(
                    PKG, messages + ".Done.Message", outcome.installed().get(0))
                : BaseMessages.getString(
                    PKG,
                    messages + ".Done.MessageMultiple",
                    Integer.toString(outcome.installed().size()),
                    nameList(outcome.installed())))
            + pendingChangesNote(hopHome, log));
    box.open();
    wStatus.setText(BaseMessages.getString(PKG, CONST_MARKETPLACE_DIALOG_STATUS_RESTART_HINT));
  }

  private String installingStatus(List<InstallRequest> requests) {
    String key =
        requests.get(0).rollback()
            ? "MarketplaceDialog.Status.RollingBack"
            : "MarketplaceDialog.Status.Installing";
    return requests.size() == 1
        ? BaseMessages.getString(PKG, key, requests.get(0).coordinates().gav())
        : BaseMessages.getString(PKG, key + "Count", Integer.toString(requests.size()));
  }

  /** The batch never started, or the dialog itself broke down. */
  private void showInstallError(List<InstallRequest> requests, Exception e, String messages) {
    // ProgressMonitorDialog wraps the real failure; unwrap so the dialog shows the cause.
    Throwable cause =
        e instanceof InvocationTargetException ite && ite.getCause() != null ? ite.getCause() : e;
    new ErrorDialog(
        shell,
        BaseMessages.getString(PKG, CONST_MARKETPLACE_DIALOG_ERROR_HEADER),
        BaseMessages.getString(
            PKG,
            messages + ".Error",
            requests.stream().map(InstallRequest::displayName).collect(Collectors.joining(", "))),
        cause);
  }

  /**
   * Report the plugins the batch could not install. One dialog for the lot: a failing repository
   * usually takes every plugin down with it, and a popup per plugin would have to be clicked away
   * as many times.
   */
  private void showInstallFailures(InstallOutcome outcome, String messages) {
    StringBuilder detail = new StringBuilder();
    for (Map.Entry<String, Exception> failure : outcome.failures().entrySet()) {
      log.logError(
          (messages.equals(PREFIX_INSTALL) ? "Failed to install " : "Failed to roll back ")
              + failure.getKey(),
          failure.getValue());
      detail
          .append("- ")
          .append(failure.getKey())
          .append(": ")
          .append(Const.NVL(failure.getValue().getMessage(), failure.getValue().toString()))
          .append(Const.CR);
    }
    String header =
        outcome.failures().size() == 1
            ? BaseMessages.getString(
                PKG, messages + ".Error", outcome.failures().keySet().iterator().next())
            : BaseMessages.getString(
                PKG,
                messages + ".Error.Multiple",
                Integer.toString(outcome.failures().size()),
                Integer.toString(outcome.failures().size() + outcome.installed().size()));
    // The first failure carries the stack trace; the rest are summarised in the message.
    Exception first = outcome.failures().values().iterator().next();
    new ErrorDialog(
        shell,
        BaseMessages.getString(PKG, CONST_MARKETPLACE_DIALOG_ERROR_HEADER),
        header + Const.CR + Const.CR + detail,
        first);
  }

  /**
   * Download and install on a worker thread behind a cancellable progress dialog, so the UI keeps
   * repainting and the user can see the zips arrive.
   *
   * <p>{@link ProgressMonitorDialog#run(boolean, IRunnableWithProgress)} pumps the event loop until
   * the work finishes and only then returns, so everything after the call is back on the UI thread
   * and needs no marshalling.
   *
   * <p>A failing plugin does not abort the batch — one unavailable artifact shouldn't cost the user
   * the other nine installs — so failures are collected and reported together at the end. Nothing
   * is thrown out of the operation, which also sidesteps a trap: {@code monitor.done()} disposes
   * the progress shell and ends the pump loop, so anything thrown after it is never observed and a
   * failed install would be reported as a success.
   */
  private InstallOutcome runInstall(List<InstallRequest> requests)
      throws InvocationTargetException, InterruptedException {
    PluginInstaller installer = new PluginInstaller(log, hopHome, config);
    List<String> installed = new ArrayList<>();
    Map<String, Exception> failures = new LinkedHashMap<>();
    AtomicBoolean cancelled = new AtomicBoolean(false);
    ProgressMonitorDialog dialog = new ProgressMonitorDialog(shell);

    IRunnableWithProgress operation =
        monitor -> {
          ProgressMonitorInstallListener listener = new ProgressMonitorInstallListener(monitor);
          listener.begin(
              installTaskName(requests),
              requests.size() == 1 ? requests.get(0).displayName() : null);
          for (int i = 0; i < requests.size(); i++) {
            if (monitor.isCanceled()) {
              cancelled.set(true);
              break;
            }
            InstallRequest request = requests.get(i);
            // Drives the "3 of 12" line and scales the bar across the batch.
            listener.item(request.displayName(), i, requests.size());
            try {
              if (request.rollback()) {
                installer.rollback(request.coordinates().artifactId(), listener);
              } else {
                installer.install(
                    request.coordinates(), true, null, request.preferredRepo(), listener);
              }
              installed.add(request.coordinates().gav());
            } catch (Exception e) {
              // Catching Exception (not just HopException) is what keeps a RuntimeException from
              // killing the worker thread silently and hanging the dialog forever.
              if (monitor.isCanceled()) {
                cancelled.set(true);
                break;
              }
              failures.put(request.displayName(), e);
            }
          }
          listener.complete();
          monitor.done();
        };

    dialog.run(true, operation);
    return new InstallOutcome(installed, failures, cancelled.get());
  }

  private String installTaskName(List<InstallRequest> requests) {
    String key =
        requests.get(0).rollback()
            ? "MarketplaceDialog.Progress.RollingBack"
            : "MarketplaceDialog.Progress.Installing";
    return requests.size() == 1
        ? BaseMessages.getString(PKG, key, requests.get(0).displayName())
        : BaseMessages.getString(PKG, key + "Count", Integer.toString(requests.size()));
  }

  private void uninstallSelected() {
    if (!MarketplaceSecurity.checkManagePlugins()) {
      return;
    }
    List<OptionalPluginInfo> selection = selected();
    if (selection.isEmpty()) {
      return;
    }
    List<OptionalPluginInfo> removable = new ArrayList<>();
    List<String> withoutReceipt = new ArrayList<>();
    try {
      for (OptionalPluginInfo info : selection) {
        if (PluginInstaller.readReceipt(hopHome, info.getArtifactId()) == null) {
          withoutReceipt.add(info.getArtifactId());
        } else {
          removable.add(info);
        }
      }
    } catch (Exception e) {
      showUninstallError(selection.stream().map(OptionalPluginInfo::getArtifactId).toList(), e);
      return;
    }

    // Plugins that ship with the client have no receipt; say so instead of silently skipping them.
    if (!withoutReceipt.isEmpty()) {
      MessageBox box = new MessageBox(shell, SWT.OK | SWT.ICON_WARNING);
      box.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Uninstall.NoReceipt.Header"));
      box.setMessage(
          withoutReceipt.size() == 1
              ? BaseMessages.getString(
                  PKG, "MarketplaceDialog.Uninstall.NoReceipt.Message", withoutReceipt.get(0))
              : BaseMessages.getString(
                  PKG,
                  "MarketplaceDialog.Uninstall.NoReceipt.MessageMultiple",
                  nameList(withoutReceipt)));
      box.open();
      if (removable.isEmpty()) {
        return;
      }
    }

    List<String> artifactIds = removable.stream().map(OptionalPluginInfo::getArtifactId).toList();
    MessageBox confirm = new MessageBox(shell, SWT.YES | SWT.NO | SWT.ICON_QUESTION);
    confirm.setText(
        BaseMessages.getString(
            PKG,
            artifactIds.size() == 1
                ? "MarketplaceDialog.Uninstall.Confirm.Header"
                : "MarketplaceDialog.Uninstall.Confirm.HeaderMultiple"));
    confirm.setMessage(
        artifactIds.size() == 1
            ? BaseMessages.getString(
                PKG, "MarketplaceDialog.Uninstall.Confirm.Message", artifactIds.get(0))
            : BaseMessages.getString(
                PKG,
                "MarketplaceDialog.Uninstall.Confirm.MessageMultiple",
                Integer.toString(artifactIds.size()),
                nameList(artifactIds)));
    if (confirm.open() != SWT.YES) {
      return;
    }

    PluginUninstaller uninstaller = new PluginUninstaller(log, hopHome);
    List<String> failed = new ArrayList<>();
    Exception firstFailure = null;
    for (String artifactId : artifactIds) {
      try {
        uninstaller.uninstall(artifactId);
      } catch (Exception e) {
        // Same reasoning as the install batch: report everything that went wrong, once.
        log.logError("Failed to uninstall " + artifactId, e);
        failed.add(artifactId);
        if (firstFailure == null) {
          firstFailure = e;
        }
      }
    }
    refreshTable(RefreshReason.AFTER_CHANGE);
    if (firstFailure != null) {
      showUninstallError(failed, firstFailure);
      return;
    }
    wStatus.setText(BaseMessages.getString(PKG, CONST_MARKETPLACE_DIALOG_STATUS_RESTART_HINT));
    String pending = pendingChangesNote(hopHome, log);
    if (!pending.isEmpty()) {
      MessageBox box = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
      box.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Pending.Header"));
      box.setMessage(pending.strip());
      box.open();
    }
  }

  /**
   * For a result message: the file changes Hop makes the next time it starts, because the files
   * were in use. Empty when there are none.
   */
  static String pendingChangesNote(Path hopHome, ILogChannel log) {
    if (hopHome == null) {
      return "";
    }
    List<PendingChanges.PendingEffect> pending;
    try {
      pending = new PendingChanges(hopHome).effects();
    } catch (HopException e) {
      log.logError("Unable to read the pending marketplace changes", e);
      return "";
    }
    if (pending.isEmpty()) {
      return "";
    }
    int shown = Math.min(pending.size(), MAX_PENDING_CHANGES_SHOWN);
    StringBuilder list = new StringBuilder();
    for (PendingChanges.PendingEffect effect : pending.subList(0, shown)) {
      list.append("\n- ")
          .append(
              BaseMessages.getString(
                  PKG, "MarketplaceDialog.Pending." + effect.effect().getLabel(), effect.target()));
    }
    if (pending.size() > shown) {
      list.append("\n")
          .append(
              BaseMessages.getString(
                  PKG, "MarketplaceDialog.Pending.More", Integer.toString(pending.size() - shown)));
    }
    return "\n\n" + BaseMessages.getString(PKG, "MarketplaceDialog.Pending.Message") + list;
  }

  private void showUninstallError(List<String> artifactIds, Exception e) {
    new ErrorDialog(
        shell,
        BaseMessages.getString(PKG, CONST_MARKETPLACE_DIALOG_ERROR_HEADER),
        BaseMessages.getString(
            PKG, "MarketplaceDialog.Uninstall.Error", String.join(", ", artifactIds)),
        e);
  }
}
