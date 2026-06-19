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

package org.apache.hop.projects.gui;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.projects.git.GitRepoAuth;
import org.apache.hop.projects.git.GitRepoProvider;
import org.apache.hop.projects.git.GitRepositoryBrowser;
import org.apache.hop.projects.git.GitRepositoryInfo;
import org.apache.hop.projects.git.GitRepositoryPage;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

/**
 * Dialog that lets users browse and select a repository from GitHub, GitHub Enterprise, GitLab, or
 * Bitbucket. Returns the selected HTTPS clone URL (and repo name) to the calling dialog.
 */
public class SelectRepositoryDialog extends Dialog {

  private static final Class<?> PKG = SelectRepositoryDialog.class;

  private static final int COL_NAME = 0;
  private static final int COL_OWNER = 1;
  private static final int COL_VISIBILITY = 2;
  private static final int COL_UPDATED = 3;

  private final PropsUi props;

  /** Previously chosen provider (pre-selects the combo). */
  private final GitRepoProvider initialProvider;

  /** Previously entered token (pre-fills the token field). */
  private final String initialToken;

  private Shell shell;

  private Combo wProvider;
  private Label wlHost;
  private Text wHost;
  private Label wlToken;
  private Text wToken;
  private Label wlUsername;
  private Text wUsername;
  private Label wlPassword;
  private Text wPassword;
  private Text wFilter;
  private Table wRepoTable;
  private Button wLoadMore;
  private Label wStatus;

  /** The HTTPS clone URL of the selected repository, or {@code null} if cancelled. */
  private String selectedCloneUrl;

  /** The name of the selected repository, or {@code null} if cancelled. */
  @Getter private String selectedRepoName;

  private final List<GitRepositoryInfo> loadedRepos = new ArrayList<>();
  private int currentPage = 1;
  private boolean hasMore = false;

  public SelectRepositoryDialog(
      Shell parent, GitRepoProvider initialProvider, String initialToken) {
    super(parent, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.RESIZE);
    this.initialProvider = initialProvider != null ? initialProvider : GitRepoProvider.GITHUB_CLOUD;
    this.initialToken = initialToken != null ? initialToken : "";
    this.props = PropsUi.getInstance();
  }

  /**
   * Opens the dialog.
   *
   * @return the HTTPS clone URL of the selected repository, or {@code null} if cancelled.
   */
  public String open() {
    Shell parent = getParent();
    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.RESIZE);
    shell.setText(BaseMessages.getString(PKG, "SelectRepositoryDialog.Shell.Name"));
    shell.setImage(
        GuiResource.getInstance()
            .getImage(
                "project.svg",
                PKG.getClassLoader(),
                ConstUi.SMALL_ICON_SIZE,
                ConstUi.SMALL_ICON_SIZE));

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();
    shell.setLayout(formLayout);
    shell.setMinimumSize(800, 750);
    PropsUi.setLook(shell);

    int margin = PropsUi.getMargin();
    int middle = props.getMiddlePct();

    // --- Bottom buttons ---
    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString("System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wOk.setEnabled(false); // enabled only when a repo is selected

    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString("System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());

    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wOk, wCancel}, margin * 3, null);

    // Status bar just above buttons
    wStatus = new Label(shell, SWT.LEFT);
    wStatus.setText("");
    PropsUi.setLook(wStatus);
    FormData fdStatus = new FormData();
    fdStatus.left = new FormAttachment(0, margin);
    fdStatus.right = new FormAttachment(100, -margin);
    fdStatus.bottom = new FormAttachment(wOk, -margin);
    wStatus.setLayoutData(fdStatus);

    // --- Scrollable content area ---
    Composite comp = new Composite(shell, SWT.NONE);
    comp.setLayout(new FormLayout());
    PropsUi.setLook(comp);
    FormData fdComp = new FormData();
    fdComp.left = new FormAttachment(0, 0);
    fdComp.right = new FormAttachment(100, 0);
    fdComp.top = new FormAttachment(0, 0);
    fdComp.bottom = new FormAttachment(wStatus, -margin);
    comp.setLayoutData(fdComp);

    // --- Provider ---
    Label wlProvider = new Label(comp, SWT.RIGHT);
    wlProvider.setText(BaseMessages.getString(PKG, "SelectRepositoryDialog.Label.Provider"));
    PropsUi.setLook(wlProvider);
    FormData fdlProvider = new FormData();
    fdlProvider.left = new FormAttachment(0, 0);
    fdlProvider.right = new FormAttachment(middle, 0);
    fdlProvider.top = new FormAttachment(0, margin * 2);
    wlProvider.setLayoutData(fdlProvider);

    wProvider = new Combo(comp, SWT.DROP_DOWN | SWT.READ_ONLY);
    wProvider.setItems(GitRepoProvider.displayNames());
    PropsUi.setLook(wProvider);
    FormData fdProvider = new FormData();
    fdProvider.left = new FormAttachment(middle, margin);
    fdProvider.right = new FormAttachment(99, 0);
    fdProvider.top = new FormAttachment(wlProvider, 0, SWT.CENTER);
    wProvider.setLayoutData(fdProvider);
    wProvider.addListener(SWT.Selection, e -> updateProviderState());

    // --- Host (GHE / self-hosted GitLab) ---
    wlHost = new Label(comp, SWT.RIGHT);
    wlHost.setText(BaseMessages.getString(PKG, "SelectRepositoryDialog.Label.Host"));
    PropsUi.setLook(wlHost);
    FormData fdlHost = new FormData();
    fdlHost.left = new FormAttachment(0, 0);
    fdlHost.right = new FormAttachment(middle, 0);
    fdlHost.top = new FormAttachment(wProvider, margin);
    wlHost.setLayoutData(fdlHost);

    wHost = new Text(comp, SWT.SINGLE | SWT.BORDER);
    PropsUi.setLook(wHost);
    wHost.setToolTipText(BaseMessages.getString(PKG, "SelectRepositoryDialog.Label.Host.Tooltip"));
    FormData fdHost = new FormData();
    fdHost.left = new FormAttachment(middle, margin);
    fdHost.right = new FormAttachment(99, 0);
    fdHost.top = new FormAttachment(wlHost, 0, SWT.CENTER);
    wHost.setLayoutData(fdHost);

    // --- Token (GitHub, GitLab) ---
    wlToken = new Label(comp, SWT.RIGHT);
    wlToken.setText(BaseMessages.getString(PKG, "SelectRepositoryDialog.Label.Token"));
    PropsUi.setLook(wlToken);
    FormData fdlToken = new FormData();
    fdlToken.left = new FormAttachment(0, 0);
    fdlToken.right = new FormAttachment(middle, 0);
    fdlToken.top = new FormAttachment(wHost, margin);
    wlToken.setLayoutData(fdlToken);

    wToken = new Text(comp, SWT.SINGLE | SWT.BORDER | SWT.PASSWORD);
    PropsUi.setLook(wToken);
    wToken.setToolTipText(
        BaseMessages.getString(PKG, "SelectRepositoryDialog.Label.Token.Tooltip"));
    FormData fdToken = new FormData();
    fdToken.left = new FormAttachment(middle, margin);
    fdToken.right = new FormAttachment(99, 0);
    fdToken.top = new FormAttachment(wlToken, 0, SWT.CENTER);
    wToken.setLayoutData(fdToken);

    // --- Username (Bitbucket) ---
    wlUsername = new Label(comp, SWT.RIGHT);
    wlUsername.setText(BaseMessages.getString(PKG, "SelectRepositoryDialog.Label.Username"));
    PropsUi.setLook(wlUsername);
    FormData fdlUsername = new FormData();
    fdlUsername.left = new FormAttachment(0, 0);
    fdlUsername.right = new FormAttachment(middle, 0);
    fdlUsername.top = new FormAttachment(wHost, margin);
    wlUsername.setLayoutData(fdlUsername);

    wUsername = new Text(comp, SWT.SINGLE | SWT.BORDER);
    PropsUi.setLook(wUsername);
    FormData fdUsername = new FormData();
    fdUsername.left = new FormAttachment(middle, margin);
    fdUsername.right = new FormAttachment(99, 0);
    fdUsername.top = new FormAttachment(wlUsername, 0, SWT.CENTER);
    wUsername.setLayoutData(fdUsername);

    // --- App password (Bitbucket) ---
    wlPassword = new Label(comp, SWT.RIGHT);
    wlPassword.setText(BaseMessages.getString(PKG, "SelectRepositoryDialog.Label.AppPassword"));
    PropsUi.setLook(wlPassword);
    FormData fdlPassword = new FormData();
    fdlPassword.left = new FormAttachment(0, 0);
    fdlPassword.right = new FormAttachment(middle, 0);
    fdlPassword.top = new FormAttachment(wUsername, margin);
    wlPassword.setLayoutData(fdlPassword);

    wPassword = new Text(comp, SWT.SINGLE | SWT.BORDER | SWT.PASSWORD);
    PropsUi.setLook(wPassword);
    wPassword.setToolTipText(
        BaseMessages.getString(PKG, "SelectRepositoryDialog.Label.AppPassword.Tooltip"));
    FormData fdPassword = new FormData();
    fdPassword.left = new FormAttachment(middle, margin);
    fdPassword.right = new FormAttachment(99, 0);
    fdPassword.top = new FormAttachment(wlPassword, 0, SWT.CENTER);
    wPassword.setLayoutData(fdPassword);

    // --- Filter + Search button ---
    Label wlFilter = new Label(comp, SWT.RIGHT);
    wlFilter.setText(BaseMessages.getString(PKG, "SelectRepositoryDialog.Label.Filter"));
    PropsUi.setLook(wlFilter);
    FormData fdlFilter = new FormData();
    fdlFilter.left = new FormAttachment(0, 0);
    fdlFilter.right = new FormAttachment(middle, 0);
    fdlFilter.top = new FormAttachment(wPassword, margin);
    wlFilter.setLayoutData(fdlFilter);

    Button wSearch = new Button(comp, SWT.PUSH);
    wSearch.setText(BaseMessages.getString(PKG, "SelectRepositoryDialog.Button.Search"));
    FormData fdSearch = new FormData();
    fdSearch.right = new FormAttachment(99, 0);
    fdSearch.top = new FormAttachment(wlFilter, 0, SWT.CENTER);
    wSearch.setLayoutData(fdSearch);
    wSearch.addListener(SWT.Selection, e -> loadRepos(true));
    PropsUi.setLook(wSearch);

    wFilter = new Text(comp, SWT.SINGLE | SWT.BORDER);
    PropsUi.setLook(wFilter);
    wFilter.setToolTipText(
        BaseMessages.getString(PKG, "SelectRepositoryDialog.Label.Filter.Tooltip"));
    FormData fdFilter = new FormData();
    fdFilter.left = new FormAttachment(middle, margin);
    fdFilter.right = new FormAttachment(wSearch, -margin);
    fdFilter.top = new FormAttachment(wlFilter, 0, SWT.CENTER);
    wFilter.setLayoutData(fdFilter);
    // Allow pressing Enter in filter field to trigger search
    wFilter.addListener(SWT.DefaultSelection, e -> loadRepos(true));

    // --- Repository table ---
    wRepoTable = new Table(comp, SWT.BORDER | SWT.FULL_SELECTION | SWT.SINGLE | SWT.V_SCROLL);
    wRepoTable.setHeaderVisible(true);
    wRepoTable.setLinesVisible(true);
    PropsUi.setLook(wRepoTable);

    TableColumn colName = new TableColumn(wRepoTable, SWT.LEFT);
    colName.setText(BaseMessages.getString(PKG, "SelectRepositoryDialog.Table.Col.Name"));
    colName.setWidth(220);

    TableColumn colOwner = new TableColumn(wRepoTable, SWT.LEFT);
    colOwner.setText(BaseMessages.getString(PKG, "SelectRepositoryDialog.Table.Col.Owner"));
    colOwner.setWidth(160);

    TableColumn colVisibility = new TableColumn(wRepoTable, SWT.LEFT);
    colVisibility.setText(
        BaseMessages.getString(PKG, "SelectRepositoryDialog.Table.Col.Visibility"));
    colVisibility.setWidth(80);

    TableColumn colUpdated = new TableColumn(wRepoTable, SWT.LEFT);
    colUpdated.setText(BaseMessages.getString(PKG, "SelectRepositoryDialog.Table.Col.Updated"));
    colUpdated.setWidth(110);

    FormData fdTable = new FormData();
    fdTable.left = new FormAttachment(0, margin);
    fdTable.right = new FormAttachment(99, 0);
    fdTable.top = new FormAttachment(wSearch, margin);
    fdTable.height = 300;
    wRepoTable.setLayoutData(fdTable);

    // Single click selects; double click selects and closes
    wRepoTable.addListener(SWT.Selection, e -> wOk.setEnabled(wRepoTable.getSelectionCount() > 0));
    wRepoTable.addListener(SWT.DefaultSelection, e -> ok());

    // --- Load more button ---
    wLoadMore = new Button(comp, SWT.PUSH);
    wLoadMore.setText(BaseMessages.getString(PKG, "SelectRepositoryDialog.Button.LoadMore"));
    wLoadMore.setEnabled(false);
    FormData fdLoadMore = new FormData();
    fdLoadMore.left = new FormAttachment(middle, margin);
    fdLoadMore.top = new FormAttachment(wRepoTable, margin);
    wLoadMore.setLayoutData(fdLoadMore);
    wLoadMore.addListener(SWT.Selection, e -> loadRepos(false));
    PropsUi.setLook(wLoadMore);

    // --- Initial data ---
    selectProvider(initialProvider);
    wToken.setText(initialToken);

    shell.open();

    while (!shell.isDisposed()) {
      if (!shell.getDisplay().readAndDispatch()) {
        shell.getDisplay().sleep();
      }
    }
    return selectedCloneUrl;
  }

  // ---------------------------------------------------------------------------
  // UI helpers
  // ---------------------------------------------------------------------------

  private void selectProvider(GitRepoProvider provider) {
    GitRepoProvider[] values = GitRepoProvider.values();
    for (int i = 0; i < values.length; i++) {
      if (values[i] == provider) {
        wProvider.select(i);
        break;
      }
    }
    updateProviderState();
  }

  private GitRepoProvider selectedProvider() {
    int idx = wProvider.getSelectionIndex();
    if (idx < 0) {
      return GitRepoProvider.GITHUB_CLOUD;
    }
    return GitRepoProvider.values()[idx];
  }

  private void updateProviderState() {
    GitRepoProvider provider = selectedProvider();
    boolean isBitbucket = provider == GitRepoProvider.BITBUCKET;
    boolean needsHost = provider.isSupportsCustomHost();

    // Host field: shown for GHE and GitLab (self-hosted)
    wlHost.setVisible(needsHost);
    wHost.setVisible(needsHost);

    // Token vs username+password
    wlToken.setVisible(!isBitbucket);
    wToken.setVisible(!isBitbucket);
    wlUsername.setVisible(isBitbucket);
    wUsername.setVisible(isBitbucket);
    wlPassword.setVisible(isBitbucket);
    wPassword.setVisible(isBitbucket);

    // Update host placeholder text
    if (provider == GitRepoProvider.GITHUB_ENTERPRISE) {
      wHost.setMessage("https://git.mycorp.com");
    } else if (provider == GitRepoProvider.GITLAB) {
      wHost.setMessage("https://gitlab.com (leave blank for GitLab.com)");
    }

    shell.layout(true, true);
  }

  // ---------------------------------------------------------------------------
  // Repository loading
  // ---------------------------------------------------------------------------

  private void loadRepos(boolean resetPage) {
    GitRepoProvider provider = selectedProvider();

    GitRepoAuth auth;
    if (provider == GitRepoProvider.BITBUCKET) {
      String username = wUsername.getText().trim();
      String password = wPassword.getText().trim();
      if (username.isEmpty() || password.isEmpty()) {
        wStatus.setText(
            BaseMessages.getString(PKG, "SelectRepositoryDialog.Status.NeedUsernamePassword"));
        return;
      }
      auth = GitRepoAuth.forBasic(username, password);
    } else {
      String token = wToken.getText().trim();
      if (token.isEmpty()) {
        wStatus.setText(BaseMessages.getString(PKG, "SelectRepositoryDialog.Status.NeedToken"));
        return;
      }
      auth = GitRepoAuth.forToken(token);
    }

    String apiBaseUrl = resolveApiBaseUrl(provider);
    String filter = wFilter.getText().trim();

    if (resetPage) {
      currentPage = 1;
      loadedRepos.clear();
      wRepoTable.removeAll();
    }

    wStatus.setText(BaseMessages.getString(PKG, "SelectRepositoryDialog.Status.Loading"));
    shell.setCursor(shell.getDisplay().getSystemCursor(SWT.CURSOR_WAIT));

    final int pageToLoad = currentPage;
    final GitRepoAuth finalAuth = auth;
    final String finalApiBaseUrl = apiBaseUrl;
    final String finalFilter = filter;

    // Run the network call in a background thread and update UI on the SWT thread.
    Thread worker =
        new Thread(
            () -> {
              GitRepositoryPage result = null;
              Exception error = null;
              try {
                GitRepositoryBrowser browser = GitRepositoryBrowser.forProvider(provider);
                result =
                    browser.listRepositories(finalApiBaseUrl, finalAuth, finalFilter, pageToLoad);
              } catch (Exception e) {
                error = e;
              }

              final GitRepositoryPage finalResult = result;
              final Exception finalError = error;

              shell
                  .getDisplay()
                  .asyncExec(
                      () -> {
                        if (shell.isDisposed()) {
                          return;
                        }
                        shell.setCursor(null);
                        if (finalError != null) {
                          wStatus.setText(
                              BaseMessages.getString(PKG, "SelectRepositoryDialog.Status.Error"));
                          new ErrorDialog(
                              shell,
                              BaseMessages.getString(PKG, "SelectRepositoryDialog.Error.Header"),
                              BaseMessages.getString(PKG, "SelectRepositoryDialog.Error.Message"),
                              finalError);
                          return;
                        }
                        if (finalResult != null) {
                          loadedRepos.addAll(finalResult.getRepositories());
                          hasMore = finalResult.isHasMore();
                          currentPage = pageToLoad + 1;
                          populateTable(loadedRepos);
                          wLoadMore.setEnabled(hasMore);
                          wStatus.setText(
                              String.format(
                                  BaseMessages.getString(
                                      PKG, "SelectRepositoryDialog.Status.Loaded"),
                                  loadedRepos.size()));
                        }
                      });
            });
    worker.setDaemon(true);
    worker.start();
  }

  private String resolveApiBaseUrl(GitRepoProvider provider) {
    String hostValue = wHost.getText().trim();
    return switch (provider) {
      case GITHUB_ENTERPRISE -> {
        if (!hostValue.isEmpty()) {
          String base = StringUtil.trimEnd(hostValue, '/');
          yield base + "/api/v3";
        }
        yield null;
      }
      case GITLAB -> {
        if (!hostValue.isEmpty()) {
          String base = StringUtil.trimEnd(hostValue, '/');
          yield base + "/api/v4";
        }
        yield null; // default to gitlab.com
      }
      default -> null; // implementations use their own defaults
    };
  }

  private void populateTable(List<GitRepositoryInfo> repos) {
    wRepoTable.removeAll();
    for (GitRepositoryInfo repo : repos) {
      TableItem item = new TableItem(wRepoTable, SWT.NONE);
      item.setText(COL_NAME, repo.getName());
      item.setText(COL_OWNER, repo.getOwner());
      item.setText(
          COL_VISIBILITY,
          repo.isPrivateRepo()
              ? BaseMessages.getString(PKG, "SelectRepositoryDialog.Visibility.Private")
              : BaseMessages.getString(PKG, "SelectRepositoryDialog.Visibility.Public"));
      item.setText(COL_UPDATED, repo.getLastUpdatedShort());
      item.setData(repo);
    }
  }

  // ---------------------------------------------------------------------------
  // OK / Cancel
  // ---------------------------------------------------------------------------

  private void ok() {
    int idx = wRepoTable.getSelectionIndex();
    if (idx < 0) {
      return;
    }
    TableItem item = wRepoTable.getItem(idx);
    GitRepositoryInfo repo = (GitRepositoryInfo) item.getData();
    if (repo == null) {
      return;
    }
    selectedCloneUrl = repo.getHttpsCloneUrl();
    selectedRepoName = repo.getName();
    dispose();
  }

  private void cancel() {
    selectedCloneUrl = null;
    selectedRepoName = null;
    dispose();
  }

  private void dispose() {
    props.setScreen(new WindowProperty(shell));
    shell.dispose();
  }
}
