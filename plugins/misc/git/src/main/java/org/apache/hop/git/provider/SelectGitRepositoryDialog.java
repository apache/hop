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

package org.apache.hop.git.provider;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
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
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

/** Browse organizations and repositories using a configured {@link GitConnection}. */
public class SelectGitRepositoryDialog extends Dialog {

  private static final Class<?> PKG = SelectGitRepositoryDialog.class;

  public enum SelectionMode {
    ORGANIZATION,
    REPOSITORY
  }

  private static final int COL_NAME = 0;
  private static final int COL_VISIBILITY = 1;
  private static final int COL_UPDATED = 2;

  private final PropsUi props;
  private final IVariables variables;
  private final GitConnection connection;
  private final String initialOwner;
  private final String initialRepository;
  private final SelectionMode selectionMode;

  private Shell shell;
  private Button wOk;
  private Combo wOrganization;
  private Text wFilter;
  private Table wRepoTable;
  private Button wLoadMore;
  private Label wStatus;

  private final List<GitOrganizationInfo> organizations = new ArrayList<>();
  private final List<GitRepositoryInfo> loadedRepos = new ArrayList<>();
  private int currentPage = 1;
  private boolean hasMore;

  /**
   * Incremented per load. A result from a superseded load is dropped, so two overlapping requests
   * cannot append to the same list or fight over the page counter.
   */
  private int loadGeneration;

  private String selectedOwner;
  private String selectedRepository;
  private boolean confirmed;

  public SelectGitRepositoryDialog(
      Shell parent,
      IVariables variables,
      GitConnection connection,
      String initialOwner,
      String initialRepository,
      SelectionMode selectionMode) {
    super(parent, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.RESIZE);
    this.variables = variables;
    this.connection = connection;
    this.initialOwner = initialOwner;
    this.initialRepository = initialRepository;
    this.selectionMode = selectionMode != null ? selectionMode : SelectionMode.REPOSITORY;
    this.props = PropsUi.getInstance();
  }

  public boolean open() {
    Shell parent = getParent();
    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.RESIZE);
    shell.setText(
        selectionMode == SelectionMode.ORGANIZATION
            ? BaseMessages.getString(PKG, "SelectGitRepositoryDialog.Shell.Title.Organization")
            : BaseMessages.getString(PKG, "SelectGitRepositoryDialog.Shell.Title"));
    shell.setImage(
        GuiResource.getInstance()
            .getImage(
                "git.svg", PKG.getClassLoader(), ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE));

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();
    shell.setLayout(formLayout);
    shell.setMinimumSize(720, 560);
    PropsUi.setLook(shell);

    int margin = PropsUi.getMargin();
    int middle = props.getMiddlePct();

    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString("System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wOk.setEnabled(false);

    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString("System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());

    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wOk, wCancel}, margin * 3, null);

    wStatus = new Label(shell, SWT.LEFT);
    wStatus.setText("");
    PropsUi.setLook(wStatus);
    FormData fdStatus = new FormData();
    fdStatus.left = new FormAttachment(0, margin);
    fdStatus.right = new FormAttachment(100, -margin);
    fdStatus.bottom = new FormAttachment(wOk, -margin);
    wStatus.setLayoutData(fdStatus);

    Composite comp = new Composite(shell, SWT.NONE);
    comp.setLayout(new FormLayout());
    PropsUi.setLook(comp);
    FormData fdComp = new FormData();
    fdComp.left = new FormAttachment(0, 0);
    fdComp.right = new FormAttachment(100, 0);
    fdComp.top = new FormAttachment(0, 0);
    fdComp.bottom = new FormAttachment(wStatus, -margin);
    comp.setLayoutData(fdComp);

    Label wlOrganization = new Label(comp, SWT.RIGHT);
    wlOrganization.setText(
        BaseMessages.getString(PKG, "SelectGitRepositoryDialog.Organization.Label"));
    PropsUi.setLook(wlOrganization);
    FormData fdlOrganization = new FormData();
    fdlOrganization.left = new FormAttachment(0, 0);
    fdlOrganization.right = new FormAttachment(middle, -margin);
    fdlOrganization.top = new FormAttachment(0, margin * 2);
    wlOrganization.setLayoutData(fdlOrganization);

    wOrganization = new Combo(comp, SWT.DROP_DOWN | SWT.READ_ONLY);
    PropsUi.setLook(wOrganization);
    FormData fdOrganization = new FormData();
    fdOrganization.left = new FormAttachment(middle, 0);
    fdOrganization.right = new FormAttachment(100, -margin);
    fdOrganization.top = new FormAttachment(wlOrganization, 0, SWT.CENTER);
    wOrganization.setLayoutData(fdOrganization);
    wOrganization.addListener(
        SWT.Selection,
        e -> {
          updateOkButton();
          if (selectionMode == SelectionMode.REPOSITORY) {
            loadRepositories(true);
          }
        });

    Button wRefreshOrgs = new Button(comp, SWT.PUSH);
    wRefreshOrgs.setText(BaseMessages.getString(PKG, "SelectGitRepositoryDialog.Refresh.Label"));
    PropsUi.setLook(wRefreshOrgs);
    FormData fdRefreshOrgs = new FormData();
    fdRefreshOrgs.top = new FormAttachment(wOrganization, margin);
    fdRefreshOrgs.left = new FormAttachment(middle, 0);
    wRefreshOrgs.setLayoutData(fdRefreshOrgs);
    wRefreshOrgs.addListener(SWT.Selection, e -> loadOrganizations());

    Label wlFilter = new Label(comp, SWT.RIGHT);
    wlFilter.setText(BaseMessages.getString(PKG, "SelectGitRepositoryDialog.Filter.Label"));
    PropsUi.setLook(wlFilter);
    FormData fdlFilter = new FormData();
    fdlFilter.left = new FormAttachment(0, 0);
    fdlFilter.right = new FormAttachment(middle, -margin);
    fdlFilter.top = new FormAttachment(wRefreshOrgs, margin);
    wlFilter.setLayoutData(fdlFilter);

    wFilter = new Text(comp, SWT.SINGLE | SWT.BORDER);
    PropsUi.setLook(wFilter);
    FormData fdFilter = new FormData();
    fdFilter.left = new FormAttachment(middle, 0);
    fdFilter.right = new FormAttachment(100, -120, SWT.RIGHT);
    fdFilter.top = new FormAttachment(wlFilter, 0, SWT.CENTER);
    wFilter.setLayoutData(fdFilter);
    if (initialRepository != null && !initialRepository.isBlank()) {
      wFilter.setText(initialRepository);
    }

    Button wSearch = new Button(comp, SWT.PUSH);
    wSearch.setText(BaseMessages.getString(PKG, "SelectGitRepositoryDialog.Search.Label"));
    PropsUi.setLook(wSearch);
    FormData fdSearch = new FormData();
    fdSearch.left = new FormAttachment(wFilter, margin);
    fdSearch.right = new FormAttachment(100, -margin);
    fdSearch.top = new FormAttachment(wlFilter, 0, SWT.CENTER);
    wSearch.setLayoutData(fdSearch);
    wSearch.addListener(SWT.Selection, e -> loadRepositories(true));

    wRepoTable = new Table(comp, SWT.BORDER | SWT.SINGLE | SWT.FULL_SELECTION);
    wRepoTable.setHeaderVisible(true);
    wRepoTable.setLinesVisible(true);
    PropsUi.setLook(wRepoTable);
    FormData fdTable = new FormData();
    fdTable.left = new FormAttachment(0, margin);
    fdTable.right = new FormAttachment(100, -margin);
    fdTable.top = new FormAttachment(wFilter, margin * 2);
    fdTable.bottom = new FormAttachment(100, -margin * 4);
    wRepoTable.setLayoutData(fdTable);

    TableColumn colName = new TableColumn(wRepoTable, SWT.LEFT);
    colName.setText(BaseMessages.getString(PKG, "SelectGitRepositoryDialog.Table.Name"));
    colName.setWidth(220);
    TableColumn colVisibility = new TableColumn(wRepoTable, SWT.LEFT);
    colVisibility.setText(
        BaseMessages.getString(PKG, "SelectGitRepositoryDialog.Table.Visibility"));
    colVisibility.setWidth(90);
    TableColumn colUpdated = new TableColumn(wRepoTable, SWT.LEFT);
    colUpdated.setText(BaseMessages.getString(PKG, "SelectGitRepositoryDialog.Table.Updated"));
    colUpdated.setWidth(110);

    wRepoTable.addListener(SWT.Selection, e -> updateOkButton());
    wRepoTable.addListener(
        SWT.DefaultSelection,
        e -> {
          if (wRepoTable.getSelectionIndex() >= 0) {
            ok();
          }
        });

    wLoadMore = new Button(comp, SWT.PUSH);
    wLoadMore.setText(BaseMessages.getString(PKG, "SelectGitRepositoryDialog.LoadMore.Label"));
    wLoadMore.setEnabled(false);
    PropsUi.setLook(wLoadMore);
    FormData fdLoadMore = new FormData();
    fdLoadMore.left = new FormAttachment(0, margin);
    fdLoadMore.top = new FormAttachment(wRepoTable, margin);
    wLoadMore.setLayoutData(fdLoadMore);
    wLoadMore.addListener(SWT.Selection, e -> loadRepositories(false));

    if (selectionMode == SelectionMode.ORGANIZATION) {
      wFilter.setEnabled(false);
      wSearch.setEnabled(false);
      wRepoTable.setEnabled(false);
      wLoadMore.setEnabled(false);
    }

    loadOrganizations();
    shell.open();
    while (!shell.isDisposed()) {
      if (!shell.getDisplay().readAndDispatch()) {
        shell.getDisplay().sleep();
      }
    }
    return confirmed;
  }

  public String getSelectedOwner() {
    return selectedOwner;
  }

  public String getSelectedRepository() {
    return selectedRepository;
  }

  private void loadOrganizations() {
    wStatus.setText(BaseMessages.getString(PKG, "SelectGitRepositoryDialog.Status.LoadingOrgs"));
    shell.setCursor(shell.getDisplay().getSystemCursor(SWT.CURSOR_WAIT));
    wOrganization.removeAll();
    organizations.clear();

    runInBackground(
        () -> {
          GitRepositoryBrowser browser =
              GitRepositoryBrowser.forProvider(connection.getGitProvider());
          String apiBaseUrl = connection.getResolvedApiBaseUrl(variables);
          GitAuth auth = connection.toAuth(variables);
          return browser.listOrganizations(apiBaseUrl, auth);
        },
        orgs -> {
          organizations.addAll(orgs);
          for (GitOrganizationInfo org : organizations) {
            wOrganization.add(org.getDisplayName());
            wOrganization.setData(org.getDisplayName(), org);
          }
          selectInitialOrganization();
          updateOkButton();
          if (!organizations.isEmpty() && selectionMode == SelectionMode.REPOSITORY) {
            loadRepositories(true);
          } else if (!organizations.isEmpty()) {
            wStatus.setText(
                BaseMessages.getString(PKG, "SelectGitRepositoryDialog.Status.OrgReady"));
          } else {
            wStatus.setText(BaseMessages.getString(PKG, "SelectGitRepositoryDialog.Status.NoOrgs"));
          }
        });
  }

  private void selectInitialOrganization() {
    if (initialOwner == null || initialOwner.isBlank()) {
      if (!organizations.isEmpty()) {
        wOrganization.select(0);
      }
      return;
    }
    for (int i = 0; i < organizations.size(); i++) {
      if (initialOwner.equals(organizations.get(i).getSlug())) {
        wOrganization.select(i);
        return;
      }
    }
    if (!organizations.isEmpty()) {
      wOrganization.select(0);
    }
  }

  private GitOrganizationInfo selectedOrganization() {
    int idx = wOrganization.getSelectionIndex();
    if (idx < 0 || idx >= organizations.size()) {
      return null;
    }
    return organizations.get(idx);
  }

  private void loadRepositories(boolean resetPage) {
    final int generation = ++loadGeneration;
    GitOrganizationInfo organization = selectedOrganization();
    if (organization == null) {
      wStatus.setText(BaseMessages.getString(PKG, "SelectGitRepositoryDialog.Status.SelectOrg"));
      return;
    }

    if (resetPage) {
      currentPage = 1;
      loadedRepos.clear();
      wRepoTable.removeAll();
    }

    wStatus.setText(BaseMessages.getString(PKG, "SelectGitRepositoryDialog.Status.LoadingRepos"));
    shell.setCursor(shell.getDisplay().getSystemCursor(SWT.CURSOR_WAIT));
    final int pageToLoad = currentPage;
    final String filter = wFilter.getText().trim();

    runInBackground(
        () -> {
          GitRepositoryBrowser browser =
              GitRepositoryBrowser.forProvider(connection.getGitProvider());
          String apiBaseUrl = connection.getResolvedApiBaseUrl(variables);
          GitAuth auth = connection.toAuth(variables);
          return browser.listRepositories(apiBaseUrl, auth, organization, filter, pageToLoad);
        },
        page -> {
          if (generation != loadGeneration) {
            return;
          }
          loadedRepos.addAll(page.getRepositories());
          hasMore = page.isHasMore();
          currentPage = pageToLoad + 1;
          populateTable(loadedRepos);
          wLoadMore.setEnabled(hasMore);
          wStatus.setText(
              BaseMessages.getString(
                  PKG,
                  "SelectGitRepositoryDialog.Status.LoadedRepos",
                  String.valueOf(loadedRepos.size())));
          selectInitialRepository();
          updateOkButton();
        });
  }

  private void updateOkButton() {
    if (wOk == null || wOk.isDisposed()) {
      return;
    }
    if (selectionMode == SelectionMode.ORGANIZATION) {
      wOk.setEnabled(selectedOrganization() != null);
      return;
    }
    wOk.setEnabled(wRepoTable.getSelectionIndex() >= 0);
  }

  private void selectInitialRepository() {
    if (initialRepository == null || initialRepository.isBlank()) {
      return;
    }
    for (int i = 0; i < wRepoTable.getItemCount(); i++) {
      TableItem item = wRepoTable.getItem(i);
      GitRepositoryInfo repo = (GitRepositoryInfo) item.getData();
      if (repo != null && initialRepository.equals(repo.getName())) {
        wRepoTable.select(i);
        wRepoTable.showSelection();
        break;
      }
    }
  }

  private void populateTable(List<GitRepositoryInfo> repos) {
    wRepoTable.removeAll();
    for (GitRepositoryInfo repo : repos) {
      TableItem item = new TableItem(wRepoTable, SWT.NONE);
      item.setText(COL_NAME, repo.getName());
      item.setText(
          COL_VISIBILITY,
          repo.isPrivateRepo()
              ? BaseMessages.getString(PKG, "SelectGitRepositoryDialog.Visibility.Private")
              : BaseMessages.getString(PKG, "SelectGitRepositoryDialog.Visibility.Public"));
      item.setText(COL_UPDATED, repo.getLastUpdatedShort());
      item.setData(repo);
    }
  }

  private interface BackgroundTask<T> {
    T run() throws Exception;
  }

  private interface BackgroundResult<T> {
    void accept(T value);
  }

  private <T> void runInBackground(BackgroundTask<T> task, BackgroundResult<T> onSuccess) {
    Thread worker =
        new Thread(
            () -> {
              T result = null;
              Exception error = null;
              try {
                result = task.run();
              } catch (Exception e) {
                error = e;
              }
              final T finalResult = result;
              final Exception finalError = error;
              // Closing the dialog while a request is in flight disposes the shell; reading its
              // display from the worker then throws SWTException on this thread.
              if (shell.isDisposed()) {
                return;
              }
              Display display = shell.getDisplay();
              if (display.isDisposed()) {
                return;
              }
              display.asyncExec(
                  () -> {
                    if (shell.isDisposed()) {
                      return;
                    }
                    shell.setCursor(null);
                    if (finalError != null) {
                      wStatus.setText(
                          BaseMessages.getString(PKG, "SelectGitRepositoryDialog.Status.Error"));
                      new ErrorDialog(
                          shell,
                          BaseMessages.getString(PKG, "SelectGitRepositoryDialog.Error.Title"),
                          BaseMessages.getString(PKG, "SelectGitRepositoryDialog.Error.Message"),
                          finalError);
                      return;
                    }
                    onSuccess.accept(finalResult);
                  });
            });
    worker.setDaemon(true);
    worker.start();
  }

  private void ok() {
    if (selectionMode == SelectionMode.ORGANIZATION) {
      GitOrganizationInfo organization = selectedOrganization();
      if (organization == null) {
        return;
      }
      selectedOwner = organization.getSlug();
      selectedRepository = null;
      confirmed = true;
      dispose();
      return;
    }

    int idx = wRepoTable.getSelectionIndex();
    if (idx < 0) {
      return;
    }
    GitRepositoryInfo repo = (GitRepositoryInfo) wRepoTable.getItem(idx).getData();
    if (repo == null) {
      return;
    }
    selectedOwner = repo.getOwner();
    selectedRepository = repo.getName();
    confirmed = true;
    dispose();
  }

  private void cancel() {
    confirmed = false;
    dispose();
  }

  private void dispose() {
    props.setScreen(new WindowProperty(shell));
    shell.dispose();
  }
}
