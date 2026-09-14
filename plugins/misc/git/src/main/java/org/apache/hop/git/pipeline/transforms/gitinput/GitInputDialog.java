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

package org.apache.hop.git.pipeline.transforms.gitinput;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.git.provider.GitConnection;
import org.apache.hop.git.provider.GitOrganizationInfo;
import org.apache.hop.git.provider.GitRepositoryBrowser;
import org.apache.hop.git.provider.GitRepositoryInfo;
import org.apache.hop.git.provider.GitResourceType;
import org.apache.hop.git.provider.LocalGitResourceClient;
import org.apache.hop.git.provider.SelectGitListDialog;
import org.apache.hop.git.provider.SelectGitRepositoryDialog;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.IGuiPluginCompositeButtonsListener;
import org.apache.hop.ui.core.gui.IGuiPluginCompositeWidgetsListener;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

/**
 * Transform dialog for Git Input.
 *
 * <p>The form itself is declared with {@code @GuiWidgetElement} annotations on {@link GitInputMeta}
 * and built by {@link GuiCompositeWidgets}. What is left here is the behaviour annotations cannot
 * express: the owner, repository and branch lists are fetched from the provider API on a background
 * thread and pushed into their combos, and the two Browse buttons open a filtered list dialog.
 */
public class GitInputDialog extends BaseTransformDialog {

  private static final Class<?> PKG = GitInputMeta.class;

  private final GitInputMeta input;
  private GuiCompositeWidgets widgets;

  private final List<GitOrganizationInfo> organizations = new ArrayList<>();
  private final List<String> repositoryNames = new ArrayList<>();
  private final List<String> branchNames = new ArrayList<>();

  /**
   * Incremented for every load that is started. A background result whose generation is stale is
   * dropped, so a slow first request cannot overwrite the list of a later, faster one.
   */
  private int loadGeneration;

  /**
   * Set while this dialog is writing widget values itself.
   *
   * <p>{@link GuiCompositeWidgets} fires its modified callback on SWT.Modify as well as on
   * SWT.Selection, so clearing a dependent field from inside the handler calls the handler again.
   * Without this guard one connection change cascades into repeated provider calls.
   */
  private boolean applyingWidgetValues;

  /** Pending debounced load, so typing does not fire one request per keystroke. */
  private Runnable pendingLoad;

  private static final int LOAD_DEBOUNCE_MS = 500;

  public GitInputDialog(
      Shell parent, IVariables variables, GitInputMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "GitInputDialog.Shell.Title"));

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    changed = input.hasChanged();

    widgets =
        GuiCompositeWidgets.addScrolledComposite(
            shell,
            variables,
            wTransformName,
            wOk,
            GitInputMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
            input);
    widgets.setCompositeButtonsListener(
        new IGuiPluginCompositeButtonsListener() {
          @Override
          public void buttonPressed(Object sourceObject) {
            // Nothing to do: the meta records which button this was.
          }

          @Override
          public void afterButtonPressed(Object sourceObject) {
            // refreshWidgetsAfterButton re-binds every widget from the meta between here and the
            // modified callback. Those writes are not user edits, and letting them drive the
            // cascade cleared the repository and branch fields before Browse could read them.
            applyingWidgetValues = true;
          }
        });
    widgets.setWidgetsListener(
        new IGuiPluginCompositeWidgetsListener() {
          @Override
          public void widgetsCreated(GuiCompositeWidgets compositeWidgets) {
            // Nothing to add after creation: every widget is annotated.
          }

          @Override
          public void widgetsPopulated(GuiCompositeWidgets compositeWidgets) {
            // Never called: GuiCompositeWidgets does not invoke this callback. The dialog sets its
            // initial state directly in open() instead.
          }

          @Override
          public void widgetModified(
              GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId) {
            input.setChanged();
            onWidgetModified(widgetId);
          }

          @Override
          public void persistContents(GuiCompositeWidgets compositeWidgets) {
            // Contents are persisted when the dialog is closed with OK.
          }
        });

    refreshResourceTypeItems();
    updateCascadeState();
    loadForCurrentSettings();
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return transformName;
  }

  /**
   * Fills the dropdowns for the settings the dialog opened with.
   *
   * <p>Only a change to the connection used to trigger a load, so a transform reopened with a
   * connection already chosen showed empty owner and repository lists until the user re-picked the
   * same connection. Each step chains into the next so an owner and repository already stored are
   * loaded too, and all of it is non-interactive: opening a dialog must not raise an error dialog
   * because a stored token has since expired.
   */
  private void loadForCurrentSettings() {
    if (isLocalSource()) {
      if (!readWidgetText(GitInputMeta.WIDGET_LOCAL_REPOSITORY_PATH).trim().isEmpty()) {
        loadLocalBranches(false);
      }
      return;
    }
    if (resolveConnection() == null) {
      return;
    }
    loadOrganizations(
        false,
        () -> {
          if (resolveOwnerSlug(readWidgetText(GitInputMeta.WIDGET_OWNER)).isEmpty()) {
            return;
          }
          loadRepositories(
              false,
              () -> {
                if (!readWidgetText(GitInputMeta.WIDGET_REPOSITORY).trim().isEmpty()) {
                  loadBranches(false);
                }
              });
        });
  }

  private void onWidgetModified(String widgetId) {
    boolean browseButton =
        GitInputMeta.WIDGET_BROWSE_REPOSITORY.equals(widgetId)
            || GitInputMeta.WIDGET_BROWSE_BRANCH.equals(widgetId);
    if (browseButton) {
      // The re-bind is finished by the time the button's own notification arrives.
      applyingWidgetValues = false;
    } else if (applyingWidgetValues) {
      return;
    }
    switch (widgetId) {
      case GitInputMeta.WIDGET_SOURCE -> {
        refreshResourceTypeItems();
        updateCascadeState();
      }
      case GitInputMeta.WIDGET_CONNECTION -> {
        organizations.clear();
        repositoryNames.clear();
        branchNames.clear();
        writeWidgetText(GitInputMeta.WIDGET_OWNER, "");
        writeWidgetText(GitInputMeta.WIDGET_REPOSITORY, "");
        writeWidgetText(GitInputMeta.WIDGET_BRANCH, "");
        setComboItems(GitInputMeta.WIDGET_OWNER, List.of());
        setComboItems(GitInputMeta.WIDGET_REPOSITORY, List.of());
        setComboItems(GitInputMeta.WIDGET_BRANCH, List.of());
        updateCascadeState();
        debounce(this::loadOrganizations);
      }
      case GitInputMeta.WIDGET_LOCAL_REPOSITORY_PATH -> {
        branchNames.clear();
        updateCascadeState();
        debounce(this::loadLocalBranches);
      }
      case GitInputMeta.WIDGET_OWNER -> {
        repositoryNames.clear();
        branchNames.clear();
        writeWidgetText(GitInputMeta.WIDGET_REPOSITORY, "");
        writeWidgetText(GitInputMeta.WIDGET_BRANCH, "");
        setComboItems(GitInputMeta.WIDGET_REPOSITORY, List.of());
        setComboItems(GitInputMeta.WIDGET_BRANCH, List.of());
        updateCascadeState();
        debounce(this::loadRepositories);
      }
      case GitInputMeta.WIDGET_REPOSITORY -> {
        branchNames.clear();
        writeWidgetText(GitInputMeta.WIDGET_BRANCH, "");
        setComboItems(GitInputMeta.WIDGET_BRANCH, List.of());
        updateCascadeState();
        debounce(this::loadBranches);
      }
      case GitInputMeta.WIDGET_BROWSE_REPOSITORY -> {
        if (consumeBrowsePress(GitInputMeta.WIDGET_BROWSE_REPOSITORY)) {
          browseRepositories();
        }
      }
      case GitInputMeta.WIDGET_BROWSE_BRANCH -> {
        if (consumeBrowsePress(GitInputMeta.WIDGET_BROWSE_BRANCH)) {
          browseBranches();
        }
      }
      default -> updateCascadeState();
    }
  }

  /**
   * Whether this notification is the real button press rather than the duplicate.
   *
   * <p>The widget framework reports one button press twice; the annotated method on the meta runs
   * once and leaves the marker this consumes.
   */
  private boolean consumeBrowsePress(String widgetId) {
    if (widgetId.equals(input.getPendingBrowse())) {
      input.setPendingBrowse(null);
      return true;
    }
    return false;
  }

  // ---------------------------------------------------------------- widget access

  private String readWidgetText(String widgetId) {
    Control control = widgets == null ? null : widgets.getWidgetsMap().get(widgetId);
    if (control instanceof TextVar textVar) {
      return textVar.getText();
    } else if (control instanceof ComboVar comboVar) {
      return comboVar.getText();
    } else if (control instanceof MetaSelectionLine<?> line) {
      return line.getText();
    } else if (control instanceof Combo combo) {
      return combo.getText();
    } else if (control instanceof Text text) {
      return text.getText();
    }
    return "";
  }

  /**
   * Runs {@code load} once the user has stopped changing the field for a moment.
   *
   * <p>The widget callback fires per keystroke, and each of these loads is an API call. Without
   * this, typing a repository name issues one request per character.
   */
  private void debounce(Runnable load) {
    if (shell == null || shell.isDisposed()) {
      return;
    }
    if (pendingLoad != null) {
      shell.getDisplay().timerExec(-1, pendingLoad);
    }
    pendingLoad =
        () -> {
          pendingLoad = null;
          if (!shell.isDisposed()) {
            load.run();
          }
        };
    shell.getDisplay().timerExec(LOAD_DEBOUNCE_MS, pendingLoad);
  }

  /** Applies widget changes this dialog makes itself, without re-entering the modified handler. */
  private void applying(Runnable changes) {
    boolean previous = applyingWidgetValues;
    applyingWidgetValues = true;
    try {
      changes.run();
    } finally {
      applyingWidgetValues = previous;
    }
  }

  private void writeWidgetText(String widgetId, String value) {
    Control control = widgets == null ? null : widgets.getWidgetsMap().get(widgetId);
    String text = Const.NVL(value, "");
    boolean previous = applyingWidgetValues;
    applyingWidgetValues = true;
    try {
      if (control instanceof TextVar textVar) {
        textVar.setText(text);
      } else if (control instanceof ComboVar comboVar) {
        comboVar.setText(text);
      } else if (control instanceof MetaSelectionLine<?> line) {
        line.setText(text);
      } else if (control instanceof Combo combo) {
        combo.setText(text);
      } else if (control instanceof Text widget) {
        widget.setText(text);
      }
    } finally {
      applyingWidgetValues = previous;
    }
  }

  private void setComboItems(String widgetId, List<String> items) {
    if (widgets == null) {
      return;
    }
    // Setting items on a ComboVar clears its text, so put the current value back afterwards.
    String current = readWidgetText(widgetId);
    applying(() -> widgets.setComboValues(widgetId, items.toArray(new String[0])));
    if (!current.isEmpty()) {
      writeWidgetText(widgetId, current);
    }
  }

  private void setWidgetEnabled(String widgetId, boolean enabled) {
    Control control = widgets == null ? null : widgets.getWidgetsMap().get(widgetId);
    if (control != null && !control.isDisposed()) {
      control.setEnabled(enabled);
    }
  }

  private boolean isLocalSource() {
    return GitInputSource.LOCAL.name().equals(readWidgetText(GitInputMeta.WIDGET_SOURCE).trim());
  }

  // ---------------------------------------------------------------- form state

  private void refreshResourceTypeItems() {
    boolean isLocal = isLocalSource();
    String[] items = isLocal ? GitResourceType.localLabels() : GitResourceType.remoteLabels();
    String current = readWidgetText(GitInputMeta.WIDGET_RESOURCE_TYPE).trim();
    widgets.setComboValues(GitInputMeta.WIDGET_RESOURCE_TYPE, items);

    boolean valid = false;
    for (String item : items) {
      if (item.equals(current)) {
        valid = true;
        break;
      }
    }
    writeWidgetText(
        GitInputMeta.WIDGET_RESOURCE_TYPE, valid ? current : GitResourceType.COMMITS.name());
  }

  private void updateCascadeState() {
    boolean isLocal = isLocalSource();
    boolean hasConnection = !isLocal && resolveConnection() != null;
    boolean hasLocalRepository =
        isLocal && !readWidgetText(GitInputMeta.WIDGET_LOCAL_REPOSITORY_PATH).trim().isEmpty();
    boolean hasOwner =
        hasConnection && !resolveOwnerSlug(readWidgetText(GitInputMeta.WIDGET_OWNER)).isEmpty();
    boolean hasRepository =
        hasOwner && !readWidgetText(GitInputMeta.WIDGET_REPOSITORY).trim().isEmpty();
    boolean hasBranchSource = hasRepository || hasLocalRepository;

    setWidgetEnabled(GitInputMeta.WIDGET_CONNECTION, !isLocal);
    setWidgetEnabled(GitInputMeta.WIDGET_LOCAL_REPOSITORY_PATH, isLocal);
    setWidgetEnabled(GitInputMeta.WIDGET_OWNER, hasConnection);
    setWidgetEnabled(GitInputMeta.WIDGET_REPOSITORY, hasOwner);
    setWidgetEnabled(GitInputMeta.WIDGET_BROWSE_REPOSITORY, hasOwner);
    setWidgetEnabled(GitInputMeta.WIDGET_STATE, !isLocal);
    setWidgetEnabled(GitInputMeta.WIDGET_MAX_PAGES, !isLocal);
    setWidgetEnabled(GitInputMeta.WIDGET_BRANCH, hasBranchSource);
    setWidgetEnabled(GitInputMeta.WIDGET_BROWSE_BRANCH, hasBranchSource);
  }

  // ---------------------------------------------------------------- browse buttons

  /**
   * Opens the provider repository browser, which lists the organizations, groups or workspaces the
   * connection can see and pages through their repositories. It sets both the owner and the
   * repository, so a user who does not know the exact slug never has to type one.
   */
  private void browseRepositories() {
    GitConnection connection = resolveConnection();
    if (connection == null) {
      return;
    }
    SelectGitRepositoryDialog dialog =
        new SelectGitRepositoryDialog(
            shell,
            variables,
            connection,
            resolveOwnerSlug(readWidgetText(GitInputMeta.WIDGET_OWNER)),
            readWidgetText(GitInputMeta.WIDGET_REPOSITORY),
            SelectGitRepositoryDialog.SelectionMode.REPOSITORY);
    if (!dialog.open()) {
      return;
    }

    writeWidgetText(GitInputMeta.WIDGET_OWNER, labelForOwnerSlug(dialog.getSelectedOwner()));
    writeWidgetText(GitInputMeta.WIDGET_REPOSITORY, dialog.getSelectedRepository());
    branchNames.clear();
    writeWidgetText(GitInputMeta.WIDGET_BRANCH, "");
    setComboItems(GitInputMeta.WIDGET_BRANCH, List.of());
    updateCascadeState();
    loadBranches();
  }

  private void browseBranches() {
    if (branchNames.isEmpty()) {
      // Nothing cached: load now and open the picker when it lands, so the first click is not a
      // no-op that the user has to repeat.
      loadBranchesOrLocal(true, this::openBranchPicker, GitRepositoryBrowser.DROPDOWN_MAX_PAGES);
      return;
    }
    openBranchPicker();
  }

  private void openBranchPicker() {
    if (branchNames.isEmpty()) {
      return;
    }
    SelectGitListDialog dialog =
        new SelectGitListDialog(
            shell,
            BaseMessages.getString(PKG, "GitInputDialog.Browse.Branches.Title"),
            branchNames,
            readWidgetText(GitInputMeta.WIDGET_BRANCH));
    if (dialog.open()) {
      writeWidgetText(GitInputMeta.WIDGET_BRANCH, dialog.getSelected());
    }
  }

  // ---------------------------------------------------------------- background loading

  private void loadBranchesOrLocal(boolean interactive) {
    loadBranchesOrLocal(interactive, null);
  }

  private void loadBranchesOrLocal(boolean interactive, Runnable onLoaded) {
    loadBranchesOrLocal(interactive, onLoaded, GitRepositoryBrowser.COMBO_PRELOAD_PAGES);
  }

  private void loadBranchesOrLocal(boolean interactive, Runnable onLoaded, int maxPages) {
    if (isLocalSource()) {
      loadLocalBranches(interactive, onLoaded);
    } else {
      loadBranches(interactive, onLoaded, maxPages);
    }
  }

  private void loadLocalBranches() {
    loadLocalBranches(false);
  }

  private void loadLocalBranches(boolean interactive) {
    loadLocalBranches(interactive, null);
  }

  private void loadLocalBranches(boolean interactive, Runnable onLoaded) {
    String repositoryPath =
        variables.resolve(readWidgetText(GitInputMeta.WIDGET_LOCAL_REPOSITORY_PATH).trim());
    if (StringUtils.isEmpty(repositoryPath)) {
      updateCascadeState();
      return;
    }

    final int generation = ++loadGeneration;
    shell.setCursor(shell.getDisplay().getSystemCursor(SWT.CURSOR_WAIT));
    runInBackground(
        () -> LocalGitResourceClient.listBranches(repositoryPath),
        branches -> {
          if (generation != loadGeneration) {
            return;
          }
          branchNames.clear();
          branchNames.addAll(branches);
          setComboItems(GitInputMeta.WIDGET_BRANCH, branchNames);
          updateCascadeState();
          if (onLoaded != null) {
            onLoaded.run();
          }
        },
        BaseMessages.getString(PKG, "GitInputDialog.Load.Branches.Message"),
        interactive);
  }

  private void loadOrganizations() {
    loadOrganizations(false);
  }

  private void loadOrganizations(boolean interactive) {
    loadOrganizations(interactive, null);
  }

  private void loadOrganizations(boolean interactive, Runnable onLoaded) {
    GitConnection connection = resolveConnection();
    if (connection == null) {
      updateCascadeState();
      return;
    }

    final int generation = ++loadGeneration;
    final String previousOwner = resolveOwnerSlug(readWidgetText(GitInputMeta.WIDGET_OWNER));

    shell.setCursor(shell.getDisplay().getSystemCursor(SWT.CURSOR_WAIT));
    runInBackground(
        () -> {
          GitRepositoryBrowser browser =
              GitRepositoryBrowser.forProvider(connection.getGitProvider());
          return browser.listOrganizations(
              connection.getResolvedApiBaseUrl(variables), connection.toAuth(variables));
        },
        orgs -> {
          if (generation != loadGeneration) {
            return;
          }
          organizations.clear();
          organizations.addAll(orgs);

          List<String> labels = new ArrayList<>();
          for (GitOrganizationInfo org : organizations) {
            labels.add(formatOrganizationLabel(org));
          }
          setComboItems(GitInputMeta.WIDGET_OWNER, labels);

          String labelForOwner = labelForOwnerSlug(previousOwner);
          if (!labelForOwner.isBlank()) {
            writeWidgetText(GitInputMeta.WIDGET_OWNER, labelForOwner);
          }
          updateCascadeState();
          if (onLoaded != null) {
            onLoaded.run();
          }
        },
        BaseMessages.getString(PKG, "GitInputDialog.Load.Organizations.Message"),
        interactive);
  }

  private void loadRepositories() {
    loadRepositories(false);
  }

  private void loadRepositories(boolean interactive) {
    loadRepositories(interactive, null);
  }

  private void loadRepositories(boolean interactive, Runnable onLoaded) {
    loadRepositories(interactive, onLoaded, GitRepositoryBrowser.COMBO_PRELOAD_PAGES);
  }

  private void loadRepositories(boolean interactive, Runnable onLoaded, int maxPages) {
    GitConnection connection = resolveConnection();
    if (connection == null) {
      updateCascadeState();
      return;
    }

    String ownerSlug = resolveOwnerSlug(readWidgetText(GitInputMeta.WIDGET_OWNER));
    if (StringUtils.isEmpty(ownerSlug)) {
      updateCascadeState();
      return;
    }

    GitOrganizationInfo organization = findOrganization(ownerSlug);
    if (organization == null) {
      organization = new GitOrganizationInfo(ownerSlug, ownerSlug, true);
    }

    final int generation = ++loadGeneration;
    final GitOrganizationInfo orgForLoad = organization;

    shell.setCursor(shell.getDisplay().getSystemCursor(SWT.CURSOR_WAIT));
    runInBackground(
        () -> {
          GitRepositoryBrowser browser =
              GitRepositoryBrowser.forProvider(connection.getGitProvider());
          return browser.listRepositoriesForDropdown(
              connection.getResolvedApiBaseUrl(variables),
              connection.toAuth(variables),
              orgForLoad,
              null,
              maxPages);
        },
        repos -> {
          if (generation != loadGeneration) {
            return;
          }
          repositoryNames.clear();
          for (GitRepositoryInfo repo : repos) {
            repositoryNames.add(repo.getName());
          }
          setComboItems(GitInputMeta.WIDGET_REPOSITORY, repositoryNames);
          updateCascadeState();
          if (onLoaded != null) {
            onLoaded.run();
          }
        },
        BaseMessages.getString(PKG, "GitInputDialog.Load.Repositories.Message"),
        interactive);
  }

  private void loadBranches() {
    loadBranches(false);
  }

  private void loadBranches(boolean interactive) {
    loadBranches(interactive, null);
  }

  private void loadBranches(boolean interactive, Runnable onLoaded) {
    loadBranches(interactive, onLoaded, GitRepositoryBrowser.COMBO_PRELOAD_PAGES);
  }

  private void loadBranches(boolean interactive, Runnable onLoaded, int maxPages) {
    GitConnection connection = resolveConnection();
    if (connection == null) {
      return;
    }

    String ownerSlug = resolveOwnerSlug(readWidgetText(GitInputMeta.WIDGET_OWNER));
    String repository = readWidgetText(GitInputMeta.WIDGET_REPOSITORY).trim();
    if (StringUtils.isEmpty(ownerSlug) || StringUtils.isEmpty(repository)) {
      return;
    }

    final int generation = ++loadGeneration;
    shell.setCursor(shell.getDisplay().getSystemCursor(SWT.CURSOR_WAIT));
    runInBackground(
        () -> {
          GitRepositoryBrowser browser =
              GitRepositoryBrowser.forProvider(connection.getGitProvider());
          return browser.listBranches(
              connection.getResolvedApiBaseUrl(variables),
              connection.toAuth(variables),
              ownerSlug,
              repository,
              maxPages);
        },
        branches -> {
          if (generation != loadGeneration) {
            return;
          }
          branchNames.clear();
          branchNames.addAll(branches);
          setComboItems(GitInputMeta.WIDGET_BRANCH, branchNames);
          updateCascadeState();
          if (onLoaded != null) {
            onLoaded.run();
          }
        },
        BaseMessages.getString(PKG, "GitInputDialog.Load.Branches.Message"),
        interactive);
  }

  @FunctionalInterface
  private interface BackgroundTask<T> {
    T run() throws Exception;
  }

  @FunctionalInterface
  private interface BackgroundResult<T> {
    void accept(T value);
  }

  /**
   * Runs a load the user did not explicitly ask for. A failure is logged rather than shown: these
   * fire while the user is still filling the form, and an unusable connection would otherwise put a
   * modal error in front of them on every edit, with no way to finish the dialog.
   */
  private <T> void runInBackground(
      BackgroundTask<T> task, BackgroundResult<T> onSuccess, String errorMessage) {
    runInBackground(task, onSuccess, errorMessage, false);
  }

  private <T> void runInBackground(
      BackgroundTask<T> task,
      BackgroundResult<T> onSuccess,
      String errorMessage,
      boolean interactive) {
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
              // Closing the dialog while a load is in flight disposes the shell; reading its
              // display from this thread then throws SWTException.
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
                      if (interactive) {
                        new ErrorDialog(
                            shell,
                            BaseMessages.getString(PKG, "GitInputDialog.Load.Error.Title"),
                            errorMessage,
                            finalError);
                      } else {
                        LogChannel.UI.logBasic(errorMessage + " " + finalError.getMessage());
                      }
                      return;
                    }
                    onSuccess.accept(finalResult);
                  });
            });
    worker.setDaemon(true);
    worker.start();
  }

  // ---------------------------------------------------------------- owner label handling

  private GitConnection resolveConnection() {
    String connectionName = variables.resolve(readWidgetText(GitInputMeta.WIDGET_CONNECTION));
    if (StringUtils.isEmpty(connectionName)) {
      return null;
    }
    try {
      IHopMetadataSerializer<GitConnection> serializer =
          metadataProvider.getSerializer(GitConnection.class);
      if (!serializer.exists(connectionName)) {
        return null;
      }
      return serializer.load(connectionName);
    } catch (HopException e) {
      return null;
    }
  }

  private GitOrganizationInfo findOrganization(String ownerSlug) {
    for (GitOrganizationInfo org : organizations) {
      if (ownerSlug.equals(org.getSlug())) {
        return org;
      }
    }
    return null;
  }

  private String formatOrganizationLabel(GitOrganizationInfo org) {
    if (org.getDisplayName().equals(org.getSlug())) {
      return org.getSlug();
    }
    return org.getDisplayName() + " (" + org.getSlug() + ")";
  }

  private String labelForOwnerSlug(String ownerSlug) {
    if (StringUtils.isEmpty(ownerSlug)) {
      return "";
    }
    GitOrganizationInfo org = findOrganization(ownerSlug);
    if (org != null) {
      return formatOrganizationLabel(org);
    }
    return ownerSlug;
  }

  /**
   * The owner combo shows "Display name (slug)" when a provider reports both, but only the slug is
   * ever stored and sent to the API.
   */
  private String resolveOwnerSlug(String text) {
    if (text == null) {
      return "";
    }
    String trimmed = text.trim();
    for (GitOrganizationInfo org : organizations) {
      if (trimmed.equals(org.getSlug()) || trimmed.equals(formatOrganizationLabel(org))) {
        return org.getSlug();
      }
    }
    int open = trimmed.lastIndexOf('(');
    int close = trimmed.lastIndexOf(')');
    if (open >= 0 && close > open) {
      return trimmed.substring(open + 1, close).trim();
    }
    return trimmed;
  }

  // ---------------------------------------------------------------- dialog result

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  private void ok() {
    if (StringUtils.isEmpty(wTransformName.getText())) {
      return;
    }

    widgets.getWidgetsContents(input, GitInputMeta.GUI_PLUGIN_ELEMENT_PARENT_ID);

    // The combo carries a display label; store the slug the API expects.
    input.setOwner(resolveOwnerSlug(input.getOwner()));
    input.setRepository(StringUtils.trimToEmpty(input.getRepository()));
    input.setBranch(StringUtils.trimToEmpty(input.getBranch()));
    input.setLocalRepositoryPath(StringUtils.trimToEmpty(input.getLocalRepositoryPath()));

    transformName = wTransformName.getText();
    input.setChanged();
    dispose();
  }
}
