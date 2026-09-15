/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ui.hopgui.notifications.config;

import java.util.UUID;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.IGuiPluginCompositeButtonsListener;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;

/**
 * Dialog for adding or editing a notification source configuration.
 *
 * <p>The form itself is declared on {@link NotificationSourceModel} and built by {@link
 * GuiCompositeWidgets}, so what is left here is the shell, the buttons, and the decisions the
 * framework cannot make: which fields the chosen source type uses, and whether what was typed is
 * worth saving.
 */
public class NotificationSourceDialog {

  private static final Class<?> PKG = NotificationSourceDialog.class;

  private final Shell shell;
  private final Shell parentShell;
  private final NotificationSourceConfig sourceConfig;
  private final NotificationSourceModel model;

  private GuiCompositeWidgets widgets;

  /**
   * Whether the dialog was closed without confirming. Only OK clears it: closing the window any
   * other way - the title bar, Escape - leaves the edits unsaved, which is what closing a dialog
   * means everywhere else.
   */
  private boolean cancelled = true;

  /** Set while one GitHub field is updating another, so the two directions do not loop. */
  private boolean syncingGithubFields;

  public NotificationSourceDialog(Shell parent, NotificationSourceConfig sourceConfig) {
    this.parentShell = parent;
    this.shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    this.sourceConfig = sourceConfig != null ? sourceConfig : new NotificationSourceConfig();
    this.model = NotificationSourceModel.fromConfig(this.sourceConfig);
    PropsUi.setLook(this.shell);
  }

  public String open() {
    Display display = parentShell.getDisplay();

    shell.setText(BaseMessages.getString(PKG, "NotificationSourceDialog.Title"));

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();
    shell.setLayout(formLayout);

    int margin = PropsUi.getMargin();

    // The buttons first: they are what the form is laid out against, so that a form too tall for
    // the window scrolls instead of pushing OK and Cancel off the bottom.
    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "NotificationSourceDialog.OK"));
    wOk.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            if (saveSource()) {
              cancelled = false;
              shell.dispose();
            }
          }
        });

    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "NotificationSourceDialog.Cancel"));
    wCancel.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            cancelled = true;
            shell.dispose();
          }
        });

    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wOk, wCancel}, margin, null);

    widgets =
        GuiCompositeWidgets.addScrolledComposite(
            shell,
            HopGui.getInstance().getVariables(),
            null,
            wOk,
            NotificationSourceModel.GUI_PLUGIN_ELEMENT_PARENT_ID,
            model);

    // The colour picker and the URL parser are annotated buttons on the model. They act on the
    // model, so it has to hold what is on screen before either of them runs.
    widgets.setCompositeButtonsListener(
        new IGuiPluginCompositeButtonsListener() {
          @Override
          public void buttonPressed(Object sourceObject) {
            readWidgets();
          }
        });

    listenForTypeChanges();
    listenForGithubEdits();
    showFieldsForSelectedType();

    // Centres on the main window and remembers where it was put, the way every other Hop dialog
    // behaves. Packing and sizing by hand left it wherever the window manager felt like.
    BaseTransformDialog.setSize(shell, 500, 400, true);

    shell.open();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }

    return cancelled ? null : sourceConfig.getId();
  }

  /** Copy what is on screen into the model. */
  private void readWidgets() {
    widgets.getWidgetsContents(model, NotificationSourceModel.GUI_PLUGIN_ELEMENT_PARENT_ID);
  }

  /** Show only the fields the chosen source type actually uses. */
  private void showFieldsForSelectedType() {
    readWidgets();
    widgets.setWidgetsHidden(model, model.widgetsToHide());
  }

  private void listenForTypeChanges() {
    Control control = widgets.getWidgetsMap().get(NotificationSourceModel.WIDGET_TYPE);
    if (control instanceof Combo combo) {
      combo.addSelectionListener(
          new SelectionAdapter() {
            @Override
            public void widgetSelected(SelectionEvent e) {
              showFieldsForSelectedType();
            }
          });
    }
  }

  /**
   * Keep the GitHub URL and the owner and repository showing the same repository.
   *
   * <p>Whichever of the two the user is typing in, the other follows. A half-typed or unparseable
   * URL leaves them alone rather than clearing them: the point is to follow the URL, not to punish
   * someone mid-keystroke.
   */
  private void listenForGithubEdits() {
    Control url = textWidget(NotificationSourceModel.WIDGET_GITHUB_URL);
    Control owner = textWidget(NotificationSourceModel.WIDGET_GITHUB_OWNER);
    Control repo = textWidget(NotificationSourceModel.WIDGET_GITHUB_REPO);
    if (url == null || owner == null || repo == null) {
      return;
    }
    url.addListener(SWT.Modify, event -> syncOwnerAndRepoFromUrl());
    owner.addListener(SWT.Modify, event -> syncUrlFromOwnerAndRepo());
    repo.addListener(SWT.Modify, event -> syncUrlFromOwnerAndRepo());
  }

  private void syncOwnerAndRepoFromUrl() {
    if (syncingGithubFields) {
      return;
    }
    readWidgets();
    String[] ownerRepo = NotificationSourceModel.parseOwnerAndRepo(model.getGithubUrl());
    if (ownerRepo == null) {
      return;
    }
    syncingGithubFields = true;
    try {
      setWidgetText(NotificationSourceModel.WIDGET_GITHUB_OWNER, ownerRepo[0]);
      setWidgetText(NotificationSourceModel.WIDGET_GITHUB_REPO, ownerRepo[1]);
    } finally {
      syncingGithubFields = false;
    }
  }

  private void syncUrlFromOwnerAndRepo() {
    if (syncingGithubFields) {
      return;
    }
    readWidgets();
    String owner = model.getGithubOwner();
    String repo = model.getGithubRepo();
    if (Utils.isEmpty(owner) || Utils.isEmpty(repo)) {
      return;
    }
    syncingGithubFields = true;
    try {
      setWidgetText(
          NotificationSourceModel.WIDGET_GITHUB_URL,
          "https://github.com/" + owner.trim() + "/" + repo.trim());
    } finally {
      syncingGithubFields = false;
    }
  }

  /**
   * @param widgetId The widget to look up
   * @return The control, or null when the form does not carry it
   */
  private Control textWidget(String widgetId) {
    Control control = widgets.getWidgetsMap().get(widgetId);
    return control == null || control.isDisposed() ? null : control;
  }

  private void setWidgetText(String widgetId, String text) {
    Control control = textWidget(widgetId);
    if (control instanceof org.apache.hop.ui.core.widget.TextVar textVar) {
      textVar.setText(text);
    } else if (control instanceof org.eclipse.swt.widgets.Text plainText) {
      plainText.setText(text);
    }
  }

  /**
   * Read the form, check it, and write it onto the source being edited.
   *
   * @return true when the source was saved and the dialog may close
   */
  private boolean saveSource() {
    readWidgets();

    if (Utils.isEmpty(trimmed(model.getName()))) {
      return refuse("NotificationSourceDialog.Error.NameRequired");
    }

    switch (model.selectedType()) {
      case GITHUB_RELEASES:
        // A URL on its own is enough: the owner and repository are read out of it, the same way
        // the Parse button does it, rather than insisting they be filled in twice.
        if (Utils.isEmpty(trimmed(model.getGithubOwner()))
            || Utils.isEmpty(trimmed(model.getGithubRepo()))) {
          String[] ownerRepo = NotificationSourceModel.parseOwnerAndRepo(model.getGithubUrl());
          if (ownerRepo == null) {
            return refuse("NotificationSourceDialog.Error.GithubRequired");
          }
          model.setGithubOwner(ownerRepo[0]);
          model.setGithubRepo(ownerRepo[1]);
        }
        break;
      case RSS_FEED:
        if (Utils.isEmpty(trimmed(model.getRssUrl()))) {
          return refuse("NotificationSourceDialog.Error.RssRequired");
        }
        break;
      case CUSTOM_PLUGIN:
        if (Utils.isEmpty(trimmed(model.getPluginId()))) {
          return refuse("NotificationSourceDialog.Error.PluginRequired");
        }
        break;
    }

    if (!checkPollInterval() || !checkDaysToGoBack()) {
      return false;
    }

    if (Utils.isEmpty(sourceConfig.getId())) {
      sourceConfig.setId("source-" + UUID.randomUUID().toString().substring(0, 8));
    }
    model.toConfig(sourceConfig);
    return true;
  }

  /**
   * @return false when the poll interval is not a positive number of minutes
   */
  private boolean checkPollInterval() {
    String value = trimmed(model.getPollIntervalMinutes());
    if (Utils.isEmpty(value)) {
      // Left empty on purpose: the source falls back to the global setting.
      model.setPollIntervalMinutes(
          org.apache.hop.core.config.HopConfig.readOptionString(
              "notification.global.pollIntervalMinutes", "60"));
      return true;
    }
    try {
      if (Integer.parseInt(value) <= 0) {
        return refuse("NotificationSourceDialog.Error.PollIntervalPositive");
      }
    } catch (NumberFormatException e) {
      return refuse("NotificationSourceDialog.Error.PollIntervalNumber", e);
    }
    return true;
  }

  /**
   * @return false when the window is not a number of days at or above zero
   */
  private boolean checkDaysToGoBack() {
    String value = trimmed(model.getDaysToGoBack());
    if (Utils.isEmpty(value)) {
      model.setDaysToGoBack("0"); // 0 means use the global setting
      return true;
    }
    try {
      if (Integer.parseInt(value) < 0) {
        return refuse("NotificationSourceDialog.Error.DaysNotNegative");
      }
    } catch (NumberFormatException e) {
      return refuse("NotificationSourceDialog.Error.DaysNumber", e);
    }
    return true;
  }

  private boolean refuse(String messageKey) {
    return refuse(messageKey, new Exception());
  }

  private boolean refuse(String messageKey, Exception e) {
    new ErrorDialog(
        shell,
        BaseMessages.getString(PKG, "NotificationSourceDialog.Error.Title"),
        BaseMessages.getString(PKG, messageKey),
        e);
    return false;
  }

  private static String trimmed(String value) {
    return value == null ? null : value.trim();
  }

  /**
   * Read a GitHub owner and repository out of a URL or an {@code owner/repo} pair.
   *
   * @param input The text to read
   * @return The owner and the repository, or null when the text is not one of those
   */
  static String[] parseOwnerAndRepo(String input) {
    return NotificationSourceModel.parseOwnerAndRepo(input);
  }

  public NotificationSourceConfig getSourceConfig() {
    return sourceConfig;
  }
}
