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

package org.apache.hop.ui.hopgui.search;

import org.apache.hop.core.Const;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.search.config.SearchConfig;
import org.apache.hop.ui.hopgui.search.config.SearchConfigSingleton;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

/** Compact settings dialog for Hop GUI search limits (also available in Configuration). */
public class SearchSettingsDialog extends Dialog {

  private static final Class<?> PKG = SearchEverywhereDialog.class; // shared search messages

  private final Shell parent;
  private final HopGui hopGui;
  private final PropsUi props = PropsUi.getInstance();
  private Shell shell;

  private TextVar wMinLength;
  private TextVar wMaxResults;
  private TextVar wMaxPerFile;
  private TextVar wMaxSizeMb;
  private Button wIncludeTextFiles;
  private Button wSearchAsYouType;
  private TextVar wDebounceMs;

  private boolean saved;

  public SearchSettingsDialog(Shell parent, HopGui hopGui) {
    super(parent, SWT.NONE);
    this.parent = parent;
    this.hopGui = hopGui;
  }

  /**
   * Open the dialog.
   *
   * @return true if the user saved changes
   */
  public boolean open() {
    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.APPLICATION_MODAL | SWT.SHEET);
    shell.setText(BaseMessages.getString(PKG, "SearchSettingsDialog.Shell.Title"));
    shell.setImage(GuiResource.getInstance().getImage("ui/images/gear.svg", 16, 16));
    PropsUi.setLook(shell);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();
    shell.setLayout(formLayout);

    int margin = PropsUi.getMargin();
    int middle = props.getMiddlePct();

    wMinLength =
        addTextField(
            "SearchSettingsDialog.MinContentQueryLength.Label",
            "SearchSettingsDialog.MinContentQueryLength.Tooltip",
            middle,
            margin,
            null);
    wMaxResults =
        addTextField(
            "SearchSettingsDialog.MaxResults.Label",
            "SearchSettingsDialog.MaxResults.Tooltip",
            middle,
            margin,
            wMinLength);
    wMaxPerFile =
        addTextField(
            "SearchSettingsDialog.MaxMatchesPerFile.Label",
            "SearchSettingsDialog.MaxMatchesPerFile.Tooltip",
            middle,
            margin,
            wMaxResults);
    wMaxSizeMb =
        addTextField(
            "SearchSettingsDialog.MaxTextFileSizeMb.Label",
            "SearchSettingsDialog.MaxTextFileSizeMb.Tooltip",
            middle,
            margin,
            wMaxPerFile);
    wDebounceMs =
        addTextField(
            "SearchSettingsDialog.DebounceMs.Label",
            "SearchSettingsDialog.DebounceMs.Tooltip",
            middle,
            margin,
            wMaxSizeMb);

    wIncludeTextFiles = new Button(shell, SWT.CHECK);
    PropsUi.setLook(wIncludeTextFiles);
    wIncludeTextFiles.setText(
        BaseMessages.getString(PKG, "SearchSettingsDialog.IncludeProjectTextFiles.Label"));
    wIncludeTextFiles.setToolTipText(
        BaseMessages.getString(PKG, "SearchSettingsDialog.IncludeProjectTextFiles.Tooltip"));
    FormData fdInclude = new FormData();
    fdInclude.left = new FormAttachment(0, 0);
    fdInclude.right = new FormAttachment(100, 0);
    fdInclude.top = new FormAttachment(wDebounceMs, margin * 2);
    wIncludeTextFiles.setLayoutData(fdInclude);

    wSearchAsYouType = new Button(shell, SWT.CHECK);
    PropsUi.setLook(wSearchAsYouType);
    wSearchAsYouType.setText(
        BaseMessages.getString(PKG, "SearchSettingsDialog.SearchAsYouType.Label"));
    wSearchAsYouType.setToolTipText(
        BaseMessages.getString(PKG, "SearchSettingsDialog.SearchAsYouType.Tooltip"));
    FormData fdAsYouType = new FormData();
    fdAsYouType.left = new FormAttachment(0, 0);
    fdAsYouType.right = new FormAttachment(100, 0);
    fdAsYouType.top = new FormAttachment(wIncludeTextFiles, margin);
    wSearchAsYouType.setLayoutData(fdAsYouType);

    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());

    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());

    BaseTransformDialog.positionBottomButtons(
        shell, new Button[] {wOk, wCancel}, margin, wSearchAsYouType);

    loadValues();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return saved;
  }

  private TextVar addTextField(
      String labelKey, String tooltipKey, int middle, int margin, Control last) {
    Label label = new Label(shell, SWT.RIGHT);
    PropsUi.setLook(label);
    label.setText(BaseMessages.getString(PKG, labelKey));
    label.setToolTipText(BaseMessages.getString(PKG, tooltipKey));
    FormData fdLabel = new FormData();
    fdLabel.left = new FormAttachment(0, 0);
    fdLabel.right = new FormAttachment(middle, -margin);
    fdLabel.top = new FormAttachment(last, last == null ? 0 : margin);
    label.setLayoutData(fdLabel);

    TextVar field = new TextVar(hopGui.getVariables(), shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(field);
    field.setToolTipText(BaseMessages.getString(PKG, tooltipKey));
    FormData fdField = new FormData();
    fdField.left = new FormAttachment(middle, 0);
    fdField.right = new FormAttachment(100, 0);
    fdField.top = new FormAttachment(label, 0, SWT.CENTER);
    field.setLayoutData(fdField);
    return field;
  }

  private void loadValues() {
    SearchConfig config = SearchConfigSingleton.getConfig();
    wMinLength.setText(
        Const.NVL(
            config.getMinContentQueryLength(), SearchConfig.DEFAULT_MIN_CONTENT_QUERY_LENGTH));
    wMaxResults.setText(Const.NVL(config.getMaxResults(), SearchConfig.DEFAULT_MAX_RESULTS));
    wMaxPerFile.setText(
        Const.NVL(config.getMaxMatchesPerFile(), SearchConfig.DEFAULT_MAX_MATCHES_PER_FILE));
    wMaxSizeMb.setText(
        Const.NVL(config.getMaxTextFileSizeMb(), SearchConfig.DEFAULT_MAX_TEXT_FILE_SIZE_MB));
    wDebounceMs.setText(Const.NVL(config.getDebounceMs(), SearchConfig.DEFAULT_DEBOUNCE_MS));
    wIncludeTextFiles.setSelection(
        config.getIncludeProjectTextFiles() == null || config.getIncludeProjectTextFiles());
    wSearchAsYouType.setSelection(
        config.getSearchAsYouType() == null || config.getSearchAsYouType());
  }

  private void ok() {
    SearchConfig config = SearchConfigSingleton.getConfig();
    config.setMinContentQueryLength(wMinLength.getText());
    config.setMaxResults(wMaxResults.getText());
    config.setMaxMatchesPerFile(wMaxPerFile.getText());
    config.setMaxTextFileSizeMb(wMaxSizeMb.getText());
    config.setDebounceMs(wDebounceMs.getText());
    config.setIncludeProjectTextFiles(wIncludeTextFiles.getSelection());
    config.setSearchAsYouType(wSearchAsYouType.getSelection());
    try {
      SearchConfigSingleton.saveConfig();
      saved = true;
      dispose();
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "SearchSettingsDialog.Error.Title"),
          BaseMessages.getString(PKG, "SearchSettingsDialog.Error.Save.Message"),
          e);
    }
  }

  private void cancel() {
    saved = false;
    dispose();
  }

  private void dispose() {
    if (shell != null && !shell.isDisposed()) {
      props.setScreen(new WindowProperty(shell));
      shell.dispose();
    }
  }
}
