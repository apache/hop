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

package org.apache.hop.workflow.actions.copyfiles;

import java.util.HashMap;
import java.util.Map;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ITextVarButtonRenderCallback;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

/** This dialog allows you to edit the Copy Files action settings. */
public class ActionCopyFilesDialog extends ActionDialog {
  private static final Class<?> PKG = ActionCopyFiles.class;

  protected static final String[] FILETYPES =
      new String[] {BaseMessages.getString(PKG, "ActionCopyFiles.Filetype.All")};

  public static final String LOCAL_ENVIRONMENT = "Local";
  public static final String STATIC_ENVIRONMENT = "<Static>";

  protected Button wPrevious;
  protected Button wCopyEmptyFolders;
  protected Button wOverwriteFiles;
  protected Button wIncludeSubfolders;
  protected Button wRemoveSourceFiles;
  protected Button wAddFileToResult;
  protected Button wDestinationIsAFile;
  protected Button wCreateDestinationFolder;

  protected ActionCopyFiles action;

  protected boolean changed;

  protected CTabFolder wTabFolder;

  private Label wlFields;

  protected TableView wFields;

  public ActionCopyFilesDialog(
      Shell parent, ActionCopyFiles action, WorkflowMeta workflowMeta, IVariables variables) {
    super(parent, workflowMeta, variables);
    this.action = action;

    if (this.action.getName() == null) {
      this.action.setName(BaseMessages.getString(PKG, "ActionCopyFiles.Name.Default"));
    }
  }

  protected void initUi() {
    createShell(BaseMessages.getString(PKG, "ActionCopyFiles.Title"), action);
    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    ModifyListener lsMod = e -> action.setChanged();
    changed = action.hasChanged();

    int middle = this.middle;
    int margin = this.margin;

    wTabFolder = new CTabFolder(shell, SWT.BORDER);
    PropsUi.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(wSpacer, margin);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(wCancel, -margin);
    wTabFolder.setLayoutData(fdTabFolder);

    // ///////////////////////////////////////////////////////////
    // / START OF FILES TAB
    // ///////////////////////////////////////////////////////////

    CTabItem wFilesTab = new CTabItem(wTabFolder, SWT.NONE);
    wFilesTab.setFont(GuiResource.getInstance().getFontDefault());
    wFilesTab.setText(BaseMessages.getString(PKG, "ActionCopyFiles.Tab.Files.Label"));

    Composite wFilesComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wFilesComp);

    FormLayout filesLayout = new FormLayout();
    filesLayout.marginWidth = 3;
    filesLayout.marginHeight = 3;
    wFilesComp.setLayout(filesLayout);

    FormData fdFilesComp = new FormData();
    fdFilesComp.left = new FormAttachment(0, 0);
    fdFilesComp.top = new FormAttachment(0, 0);
    fdFilesComp.right = new FormAttachment(100, 0);
    fdFilesComp.bottom = new FormAttachment(100, 0);
    wFilesComp.setLayoutData(fdFilesComp);

    wFilesComp.layout();
    wFilesTab.setControl(wFilesComp);

    // ///////////////////////////////////////////////////////////
    // / END OF FILES TAB
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF SETTINGS TAB ///
    // ////////////////////////

    CTabItem wSettingsTab = new CTabItem(wTabFolder, SWT.NONE);
    wSettingsTab.setFont(GuiResource.getInstance().getFontDefault());
    wSettingsTab.setText(BaseMessages.getString(PKG, "ActionCopyFiles.Settings.Label"));

    Composite wSettingsComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wSettingsComp);

    FormLayout settingsLayout = new FormLayout();
    settingsLayout.marginWidth = 3;
    settingsLayout.marginHeight = 3;
    wSettingsComp.setLayout(settingsLayout);

    SelectionAdapter listener =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
          }
        };

    wIncludeSubfolders =
        createSettingsButton(
            wSettingsComp,
            BaseMessages.getString(PKG, "ActionCopyFiles.IncludeSubfolders.Label"),
            BaseMessages.getString(PKG, "ActionCopyFiles.IncludeSubfolders.Tooltip"),
            null,
            listener);

    wDestinationIsAFile =
        createSettingsButton(
            wSettingsComp,
            BaseMessages.getString(PKG, "ActionCopyFiles.DestinationIsAFile.Label"),
            BaseMessages.getString(PKG, "ActionCopyFiles.DestinationIsAFile.Tooltip"),
            wIncludeSubfolders,
            listener);

    wCopyEmptyFolders =
        createSettingsButton(
            wSettingsComp,
            BaseMessages.getString(PKG, "ActionCopyFiles.CopyEmptyFolders.Label"),
            BaseMessages.getString(PKG, "ActionCopyFiles.CopyEmptyFolders.Tooltip"),
            wDestinationIsAFile,
            listener);

    wCreateDestinationFolder =
        createSettingsButton(
            wSettingsComp,
            BaseMessages.getString(PKG, "ActionCopyFiles.CreateDestinationFolder.Label"),
            BaseMessages.getString(PKG, "ActionCopyFiles.CreateDestinationFolder.Tooltip"),
            wCopyEmptyFolders,
            listener);

    wOverwriteFiles =
        createSettingsButton(
            wSettingsComp,
            BaseMessages.getString(PKG, "ActionCopyFiles.OverwriteFiles.Label"),
            BaseMessages.getString(PKG, "ActionCopyFiles.OverwriteFiles.Tooltip"),
            wCreateDestinationFolder,
            listener);

    wRemoveSourceFiles =
        createSettingsButton(
            wSettingsComp,
            BaseMessages.getString(PKG, "ActionCopyFiles.RemoveSourceFiles.Label"),
            BaseMessages.getString(PKG, "ActionCopyFiles.RemoveSourceFiles.Tooltip"),
            wOverwriteFiles,
            listener);

    wPrevious =
        createSettingsButton(
            wSettingsComp,
            BaseMessages.getString(PKG, "ActionCopyFiles.Previous.Label"),
            BaseMessages.getString(PKG, "ActionCopyFiles.Previous.Tooltip"),
            wRemoveSourceFiles,
            listener);

    wAddFileToResult =
        createSettingsButton(
            wSettingsComp,
            BaseMessages.getString(PKG, "ActionCopyFiles.AddFileToResult.Label"),
            BaseMessages.getString(PKG, "ActionCopyFiles.AddFileToResult.Tooltip"),
            wPrevious,
            listener);

    FormData fdSettingsComp = new FormData();
    fdSettingsComp.left = new FormAttachment(0, 0);
    fdSettingsComp.top = new FormAttachment(0, 0);
    fdSettingsComp.right = new FormAttachment(100, 0);
    fdSettingsComp.bottom = new FormAttachment(100, 0);
    wSettingsComp.setLayoutData(fdSettingsComp);

    wSettingsComp.layout();
    wSettingsTab.setControl(wSettingsComp);
    PropsUi.setLook(wSettingsComp);

    // ///////////////////////////////////////////////////////////
    // / END OF SETTINGS TAB
    // ///////////////////////////////////////////////////////////

    wlFields = new Label(wFilesComp, SWT.NONE);
    wlFields.setText(BaseMessages.getString(PKG, "ActionCopyFiles.Fields.Label"));
    PropsUi.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, margin);
    fdlFields.top = new FormAttachment(wFilesComp, 0, SWT.CENTER);
    wlFields.setLayoutData(fdlFields);

    int rows =
        action.sourceFileFolder == null
            ? 1
            : (action.sourceFileFolder.length == 0 ? 0 : action.sourceFileFolder.length);
    final int FieldsRows = rows;

    ColumnInfo[] colinf =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "ActionCopyFiles.Fields.SourceEnvironment.Label"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              false,
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ActionCopyFiles.Fields.SourceFileFolder.Label"),
              ColumnInfo.COLUMN_TYPE_TEXT_BUTTON,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ActionCopyFiles.Fields.Wildcard.Label"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ActionCopyFiles.Fields.DestinationEnvironment.Label"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              false,
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ActionCopyFiles.Fields.DestinationFileFolder.Label"),
              ColumnInfo.COLUMN_TYPE_TEXT_BUTTON,
              false)
        };

    setComboValues(colinf[0]);

    ITextVarButtonRenderCallback callback =
        () -> {
          String envType = wFields.getActiveTableItem().getText(wFields.getActiveTableColumn() - 1);
          return !STATIC_ENVIRONMENT.equalsIgnoreCase(envType);
        };

    colinf[1].setUsingVariables(true);
    colinf[1].setToolTip(
        BaseMessages.getString(PKG, "ActionCopyFiles.Fields.SourceFileFolder.Tooltip"));
    colinf[1].setTextVarButtonSelectionListener(getFileSelectionAdapter());
    colinf[1].setRenderTextVarButtonCallback(callback);

    colinf[2].setUsingVariables(true);
    colinf[2].setToolTip(BaseMessages.getString(PKG, "ActionCopyFiles.Fields.Wildcard.Tooltip"));

    setComboValues(colinf[3]);

    colinf[4].setUsingVariables(true);
    colinf[4].setToolTip(
        BaseMessages.getString(PKG, "ActionCopyFiles.Fields.DestinationFileFolder.Tooltip"));
    colinf[4].setTextVarButtonSelectionListener(getFileSelectionAdapter());

    wFields =
        new TableView(
            variables,
            wFilesComp,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colinf,
            FieldsRows,
            lsMod,
            props);

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, margin);
    fdFields.top = new FormAttachment(wlFields, margin);
    fdFields.right = new FormAttachment(100, -margin);
    fdFields.bottom = new FormAttachment(100, -margin);
    wFields.setLayoutData(fdFields);

    refreshArgFromPrevious();
  }

  @Override
  public IAction open() {
    initUi();
    getData();
    wTabFolder.setSelection(0);
    focusActionName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return action;
  }

  protected Button createSettingsButton(
      Composite p, String text, String title, Control top, SelectionAdapter sa) {
    Button button = new Button(p, SWT.CHECK);
    button.setText(text);
    button.setToolTipText(title);
    PropsUi.setLook(button);
    FormData fd = new FormData();
    fd.left = new FormAttachment(props.getMiddlePct(), PropsUi.getMargin() * 2);
    if (top == null) {
      fd.top = new FormAttachment(0, 10);
    } else {
      fd.top = new FormAttachment(top, 5);
    }
    fd.right = new FormAttachment(100, 0);
    button.setLayoutData(fd);
    button.addSelectionListener(sa);
    return button;
  }

  protected SelectionAdapter getFileSelectionAdapter() {
    return new SelectionAdapter() {
      @Override
      public void widgetSelected(SelectionEvent e) {
        String filename = BaseDialog.presentFileDialog(shell, null, null, true); // all files
        if (filename != null) {
          wFields.getActiveTableItem().setText(wFields.getActiveTableColumn(), filename);
        }
      }
    };
  }

  private void refreshArgFromPrevious() {
    wlFields.setEnabled(!wPrevious.getSelection());
    wFields.setEnabled(!wPrevious.getSelection());
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    wName.setText(Const.NVL(action.getName(), ""));
    wCopyEmptyFolders.setSelection(action.copyEmptyFolders);

    if (action.sourceFileFolder != null) {
      for (int i = 0; i < action.sourceFileFolder.length; i++) {
        TableItem ti = wFields.table.getItem(i);
        if (action.sourceFileFolder[i] != null) {
          String sourceUrl = action.sourceFileFolder[i];
          String clusterName = action.getConfigurationBy(sourceUrl);
          if (clusterName != null) {
            clusterName =
                clusterName.startsWith(ActionCopyFiles.LOCAL_SOURCE_FILE)
                    ? LOCAL_ENVIRONMENT
                    : clusterName;
            clusterName =
                clusterName.startsWith(ActionCopyFiles.STATIC_SOURCE_FILE)
                    ? STATIC_ENVIRONMENT
                    : clusterName;

            ti.setText(1, clusterName);
            sourceUrl =
                clusterName.equals(LOCAL_ENVIRONMENT) || clusterName.equals(STATIC_ENVIRONMENT)
                    ? sourceUrl
                    : action.getUrlPath(sourceUrl);
          }
          if (sourceUrl != null) {
            sourceUrl = sourceUrl.replace(ActionCopyFiles.SOURCE_URL + i + "-", "");
          } else {
            sourceUrl = "";
          }
          ti.setText(2, sourceUrl);
        }
        if (action.wildcard[i] != null) {
          ti.setText(3, action.wildcard[i]);
        }
        if (action.destinationFileFolder[i] != null) {
          String destinationURL = action.destinationFileFolder[i];
          String clusterName = action.getConfigurationBy(destinationURL);
          if (clusterName != null) {
            clusterName =
                clusterName.startsWith(ActionCopyFiles.LOCAL_DEST_FILE)
                    ? LOCAL_ENVIRONMENT
                    : clusterName;
            clusterName =
                clusterName.startsWith(ActionCopyFiles.STATIC_DEST_FILE)
                    ? STATIC_ENVIRONMENT
                    : clusterName;
            ti.setText(4, clusterName);
            destinationURL =
                clusterName.equals(LOCAL_ENVIRONMENT) || clusterName.equals(STATIC_ENVIRONMENT)
                    ? destinationURL
                    : action.getUrlPath(destinationURL);
          }
          if (destinationURL != null) {
            destinationURL = destinationURL.replace(ActionCopyFiles.DEST_URL + i + "-", "");
          } else {
            destinationURL = "";
          }
          ti.setText(5, destinationURL);
        }
      }

      wFields.optimizeTableView();
    }
    wPrevious.setSelection(action.argFromPrevious);
    wOverwriteFiles.setSelection(action.overwriteFiles);
    wIncludeSubfolders.setSelection(action.includeSubFolders);
    wRemoveSourceFiles.setSelection(action.removeSourceFiles);
    wDestinationIsAFile.setSelection(action.destinationIsAFile);
    wCreateDestinationFolder.setSelection(action.createDestinationFolder);

    wAddFileToResult.setSelection(action.addResultFilenames);
  }

  @Override
  protected void onActionNameModified() {
    action.setChanged();
  }

  private void cancel() {
    action.setChanged(changed);
    action = null;
    dispose();
  }

  protected void ok() {
    if (Utils.isEmpty(wName.getText())) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setText(BaseMessages.getString(PKG, "System.TransformActionNameMissing.Title"));
      mb.setMessage(BaseMessages.getString(PKG, "System.ActionNameMissing.Msg"));
      mb.open();
      return;
    }

    action.setName(wName.getText());
    action.setCopyEmptyFolders(wCopyEmptyFolders.getSelection());
    action.setOverwriteFiles(wOverwriteFiles.getSelection());
    action.setIncludeSubFolders(wIncludeSubfolders.getSelection());
    action.setArgFromPrevious(wPrevious.getSelection());
    action.setRemoveSourceFiles(wRemoveSourceFiles.getSelection());
    action.setAddResultFilenames(wAddFileToResult.getSelection());
    action.setDestinationIsAFile(wDestinationIsAFile.getSelection());
    action.setCreateDestinationFolder(wCreateDestinationFolder.getSelection());

    int nrItems = wFields.nrNonEmpty();

    Map<String, String> sourceDestinationMappings = new HashMap<>();
    action.sourceFileFolder = new String[nrItems];
    action.destinationFileFolder = new String[nrItems];
    action.wildcard = new String[nrItems];

    for (int i = 0; i < nrItems; i++) {
      String sourceNc = wFields.getNonEmpty(i).getText(1);
      sourceNc =
          sourceNc.equals(LOCAL_ENVIRONMENT) ? ActionCopyFiles.LOCAL_SOURCE_FILE + i : sourceNc;
      sourceNc =
          sourceNc.equals(STATIC_ENVIRONMENT) ? ActionCopyFiles.STATIC_SOURCE_FILE + i : sourceNc;
      String source = wFields.getNonEmpty(i).getText(2);
      String wild = wFields.getNonEmpty(i).getText(3);
      String destNc = wFields.getNonEmpty(i).getText(4);
      destNc = destNc.equals(LOCAL_ENVIRONMENT) ? ActionCopyFiles.LOCAL_DEST_FILE + i : destNc;
      destNc = destNc.equals(STATIC_ENVIRONMENT) ? ActionCopyFiles.STATIC_DEST_FILE + i : destNc;
      String dest = wFields.getNonEmpty(i).getText(5);
      source = ActionCopyFiles.SOURCE_URL + i + "-" + source;
      dest = ActionCopyFiles.DEST_URL + i + "-" + dest;
      action.sourceFileFolder[i] =
          action.loadURL(source, sourceNc, getMetadataProvider(), sourceDestinationMappings);
      action.destinationFileFolder[i] =
          action.loadURL(dest, destNc, getMetadataProvider(), sourceDestinationMappings);
      action.wildcard[i] = wild;
    }
    action.setConfigurationMappings(sourceDestinationMappings);

    dispose();
  }

  public boolean showFileButtons() {
    return true;
  }

  protected void setComboValues(ColumnInfo colInfo) {
    String[] values = {LOCAL_ENVIRONMENT, STATIC_ENVIRONMENT};
    colInfo.setComboValues(values);
  }
}
