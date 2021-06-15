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

package org.apache.hop.workflow.actions.addresultfilenames;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.IActionDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

/**
 * This dialog allows you to edit the Delete Files action settings.
 *
 * @author Samatar Hassan
 * @since 06-05-2007
 */
public class ActionAddResultFilenamesDialog extends ActionDialog implements IActionDialog {
  private static final Class<?> PKG = ActionAddResultFilenamesDialog.class; // For Translator

  private static final String[] FILETYPES =
      new String[] {BaseMessages.getString(PKG, "ActionAddResultFilenames.Filetype.All")};

  private Text wName;

  private Label wlFilename;
  private Button wbFilename, wbDirectory;
  private TextVar wFilename;

  private Button wIncludeSubfolders;

  private Button wDeleteAllBefore;

  private ActionAddResultFilenames action;
  private Shell shell;

  private boolean changed;

  private Button wPrevious;

  private Label wlFields;
  private TableView wFields;

  private Label wlFilemask;
  private TextVar wFilemask;

  private Button wbdFilename; // Delete
  private Button wbeFilename; // Edit
  private Button wbaFilename; // Add or change

  public ActionAddResultFilenamesDialog(
      Shell parent, IAction action, WorkflowMeta workflowMeta, IVariables variables) {
    super(parent, workflowMeta, variables);
    this.action = (ActionAddResultFilenames) action;

    if (this.action.getName() == null) {
      this.action.setName(BaseMessages.getString(PKG, "ActionAddResultFilenames.Name.Default"));
    }
  }

  public IAction open() {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE);
    props.setLook(shell);
    WorkflowDialog.setShellImage(shell, action);

    ModifyListener lsMod = e -> action.setChanged();
    changed = action.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "ActionAddResultFilenames.Title"));

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Buttons at the bottom
    //
    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wOk, wCancel}, margin, null);

    // Filename line
    Label wlName = new Label(shell, SWT.RIGHT);
    wlName.setText(BaseMessages.getString(PKG, "ActionAddResultFilenames.Name.Label"));
    props.setLook(wlName);
    FormData fdlName = new FormData();
    fdlName.left = new FormAttachment(0, 0);
    fdlName.right = new FormAttachment(middle, -margin);
    fdlName.top = new FormAttachment(0, margin);
    wlName.setLayoutData(fdlName);
    wName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wName);
    wName.addModifyListener(lsMod);
    FormData fdName = new FormData();
    fdName.left = new FormAttachment(middle, 0);
    fdName.top = new FormAttachment(0, margin);
    fdName.right = new FormAttachment(100, 0);
    wName.setLayoutData(fdName);

    // SETTINGS grouping?
    // ////////////////////////
    // START OF SETTINGS GROUP
    //

    Group wgSettings = new Group(shell, SWT.SHADOW_NONE);
    props.setLook(wgSettings);
    wgSettings.setText(BaseMessages.getString(PKG, "ActionAddResultFilenames.Settings.Label"));

    FormLayout groupLayout = new FormLayout();
    groupLayout.marginWidth = 10;
    groupLayout.marginHeight = 10;
    wgSettings.setLayout(groupLayout);

    Label wlIncludeSubfolders = new Label(wgSettings, SWT.RIGHT);
    wlIncludeSubfolders.setText(
        BaseMessages.getString(PKG, "ActionAddResultFilenames.IncludeSubfolders.Label"));
    props.setLook(wlIncludeSubfolders);
    FormData fdlIncludeSubfolders = new FormData();
    fdlIncludeSubfolders.left = new FormAttachment(0, 0);
    fdlIncludeSubfolders.top = new FormAttachment(0, 2 * margin);
    fdlIncludeSubfolders.right = new FormAttachment(middle, -margin);
    wlIncludeSubfolders.setLayoutData(fdlIncludeSubfolders);
    wIncludeSubfolders = new Button(wgSettings, SWT.CHECK);
    props.setLook(wIncludeSubfolders);
    wIncludeSubfolders.setToolTipText(
        BaseMessages.getString(PKG, "ActionAddResultFilenames.IncludeSubfolders.Tooltip"));
    FormData fdIncludeSubfolders = new FormData();
    fdIncludeSubfolders.left = new FormAttachment(middle, 0);
    fdIncludeSubfolders.top = new FormAttachment(wlIncludeSubfolders, 0, SWT.CENTER);
    fdIncludeSubfolders.right = new FormAttachment(100, 0);
    wIncludeSubfolders.setLayoutData(fdIncludeSubfolders);
    wIncludeSubfolders.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
          }
        });

    Label wlPrevious = new Label(wgSettings, SWT.RIGHT);
    wlPrevious.setText(BaseMessages.getString(PKG, "ActionAddResultFilenames.Previous.Label"));
    props.setLook(wlPrevious);
    FormData fdlPrevious = new FormData();
    fdlPrevious.left = new FormAttachment(0, 0);
    fdlPrevious.top = new FormAttachment(wlIncludeSubfolders, 2 * margin);
    fdlPrevious.right = new FormAttachment(middle, -margin);
    wlPrevious.setLayoutData(fdlPrevious);
    wPrevious = new Button(wgSettings, SWT.CHECK);
    props.setLook(wPrevious);
    wPrevious.setSelection(action.isArgFromPrevious());
    wPrevious.setToolTipText(
        BaseMessages.getString(PKG, "ActionAddResultFilenames.Previous.Tooltip"));
    FormData fdPrevious = new FormData();
    fdPrevious.left = new FormAttachment(middle, 0);
    fdPrevious.top = new FormAttachment(wlPrevious, 0, SWT.CENTER);
    fdPrevious.right = new FormAttachment(100, 0);
    wPrevious.setLayoutData(fdPrevious);
    wPrevious.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {

            setPrevious();
            action.setChanged();
          }
        });

    Label wlDeleteAllBefore = new Label(wgSettings, SWT.RIGHT);
    wlDeleteAllBefore.setText(
        BaseMessages.getString(PKG, "ActionAddResultFilenames.DeleteAllBefore.Label"));
    props.setLook(wlDeleteAllBefore);
    FormData fdlDeleteAllBefore = new FormData();
    fdlDeleteAllBefore.left = new FormAttachment(0, 0);
    fdlDeleteAllBefore.top = new FormAttachment(wlPrevious, 2 * margin);
    fdlDeleteAllBefore.right = new FormAttachment(middle, -margin);
    wlDeleteAllBefore.setLayoutData(fdlDeleteAllBefore);
    wDeleteAllBefore = new Button(wgSettings, SWT.CHECK);
    props.setLook(wDeleteAllBefore);
    wDeleteAllBefore.setToolTipText(
        BaseMessages.getString(PKG, "ActionAddResultFilenames.DeleteAllBefore.Tooltip"));
    FormData fdDeleteAllBefore = new FormData();
    fdDeleteAllBefore.left = new FormAttachment(middle, 0);
    fdDeleteAllBefore.top = new FormAttachment(wlDeleteAllBefore, 0, SWT.CENTER);
    fdDeleteAllBefore.right = new FormAttachment(100, 0);
    wDeleteAllBefore.setLayoutData(fdDeleteAllBefore);
    wDeleteAllBefore.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
          }
        });

    FormData fdSettings = new FormData();
    fdSettings.left = new FormAttachment(0, margin);
    fdSettings.top = new FormAttachment(wName, margin);
    fdSettings.right = new FormAttachment(100, -margin);
    wgSettings.setLayoutData(fdSettings);

    // ///////////////////////////////////////////////////////////
    // / END OF SETTINGS GROUP
    // ///////////////////////////////////////////////////////////

    // Filename line
    wlFilename = new Label(shell, SWT.RIGHT);
    wlFilename.setText(BaseMessages.getString(PKG, "ActionAddResultFilenames.Filename.Label"));
    props.setLook(wlFilename);
    FormData fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment(0, 0);
    fdlFilename.top = new FormAttachment(wgSettings, 2 * margin);
    fdlFilename.right = new FormAttachment(middle, -margin);
    wlFilename.setLayoutData(fdlFilename);

    // Browse Source folders button ...
    wbDirectory = new Button(shell, SWT.PUSH | SWT.CENTER);
    props.setLook(wbDirectory);
    wbDirectory.setText(
        BaseMessages.getString(PKG, "ActionAddResultFilenames.BrowseFolders.Label"));
    FormData fdbDirectory = new FormData();
    fdbDirectory.right = new FormAttachment(100, margin);
    fdbDirectory.top = new FormAttachment(wgSettings, margin);
    wbDirectory.setLayoutData(fdbDirectory);

    wbDirectory.addListener(
        SWT.Selection, e -> BaseDialog.presentDirectoryDialog(shell, wFilename, variables));

    wbFilename = new Button(shell, SWT.PUSH | SWT.CENTER);
    props.setLook(wbFilename);
    wbFilename.setText(BaseMessages.getString(PKG, "ActionAddResultFilenames.BrowseFiles.Label"));
    FormData fdbFilename = new FormData();
    fdbFilename.right = new FormAttachment(100, 0);
    fdbFilename.top = new FormAttachment(wgSettings, margin);
    fdbFilename.right = new FormAttachment(wbDirectory, -margin);
    wbFilename.setLayoutData(fdbFilename);

    wbaFilename = new Button(shell, SWT.PUSH | SWT.CENTER);
    props.setLook(wbaFilename);
    wbaFilename.setText(BaseMessages.getString(PKG, "ActionAddResultFilenames.FilenameAdd.Button"));
    FormData fdbaFilename = new FormData();
    fdbaFilename.right = new FormAttachment(wbFilename, -margin);
    fdbaFilename.top = new FormAttachment(wgSettings, margin);
    wbaFilename.setLayoutData(fdbaFilename);

    wFilename = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wFilename);
    wFilename.addModifyListener(lsMod);
    FormData fdFilename = new FormData();
    fdFilename.left = new FormAttachment(middle, 0);
    fdFilename.top = new FormAttachment(wgSettings, 2 * margin);
    fdFilename.right = new FormAttachment(wbaFilename, -2 * margin);
    wFilename.setLayoutData(fdFilename);

    // Whenever something changes, set the tooltip to the expanded version:
    wFilename.addModifyListener(
        e -> wFilename.setToolTipText(variables.resolve(wFilename.getText())));

    wbFilename.addListener(
        SWT.Selection,
        e ->
            BaseDialog.presentFileDialog(
                shell, wFilename, variables, new String[] {"*"}, FILETYPES, true));

    // Filemask
    wlFilemask = new Label(shell, SWT.RIGHT);
    wlFilemask.setText(BaseMessages.getString(PKG, "ActionAddResultFilenames.Wildcard.Label"));
    props.setLook(wlFilemask);
    FormData fdlFilemask = new FormData();
    fdlFilemask.left = new FormAttachment(0, 0);
    fdlFilemask.top = new FormAttachment(wFilename, margin);
    fdlFilemask.right = new FormAttachment(middle, -margin);
    wlFilemask.setLayoutData(fdlFilemask);
    wFilemask =
        new TextVar(
            variables,
            shell,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER,
            BaseMessages.getString(PKG, "ActionAddResultFilenames.Wildcard.Tooltip"));
    props.setLook(wFilemask);
    wFilemask.addModifyListener(lsMod);
    FormData fdFilemask = new FormData();
    fdFilemask.left = new FormAttachment(middle, 0);
    fdFilemask.top = new FormAttachment(wFilename, margin);
    fdFilemask.right = new FormAttachment(wbaFilename, -2 * margin);
    wFilemask.setLayoutData(fdFilemask);

    wlFields = new Label(shell, SWT.NONE);
    wlFields.setText(BaseMessages.getString(PKG, "ActionAddResultFilenames.Fields.Label"));
    props.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.right = new FormAttachment(middle, -margin);
    fdlFields.top = new FormAttachment(wFilemask, margin);
    wlFields.setLayoutData(fdlFields);

    // Buttons to the right of the screen...
    wbdFilename = new Button(shell, SWT.PUSH | SWT.CENTER);
    props.setLook(wbdFilename);
    wbdFilename.setText(
        BaseMessages.getString(PKG, "ActionAddResultFilenames.FilenameDelete.Button"));
    wbdFilename.setToolTipText(
        BaseMessages.getString(PKG, "ActionAddResultFilenames.FilenameDelete.Tooltip"));
    FormData fdbdFilename = new FormData();
    fdbdFilename.right = new FormAttachment(100, 0);
    fdbdFilename.top = new FormAttachment(wlFields, margin);
    wbdFilename.setLayoutData(fdbdFilename);

    wbeFilename = new Button(shell, SWT.PUSH | SWT.CENTER);
    props.setLook(wbeFilename);
    wbeFilename.setText(
        BaseMessages.getString(PKG, "ActionAddResultFilenames.FilenameEdit.Button"));
    wbeFilename.setToolTipText(
        BaseMessages.getString(PKG, "ActionAddResultFilenames.FilenameEdit.Tooltip"));
    FormData fdbeFilename = new FormData();
    fdbeFilename.right = new FormAttachment(100, 0);
    fdbeFilename.left = new FormAttachment(wbdFilename, 0, SWT.LEFT);
    fdbeFilename.top = new FormAttachment(wbdFilename, margin);
    wbeFilename.setLayoutData(fdbeFilename);

    ColumnInfo[] colinf =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "ActionAddResultFilenames.Fields.Argument.Label"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ActionAddResultFilenames.Fields.Wildcard.Label"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
        };

    colinf[0].setUsingVariables(true);
    colinf[0].setToolTip(BaseMessages.getString(PKG, "ActionAddResultFilenames.Fields.Column"));
    colinf[1].setUsingVariables(true);
    colinf[1].setToolTip(BaseMessages.getString(PKG, "ActionAddResultFilenames.Wildcard.Column"));

    wFields =
        new TableView(
            variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colinf,
            action.getArguments().size(),
            lsMod,
            props);

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(wlFields, margin);
    fdFields.right = new FormAttachment(wbdFilename, -2 * margin);
    fdFields.bottom = new FormAttachment(wOk, -2 * margin);
    wFields.setLayoutData(fdFields);

    wlFields.setEnabled(!action.isArgFromPrevious());
    wFields.setEnabled(!action.isArgFromPrevious());

    // Add the file to the list of files...
    SelectionAdapter selA =
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent arg0) {
            wFields.add(wFilename.getText(), wFilemask.getText());
            wFilename.setText("");
            wFilemask.setText("");
            wFields.removeEmptyRows();
            wFields.setRowNums();
            wFields.optWidth(true);
          }
        };
    wbaFilename.addSelectionListener(selA);
    wFilename.addSelectionListener(selA);

    // Delete files from the list of files...
    wbdFilename.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent arg0) {
            int[] idx = wFields.getSelectionIndices();
            wFields.remove(idx);
            wFields.removeEmptyRows();
            wFields.setRowNums();
          }
        });

    // Edit the selected file & remove from the list...
    wbeFilename.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent arg0) {
            int idx = wFields.getSelectionIndex();
            if (idx >= 0) {
              String[] string = wFields.getItem(idx);
              wFilename.setText(string[0]);
              wFilemask.setText(string[1]);
              wFields.remove(idx);
            }
            wFields.removeEmptyRows();
            wFields.setRowNums();
          }
        });

    getData();
    setPrevious();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return action;
  }

  private void setPrevious() {
    wlFields.setEnabled(!wPrevious.getSelection());
    wFields.setEnabled(!wPrevious.getSelection());

    wFilename.setEnabled(!wPrevious.getSelection());
    wlFilename.setEnabled(!wPrevious.getSelection());
    wbFilename.setEnabled(!wPrevious.getSelection());

    wlFilemask.setEnabled(!wPrevious.getSelection());
    wFilemask.setEnabled(!wPrevious.getSelection());

    wbdFilename.setEnabled(!wPrevious.getSelection());
    wbeFilename.setEnabled(!wPrevious.getSelection());
    wbaFilename.setEnabled(!wPrevious.getSelection());

    wbDirectory.setEnabled(!wPrevious.getSelection());
  }

  public void dispose() {
    WindowProperty winprop = new WindowProperty(shell);
    props.setScreen(winprop);
    shell.dispose();
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    if (action.getName() != null) {
      wName.setText(action.getName());
    }
    for (int i = 0; i < action.getArguments().size(); i++) {
      Argument argument = action.getArguments().get(i);
      TableItem ti = wFields.table.getItem(i);
      ti.setText(1, Const.NVL(argument.getArgument(), ""));
      ti.setText(2, Const.NVL(argument.getMask(), ""));
    }
    wFields.setRowNums();
    wFields.optWidth(true);

    wPrevious.setSelection(action.isArgFromPrevious());
    wIncludeSubfolders.setSelection(action.isIncludeSubFolders());
    wDeleteAllBefore.setSelection(action.isDeleteAllBefore());

    wName.selectAll();
    wName.setFocus();
  }

  private void cancel() {
    action.setChanged(changed);
    action = null;
    dispose();
  }

  private void ok() {
    if (Utils.isEmpty(wName.getText())) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setText(BaseMessages.getString(PKG, "System.TransformActionNameMissing.Title"));
      mb.setMessage(BaseMessages.getString(PKG, "System.ActionNameMissing.Msg"));
      mb.open();
      return;
    }
    action.setName(wName.getText());
    action.setIncludeSubFolders(wIncludeSubfolders.getSelection());
    action.setArgumentsPrevious(wPrevious.getSelection());
    action.setDeleteAllBefore(wDeleteAllBefore.getSelection());

    action.getArguments().clear();
    for (TableItem item : wFields.getNonEmptyItems()) {
      String arg = item.getText(1);
      String mask = item.getText(2);
      if (StringUtils.isNotEmpty(arg)) {
        action.getArguments().add(new Argument(arg, mask));
      }
    }

    dispose();
  }
}
