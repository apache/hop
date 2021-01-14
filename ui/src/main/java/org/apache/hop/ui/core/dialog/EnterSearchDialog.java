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

package org.apache.hop.ui.core.dialog;

import org.apache.hop.core.Const;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

public class EnterSearchDialog {
  private static final Class<?> PKG = EnterSearchDialog.class; // For Translator

  private static final PropsUi props = PropsUi.getInstance();
  private Shell parentShell;
  private Shell shell;

  private boolean retval;
  private Display display;

  private boolean searchingTransforms;
  private boolean searchingDatabases;
  private boolean searchingNotes;
  private String filterString;

  private Label wlTransform;
  private Button wTransform;

  private Label wlDB;
  private Button wDB;
  private Label wlNote;
  private Button wNote;

  private Label wlFilter;
  private Text wFilter;

  public EnterSearchDialog(Shell parentShell) {
    this.parentShell = parentShell;
    this.display = parentShell.getDisplay();

    retval = true;

    searchingTransforms = true;
    searchingDatabases = true;
    searchingNotes = true;
  }

  public boolean open() {
    shell = new Shell(parentShell, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
    props.setLook(shell);
    shell.setImage(GuiResource.getInstance().getImageHopUi());
    shell.setText(BaseMessages.getString(PKG, "EnterSearchDialog.Shell.Title"));

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;
    shell.setLayout(formLayout);

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // Search Transforms?...
    wlTransform = new Label(shell, SWT.RIGHT);
    wlTransform.setText(BaseMessages.getString(PKG, "EnterSearchDialog.Transform.Label"));
    props.setLook(wlTransform);
    FormData fdlTransform = new FormData();
    fdlTransform.left = new FormAttachment(0, 0);
    fdlTransform.top = new FormAttachment(0, 0);
    fdlTransform.right = new FormAttachment(middle, -margin);
    wlTransform.setLayoutData(fdlTransform);

    wTransform = new Button(shell, SWT.CHECK);
    props.setLook(wTransform);
    wTransform.setToolTipText(BaseMessages.getString(PKG, "EnterSearchDialog.Transform.Tooltip"));
    FormData fdTransform = new FormData();
    fdTransform.left = new FormAttachment(middle, 0);
    fdTransform.top = new FormAttachment(0, 0);
    fdTransform.right = new FormAttachment(100, 0);
    wTransform.setLayoutData(fdTransform);

    // Search databases...
    wlDB = new Label(shell, SWT.RIGHT);
    wlDB.setText(BaseMessages.getString(PKG, "EnterSearchDialog.DB.Label"));
    props.setLook(wlDB);
    FormData fdlDB = new FormData();
    fdlDB.left = new FormAttachment(0, 0);
    fdlDB.top = new FormAttachment(wTransform, margin);
    fdlDB.right = new FormAttachment(middle, -margin);
    wlDB.setLayoutData(fdlDB);
    wDB = new Button(shell, SWT.CHECK);
    props.setLook(wDB);
    wDB.setToolTipText(BaseMessages.getString(PKG, "EnterSearchDialog.DB.Tooltip"));
    FormData fdDB = new FormData();
    fdDB.left = new FormAttachment(middle, 0);
    fdDB.top = new FormAttachment(wTransform, margin);
    fdDB.right = new FormAttachment(100, 0);
    wDB.setLayoutData(fdDB);

    // Search notes...
    wlNote = new Label(shell, SWT.RIGHT);
    wlNote.setText(BaseMessages.getString(PKG, "EnterSearchDialog.Note.Label"));
    props.setLook(wlNote);
    FormData fdlNote = new FormData();
    fdlNote.left = new FormAttachment(0, 0);
    fdlNote.top = new FormAttachment(wDB, margin);
    fdlNote.right = new FormAttachment(middle, -margin);
    wlNote.setLayoutData(fdlNote);
    wNote = new Button(shell, SWT.CHECK);
    props.setLook(wNote);
    wNote.setToolTipText(BaseMessages.getString(PKG, "EnterSearchDialog.Note.Tooltip"));
    FormData fdNote = new FormData();
    fdNote.left = new FormAttachment(middle, 0);
    fdNote.top = new FormAttachment(wDB, margin);
    fdNote.right = new FormAttachment(100, 0);
    wNote.setLayoutData(fdNote);

    // Filter line
    wlFilter = new Label(shell, SWT.RIGHT);
    wlFilter.setText(
        BaseMessages.getString(PKG, "EnterSearchDialog.FilterSelection.Label")); // Select filter
    props.setLook(wlFilter);
    FormData fdlFilter = new FormData();
    fdlFilter.left = new FormAttachment(0, 0);
    fdlFilter.right = new FormAttachment(middle, -margin);
    fdlFilter.top = new FormAttachment(wNote, 3 * margin);
    wlFilter.setLayoutData(fdlFilter);
    wFilter = new Text(shell, SWT.SINGLE | SWT.BORDER);
    props.setLook(wFilter);
    FormData fdFilter = new FormData();
    fdFilter.left = new FormAttachment(middle, 0);
    fdFilter.top = new FormAttachment(wNote, 3 * margin);
    fdFilter.right = new FormAttachment(100, 0);
    wFilter.setLayoutData(fdFilter);

    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            ok();
          }
        });

    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            cancel();
          }
        });

    BaseTransformDialog.positionBottomButtons(
        shell, new Button[] {wOk, wCancel}, props.getMargin(), wFilter);

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener(
        new ShellAdapter() {
          public void shellClosed(ShellEvent e) {
            cancel();
          }
        });
    SelectionAdapter lsDef =
        new SelectionAdapter() {
          public void widgetDefaultSelected(SelectionEvent e) {
            ok();
          }
        };

    wFilter.addSelectionListener(lsDef);

    getData();

    BaseTransformDialog.setSize(shell);

    shell.open();

    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }

    return retval;
  }

  private void getData() {
    wTransform.setSelection(searchingTransforms);
    wDB.setSelection(searchingDatabases);
    wNote.setSelection(searchingNotes);
    wFilter.setText(Const.NVL(filterString, ""));

    wFilter.setFocus();
  }

  public void dispose() {
    WindowProperty winprop = new WindowProperty(shell);
    props.setScreen(winprop);

    shell.dispose();
  }

  public void cancel() {
    retval = false;
    dispose();
  }

  public void ok() {
    retval = true;
    searchingTransforms = wTransform.getSelection();
    searchingDatabases = wDB.getSelection();
    searchingNotes = wNote.getSelection();
    filterString = wFilter.getText();

    dispose();
  }

  public boolean isSearchingTransforms() {
    return searchingTransforms;
  }

  public boolean isSearchingDatabases() {
    return searchingDatabases;
  }

  public boolean isSearchingNotes() {
    return searchingNotes;
  }

  /** @return Returns the filterString. */
  public String getFilterString() {
    return filterString;
  }

  /** @param filterString The filterString to set. */
  public void setFilterString(String filterString) {
    this.filterString = filterString;
  }
}
