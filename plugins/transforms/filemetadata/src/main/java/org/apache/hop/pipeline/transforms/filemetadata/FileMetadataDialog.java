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

package org.apache.hop.pipeline.transforms.filemetadata;

import static org.apache.hop.pipeline.transforms.filemetadata.FileMetadataMeta.FMCandidate;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

/** */
public class FileMetadataDialog extends BaseTransformDialog {

  /**
   * The PKG member is used when looking up internationalized strings. The properties file with
   * localized keys is expected to reside in {the package of the class
   * specified}/messages/messages_{locale}.properties
   */
  private static final Class<?> PKG = FileMetadataMeta.class;

  // this is the object the stores the transform's settings
  // the dialog reads the settings from it when opening
  // the dialog writes the settings to it when confirmed
  private final FileMetadataMeta meta;
  private Label wlFilename;
  private TextVar wFilename;

  private Button wFileInField;

  private Label wlFilenameField;
  private CCombo wFilenameField;

  private TableView wDelimiterCandidates;
  private TableView wEnclosureCandidates;
  private TextVar wLimit;
  private ComboVar wDefaultCharset;

  private boolean gotEncodings = false;
  private boolean getPreviousFields = false;

  /**
   * The constructor should simply invoke super() and save the incoming meta object to a local
   * variable, so it can conveniently read and write settings from/to it.
   *
   * @param parent the SWT shell to open the dialog in
   * @param variables in the meta object holding the transform's settings
   * @param pipelineMeta transformation description
   * @param transformMeta name the transform name
   */
  public FileMetadataDialog(
      Shell parent,
      IVariables variables,
      FileMetadataMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    meta = transformMeta;
  }

  private void setEncodings() {
    // Encoding of the text file:
    if (!gotEncodings) {
      gotEncodings = true;

      wDefaultCharset.removeAll();
      List<Charset> values = new ArrayList<>(Charset.availableCharsets().values());
      for (Charset charSet : values) {
        wDefaultCharset.add(charSet.displayName());
      }

      // Now select the default!
      String defEncoding = meta.getDefaultCharset();
      int idx = Const.indexOfString(defEncoding, wDefaultCharset.getItems());
      if (idx >= 0) {
        wDefaultCharset.select(idx);
      }
    }
  }

  /** */
  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "FileMetadata.Shell.Title"));

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    ModifyListener lsMod = e -> meta.setChanged();

    // OK and cancel buttons
    Control lastControl = wSpacer;

    // Filename...
    //
    // The filename browse button
    //
    Button wbbFilename = new Button(shell, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbbFilename);
    wbbFilename.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    wbbFilename.setToolTipText(
        BaseMessages.getString(PKG, "System.Tooltip.BrowseForFileOrDirAndAdd"));
    FormData fdbFilename = new FormData();
    fdbFilename.top = new FormAttachment(lastControl, margin);
    fdbFilename.right = new FormAttachment(100, 0);
    wbbFilename.setLayoutData(fdbFilename);

    // The field itself...
    //
    wlFilename = new Label(shell, SWT.RIGHT);
    wlFilename.setText(BaseMessages.getString(PKG, "FileMetadata.Filename"));
    PropsUi.setLook(wlFilename);
    FormData fdlFilename = new FormData();
    fdlFilename.top = new FormAttachment(lastControl, margin);
    fdlFilename.left = new FormAttachment(0, 0);
    fdlFilename.right = new FormAttachment(middle, -margin);
    wlFilename.setLayoutData(fdlFilename);
    wFilename = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFilename);
    FormData fdFilename = new FormData();
    fdFilename.top = new FormAttachment(lastControl, margin);
    fdFilename.left = new FormAttachment(middle, 0);
    fdFilename.right = new FormAttachment(wbbFilename, -margin);
    wFilename.setLayoutData(fdFilename);

    // Is Filename defined in a Field
    Label wlFileInField = new Label(shell, SWT.RIGHT);
    wlFileInField.setText(BaseMessages.getString(PKG, "FileMetadata.FileInField.Label"));
    PropsUi.setLook(wlFileInField);
    FormData fdlFileInField = new FormData();
    fdlFileInField.left = new FormAttachment(0, -margin);
    fdlFileInField.top = new FormAttachment(wFilename, margin);
    fdlFileInField.right = new FormAttachment(middle, -margin);
    wlFileInField.setLayoutData(fdlFileInField);

    wFileInField = new Button(shell, SWT.CHECK);
    PropsUi.setLook(wFileInField);
    wFileInField.setToolTipText(BaseMessages.getString(PKG, "FileMetadata.FileInField.Tooltip"));
    FormData fdFileField = new FormData();
    fdFileField.left = new FormAttachment(middle, -margin);
    fdFileField.top = new FormAttachment(wlFileInField, 0, SWT.CENTER);
    wFileInField.setLayoutData(fdFileField);
    SelectionAdapter lfilefield =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            activateFileField();

            meta.setChanged();
          }
        };
    wFileInField.addSelectionListener(lfilefield);

    // Filename field
    wlFilenameField = new Label(shell, SWT.RIGHT);
    wlFilenameField.setText(BaseMessages.getString(PKG, "FileMetadata.FilenameField.Label"));
    PropsUi.setLook(wlFilenameField);
    FormData fdlFilenameField = new FormData();
    fdlFilenameField.left = new FormAttachment(0, -margin);
    fdlFilenameField.top = new FormAttachment(wFileInField, margin);
    fdlFilenameField.right = new FormAttachment(middle, -margin);
    wlFilenameField.setLayoutData(fdlFilenameField);

    wFilenameField = new CCombo(shell, SWT.BORDER | SWT.READ_ONLY);
    wFilenameField.setEditable(true);
    PropsUi.setLook(wFilenameField);
    wFilenameField.addModifyListener(lsMod);
    FormData fdFilenameField = new FormData();
    fdFilenameField.left = new FormAttachment(middle, -margin);
    fdFilenameField.top = new FormAttachment(wFileInField, margin);
    fdFilenameField.right = new FormAttachment(100, -margin);
    wFilenameField.setLayoutData(fdFilenameField);
    wFilenameField.addFocusListener(
        new FocusListener() {
          @Override
          public void focusLost(FocusEvent e) {
            // Do Nothing
          }

          @Override
          public void focusGained(FocusEvent e) {
            Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
            shell.setCursor(busy);
            getFields();
            shell.setCursor(null);
            busy.dispose();
          }
        });

    // options panel for DELIMITED_LAYOUT
    Group gDelimitedLayout = new Group(shell, SWT.SHADOW_NONE);
    gDelimitedLayout.setText(
        BaseMessages.getString(PKG, "FileMetadata.DelimitedLayout.Group.Label"));
    FormLayout gDelimitedLayoutLayout = new FormLayout();
    gDelimitedLayoutLayout.marginWidth = 3;
    gDelimitedLayoutLayout.marginHeight = 3;
    gDelimitedLayout.setLayout(gDelimitedLayoutLayout);
    PropsUi.setLook(gDelimitedLayout);

    // Limit input ...
    Label wlLimit = new Label(gDelimitedLayout, SWT.RIGHT);
    wlLimit.setText(BaseMessages.getString(PKG, "FileMetadata.methods.DELIMITED_FIELDS.limit"));
    PropsUi.setLook(wlLimit);
    FormData fdlLimit = new FormData();
    fdlLimit.left = new FormAttachment(0, 0);
    fdlLimit.right = new FormAttachment(middle, -margin);
    fdlLimit.top = new FormAttachment(0, margin);
    wlLimit.setLayoutData(fdlLimit);
    wLimit = new TextVar(variables, gDelimitedLayout, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wLimit.setToolTipText(
        BaseMessages.getString(PKG, "FileMetadata.methods.DELIMITED_FIELDS.limit.tooltip"));
    PropsUi.setLook(wLimit);
    FormData fdLimit = new FormData();
    fdLimit.top = new FormAttachment(0, margin);
    fdLimit.left = new FormAttachment(middle, 0);
    fdLimit.right = new FormAttachment(100, 0);

    wLimit.setLayoutData(fdLimit);
    lastControl = wLimit;

    // Charset
    Label wlEncoding = new Label(gDelimitedLayout, SWT.RIGHT);
    wlEncoding.setText(
        BaseMessages.getString(PKG, "FileMetadata.methods.DELIMITED_FIELDS.default_charset"));
    PropsUi.setLook(wlEncoding);
    FormData fdlDefaultCharset = new FormData();
    fdlDefaultCharset.top = new FormAttachment(lastControl, margin);
    fdlDefaultCharset.left = new FormAttachment(0, 0);
    fdlDefaultCharset.right = new FormAttachment(middle, -margin);
    wlEncoding.setLayoutData(fdlDefaultCharset);
    wDefaultCharset = new ComboVar(variables, gDelimitedLayout, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wDefaultCharset);
    FormData fdDefaultCharset = new FormData();
    fdDefaultCharset.top = new FormAttachment(lastControl, margin);
    fdDefaultCharset.left = new FormAttachment(middle, 0);
    fdDefaultCharset.right = new FormAttachment(100, 0);
    wDefaultCharset.setLayoutData(fdDefaultCharset);

    wDefaultCharset.addListener(
        SWT.FocusIn,
        e -> {
          Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
          shell.setCursor(busy);
          setEncodings();
          shell.setCursor(null);
          busy.dispose();
        });

    int candidateCount = meta.getDelimiterCandidates().size();

    ColumnInfo[] delimiterColumns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(
                  PKG, "FileMetadata.methods.DELIMITED_FIELDS.delimiter_candidates"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false)
        };
    delimiterColumns[0].setUsingVariables(true);

    wDelimiterCandidates =
        new TableView(
            variables,
            gDelimitedLayout,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            delimiterColumns,
            candidateCount,
            null,
            props);
    FormData fdDelimiterCandidates = new FormData();
    fdDelimiterCandidates.left = new FormAttachment(0, 0);
    fdDelimiterCandidates.right = new FormAttachment(100, 0);
    fdDelimiterCandidates.top = new FormAttachment(wDefaultCharset, margin);
    fdDelimiterCandidates.bottom = new FormAttachment(50, 0);
    wDelimiterCandidates.setLayoutData(fdDelimiterCandidates);

    candidateCount = meta.getEnclosureCandidates().size();

    ColumnInfo[] enclosureColumns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(
                  PKG, "FileMetadata.methods.DELIMITED_FIELDS.enclosure_candidates"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false)
        };

    enclosureColumns[0].setUsingVariables(true);

    wEnclosureCandidates =
        new TableView(
            variables,
            gDelimitedLayout,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            enclosureColumns,
            candidateCount,
            null,
            props);
    FormData fdEnclosureCandidates = new FormData();
    fdEnclosureCandidates.left = new FormAttachment(0, 0);
    fdEnclosureCandidates.right = new FormAttachment(100, 0);
    fdEnclosureCandidates.top = new FormAttachment(50, margin);
    fdEnclosureCandidates.bottom = new FormAttachment(100, 0);
    wEnclosureCandidates.setLayoutData(fdEnclosureCandidates);

    FormData fdQueryGroup = new FormData();
    fdQueryGroup.left = new FormAttachment(0, 0);
    fdQueryGroup.right = new FormAttachment(100, 0);
    fdQueryGroup.top = new FormAttachment(wFilenameField, margin);
    fdQueryGroup.bottom = new FormAttachment(wOk, -margin);
    gDelimitedLayout.setLayoutData(fdQueryGroup);

    // populate the dialog with the values from the meta object
    populateDialog();

    // restore the changed flag to original value, as the modify listeners fire during dialog
    // population
    meta.setChanged(changed);

    // Listen to the browse button next to the file name
    wbbFilename.addListener(
        SWT.Selection,
        e ->
            BaseDialog.presentFileDialog(
                shell,
                wFilename,
                variables,
                new String[] {"*.txt;*.csv", "*.csv", "*.txt", "*"},
                new String[] {
                  BaseMessages.getString(PKG, "System.FileType.CSVFiles")
                      + ", "
                      + BaseMessages.getString(PKG, "System.FileType.TextFiles"),
                  BaseMessages.getString(PKG, "System.FileType.CSVFiles"),
                  BaseMessages.getString(PKG, "System.FileType.TextFiles"),
                  BaseMessages.getString(PKG, "System.FileType.AllFiles")
                },
                true));
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    // at this point the dialog has closed, so either ok() or cancel() have been executed
    // The "TransformName" variable is inherited from BaseTransformDialog
    return transformName;
  }

  private void activateFileField() {

    wlFilenameField.setEnabled(wFileInField.getSelection());
    wFilenameField.setEnabled(wFileInField.getSelection());

    wlFilename.setEnabled(!wFileInField.getSelection());
    wFilename.setEnabled(!wFileInField.getSelection());

    if (wFileInField.getSelection()) {
      wFilename.setText("");
    } else {
      wFilenameField.setText("");
    }
  }

  private void getFields() {
    try {
      if (!getPreviousFields) {
        getPreviousFields = true;

        wFilenameField.removeAll();

        IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
        if (r != null) {
          wFilenameField.setItems(r.getFieldNames());
        }
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "FileMetadata.FailedToGetFields.DialogTitle"),
          BaseMessages.getString(PKG, "FileMetadata.FailedToGetFields.DialogMessage"),
          ke);
    }
  }

  /**
   * This helper method puts the transform configuration stored in the meta object and puts it into
   * the dialog controls.
   */
  private void populateDialog() {
    wFilename.setText(Const.NVL(meta.getFileName(), ""));

    wFileInField.setSelection(meta.isFilenameInField());

    if (meta.getFilenameField() != null) {
      wFilenameField.setText(meta.getFilenameField());
    }
    wLimit.setText(Const.NVL(meta.getLimitRows(), ""));
    wDefaultCharset.setText(Const.NVL(meta.getDefaultCharset(), ""));

    for (int i = 0; i < meta.getDelimiterCandidates().size(); i++) {
      FMCandidate candidate = meta.getDelimiterCandidates().get(i);
      TableItem item = wDelimiterCandidates.table.getItem(i);
      item.setText(1, Const.NVL(candidate.getCandidate(), ""));
    }
    for (int i = 0; i < meta.getEnclosureCandidates().size(); i++) {
      FMCandidate candidate = meta.getEnclosureCandidates().get(i);
      TableItem item = wEnclosureCandidates.table.getItem(i);
      item.setText(1, Const.NVL(candidate.getCandidate(), ""));
    }
  }

  /** Called when the user cancels the dialog. */
  private void cancel() {
    // The "TransformName" variable will be the return value for the open() method.
    // Setting to null to indicate that dialog was cancelled.
    transformName = null;
    // close the SWT dialog window
    dispose();
  }

  /** Called when the user confirms the dialog */
  private void ok() {
    // The "TransformName" variable will be the return value for the open() method.
    // Setting to transform name from the dialog control
    transformName = wTransformName.getText();

    meta.setFileName(wFilename.getText());
    meta.setFilenameInField(wFileInField.getSelection());
    meta.setFilenameField(wFilenameField.getText());
    meta.setLimitRows(wLimit.getText());
    meta.setDefaultCharset(wDefaultCharset.getText());

    // delimiter candidates
    meta.getDelimiterCandidates().clear();
    for (TableItem item : wDelimiterCandidates.getNonEmptyItems()) {
      meta.getDelimiterCandidates().add(new FMCandidate(item.getText(1)));
    }

    // enclosure candidates
    meta.getEnclosureCandidates().clear();
    for (TableItem item : wEnclosureCandidates.getNonEmptyItems()) {
      meta.getEnclosureCandidates().add(new FMCandidate(item.getText(1)));
    }

    // close the SWT dialog window
    dispose();
  }
}
