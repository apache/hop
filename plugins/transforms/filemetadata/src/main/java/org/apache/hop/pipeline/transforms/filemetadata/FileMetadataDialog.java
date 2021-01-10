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

package org.apache.hop.pipeline.transforms.filemetadata;

import org.apache.hop.core.Const;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 */
public class FileMetadataDialog extends BaseTransformDialog implements ITransformDialog {

  /**
   * The PKG member is used when looking up internationalized strings.
   * The properties file with localized keys is expected to reside in
   * {the package of the class specified}/messages/messages_{locale}.properties
   */
  private static final Class<?> PKG = FileMetadataMeta.class; // For Translator

  // this is the object the stores the transform's settings
  // the dialog reads the settings from it when opening
  // the dialog writes the settings to it when confirmed
  private FileMetadataMeta meta;

  private TextVar wFilename;

  private TableView wDelimiterCandidates;
  private TableView wEnclosureCandidates;
  private TextVar wLimit;
  private ComboVar wDefaultCharset;

  private boolean gotEncodings = false;

  /**
   * The constructor should simply invoke super() and save the incoming meta
   * object to a local variable, so it can conveniently read and write settings
   * from/to it.
   *
   * @param parent    the SWT shell to open the dialog in
   * @param in        the meta object holding the transform's settings
   * @param pipelineMeta transformation description
   * @param sname     the transform name
   */
  public FileMetadataDialog( Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname) {
    super(parent, variables, (BaseTransformMeta) in, pipelineMeta, sname );
    meta = (FileMetadataMeta) in;
  }

//  private final String[] emptyFieldList = new String[0];
//
//  private String[] getFieldListForCombo() {
//    String[] items;
//    try {
//      RowMetaInterface r = pipelineMeta.getPrevTransformFields(TransformName);
//      items = r.getFieldNames();
//    } catch (KettleException exception) {
//      items = emptyFieldList;
//    }
//    return items;
//  }


  private void setEncodings() {
    // Encoding of the text file:
    if ( !gotEncodings ) {
      gotEncodings = true;

      wDefaultCharset.removeAll();
      List<Charset> values = new ArrayList<>( Charset.availableCharsets().values() );
      for ( int i = 0; i < values.size(); i++ ) {
        Charset charSet = values.get( i );
        wDefaultCharset.add(charSet.displayName());
      }

      // Now select the default!
      String defEncoding = meta.getDefaultCharset();
      int idx = Const.indexOfString(defEncoding, wDefaultCharset.getItems());
      if ( idx >= 0 ) {
        wDefaultCharset.select(idx);
      }
    }
  }

  /**
   */
  public String open() {

    // store some convenient SWT variables
    Shell parent = getParent();
    Display display = parent.getDisplay();

    // SWT code for preparing the dialog
    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
    props.setLook(shell);
    setShellImage(shell, meta);

    // Save the value of the changed flag on the meta object. If the user cancels
    // the dialog, it will be restored to this saved value.
    // The "changed" variable is inherited from BaseTransformDialog
    changed = meta.hasChanged();

    // The ModifyListener used on all controls. It will update the meta object to
    // indicate that changes are being made.
    ModifyListener lsMod = new ModifyListener() {
      public void modifyText(ModifyEvent e) {
        meta.setChanged();
      }
    };

    // default listener (for hitting "enter")
    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected(SelectionEvent e) {
        ok();
      }
    };



    // ------------------------------------------------------- //
    // SWT code for building the actual settings dialog        //
    // ------------------------------------------------------- //
    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "FileMetadata.Shell.Title"));

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // OK and cancel buttons
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    BaseTransformDialog.positionBottomButtons(shell, new Button[]{wOk, wCancel}, margin, null);


    // TransformName line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "System.Label.TransformName"));
    props.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    fdlTransformName.top = new FormAttachment(0, margin);
    wlTransformName.setLayoutData(fdlTransformName);

    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    props.setLook(wTransformName);
    wTransformName.addModifyListener(lsMod);
    wTransformName.addSelectionListener(lsDef);

    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.top = new FormAttachment(0, margin);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);

    Control lastControl = wTransformName;

    // Filename...
    //
    // The filename browse button
    //
    Button wbbFilename = new Button(shell, SWT.PUSH | SWT.CENTER);
    props.setLook(wbbFilename);
    wbbFilename.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    wbbFilename.setToolTipText(BaseMessages.getString(PKG, "System.Tooltip.BrowseForFileOrDirAndAdd"));
    FormData fdbFilename = new FormData();
    fdbFilename.top = new FormAttachment(lastControl, margin);
    fdbFilename.right = new FormAttachment(100, 0);
    wbbFilename.setLayoutData(fdbFilename);

    // The field itself...
    //
    Label wlFilename = new Label(shell, SWT.RIGHT);
    wlFilename.setText(BaseMessages.getString(PKG, "FileMetadata.Filename"));
    props.setLook(wlFilename);
    FormData fdlFilename = new FormData();
    fdlFilename.top = new FormAttachment(lastControl, margin);
    fdlFilename.left = new FormAttachment(0, 0);
    fdlFilename.right = new FormAttachment(middle, -margin);
    wlFilename.setLayoutData(fdlFilename);
    wFilename = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wFilename);
    wFilename.addModifyListener(lsMod);
    FormData fdFilename = new FormData();
    fdFilename.top = new FormAttachment(lastControl, margin);
    fdFilename.left = new FormAttachment(middle, 0);
    fdFilename.right = new FormAttachment(wbbFilename, -margin);
    wFilename.setLayoutData(fdFilename);
    wFilename.addSelectionListener(lsDef);

    lastControl = wFilename;

    // options panel for DELIMITED_LAYOUT
    Group gDelimitedLayout = new Group(shell, SWT.SHADOW_ETCHED_IN);
    gDelimitedLayout.setText("Delimited Layout");
    FormLayout gDelimitedLayoutLayout = new FormLayout();
    gDelimitedLayoutLayout.marginWidth = 3;
    gDelimitedLayoutLayout.marginHeight = 3;
    gDelimitedLayout.setLayout(gDelimitedLayoutLayout);
    props.setLook(gDelimitedLayout);

    // Limit input ...
    Label wlLimit = new Label(gDelimitedLayout, SWT.RIGHT);
    wlLimit.setText( BaseMessages.getString( PKG, "FileMetadata.methods.DELIMITED_FIELDS.limit" ) );
    props.setLook( wlLimit );
    FormData fdlLimit = new FormData();
    fdlLimit.left = new FormAttachment( 0, 0 );
    fdlLimit.right = new FormAttachment( middle, -margin );
    fdlLimit.top = new FormAttachment( 0, margin );
    wlLimit.setLayoutData( fdlLimit );
    wLimit = new TextVar( variables, gDelimitedLayout, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wLimit.setToolTipText( BaseMessages.getString( PKG, "FileMetadata.methods.DELIMITED_FIELDS.limit.tooltip" ) );
    props.setLook( wLimit );
    wLimit.addModifyListener( lsMod );
    FormData fdLimit = new FormData();
    fdLimit.top = new FormAttachment(0,margin);
    fdLimit.left = new FormAttachment( middle, 0 );
    fdLimit.right = new FormAttachment( 100, 0 );

    wLimit.setLayoutData( fdLimit );
    lastControl = wLimit;

    // Charset
    Label wlEncoding = new Label( gDelimitedLayout, SWT.RIGHT );
    wlEncoding.setText( BaseMessages.getString( PKG, "FileMetadata.methods.DELIMITED_FIELDS.default_charset" ));
    props.setLook( wlEncoding );
    FormData fdlDefaultCharset = new FormData();
    fdlDefaultCharset.top = new FormAttachment( lastControl, margin );
    fdlDefaultCharset.left = new FormAttachment( 0, 0 );
    fdlDefaultCharset.right = new FormAttachment( middle, -margin );
    wlEncoding.setLayoutData( fdlDefaultCharset );
    wDefaultCharset = new ComboVar( variables, gDelimitedLayout, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook(wDefaultCharset);
    wDefaultCharset.addModifyListener(lsMod);
    FormData fdDefaultCharset = new FormData();
    fdDefaultCharset.top = new FormAttachment( lastControl, margin );
    fdDefaultCharset.left = new FormAttachment( middle, 0 );
    fdDefaultCharset.right = new FormAttachment( 100, 0 );
    wDefaultCharset.setLayoutData(fdDefaultCharset);
    lastControl = wDefaultCharset;

    wDefaultCharset.addFocusListener(new FocusListener() {
      public void focusLost(org.eclipse.swt.events.FocusEvent e) {
      }

      public void focusGained(org.eclipse.swt.events.FocusEvent e) {
        Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
        shell.setCursor(busy);
        setEncodings();
        shell.setCursor(null);
        busy.dispose();
      }
    });

    int candidateCount = meta.getDelimiterCandidates().size();

    ColumnInfo[] colinf = new ColumnInfo[]{
        new ColumnInfo(BaseMessages.getString(PKG, "FileMetadata.methods.DELIMITED_FIELDS.delimiter_candidates"), ColumnInfo.COLUMN_TYPE_TEXT, false)
    };

    colinf[0].setUsingVariables(true);

    wDelimiterCandidates = new TableView(variables, gDelimitedLayout, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, candidateCount, lsMod, props);
    FormData fdDelimiterCandidates = new FormData();
    fdDelimiterCandidates.left = new FormAttachment(0, 0);
    fdDelimiterCandidates.right = new FormAttachment(100, 0);
    fdDelimiterCandidates.top = new FormAttachment(wDefaultCharset, margin);
    fdDelimiterCandidates.bottom = new FormAttachment(50, 0);
    wDelimiterCandidates.setLayoutData(fdDelimiterCandidates);

    candidateCount = meta.getEnclosureCandidates().size();

    colinf = new ColumnInfo[]{
        new ColumnInfo(BaseMessages.getString(PKG, "FileMetadata.methods.DELIMITED_FIELDS.enclosure_candidates"), ColumnInfo.COLUMN_TYPE_TEXT, false)
    };

    colinf[0].setUsingVariables(true);

    wEnclosureCandidates = new TableView(variables, gDelimitedLayout, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, candidateCount, lsMod, props);
    FormData fdEnclosureCandidates = new FormData();
    fdEnclosureCandidates.left = new FormAttachment(0, 0);
    fdEnclosureCandidates.right = new FormAttachment(100, 0);
    fdEnclosureCandidates.top = new FormAttachment(50, margin);
    fdEnclosureCandidates.bottom = new FormAttachment(100, 0);
    wEnclosureCandidates.setLayoutData(fdEnclosureCandidates);

    FormData fdQueryGroup = new FormData();
    fdQueryGroup.left = new FormAttachment(0, 0);
    fdQueryGroup.right = new FormAttachment(100, 0);
    fdQueryGroup.top = new FormAttachment(wFilename, margin);
    fdQueryGroup.bottom = new FormAttachment(wOk, -2*margin);
    gDelimitedLayout.setLayoutData(fdQueryGroup);

    lastControl = gDelimitedLayout;


    // Add listeners for cancel and OK


    // Detect X or ALT-F4 or something that kills this window and cancel the dialog properly
    shell.addShellListener(new ShellAdapter() {
      public void shellClosed(ShellEvent e) {
        cancel();
      }
    });

    // Set/Restore the dialog size based on last position on screen
    // The setSize() method is inherited from BaseTransformDialog
    setSize();

    // populate the dialog with the values from the meta object
    populateDialog();

    // restore the changed flag to original value, as the modify listeners fire during dialog population
    meta.setChanged(changed);

    // Listen to the browse button next to the file name
    wbbFilename.addSelectionListener(new SelectionAdapter() {
      public void widgetSelected(SelectionEvent e) {
        FileDialog dialog = new FileDialog(shell, SWT.OPEN);
        dialog.setFilterExtensions(new String[]{"*.txt;*.csv", "*.csv", "*.txt", "*"});
        if (wFilename.getText() != null) {
          String fileName = variables.resolve(wFilename.getText());
          dialog.setFileName(fileName);
        }

        dialog.setFilterNames(new String[]{
            BaseMessages.getString(PKG, "System.FileType.CSVFiles") + ", "
                + BaseMessages.getString(PKG, "System.FileType.TextFiles"),
            BaseMessages.getString(PKG, "System.FileType.CSVFiles"),
            BaseMessages.getString(PKG, "System.FileType.TextFiles"),
            BaseMessages.getString(PKG, "System.FileType.AllFiles")});

        if (dialog.open() != null) {
          String str = dialog.getFilterPath() + System.getProperty("file.separator") + dialog.getFileName();
          wFilename.setText(str);
        }
      }
    });

    // open dialog and enter event loop
    shell.open();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch())
        display.sleep();
    }

    // at this point the dialog has closed, so either ok() or cancel() have been executed
    // The "TransformName" variable is inherited from BaseTransformDialog
    return transformName;
  }

  /**
   * This helper method puts the transform configuration stored in the meta object
   * and puts it into the dialog controls.
   */
  private void populateDialog() {
    wTransformName.selectAll();

    if (meta.getFileName() != null) {
      wFilename.setText(meta.getFileName());
    }

    if (meta.getLimitRows() != null) {
      wLimit.setText(meta.getLimitRows());
    }

    if (meta.getDefaultCharset() != null) {
      wDefaultCharset.setText(meta.getDefaultCharset());
    }

    if (meta.getDelimiterCandidates() != null) {
      for (int i = 0; i < meta.getDelimiterCandidates().size(); i++) {
        String candidate = meta.getDelimiterCandidates().get(i);
        TableItem item = wDelimiterCandidates.table.getItem(i);
        item.setText(1, Const.NVL(candidate, ""));
      }
    }

    if (meta.getEnclosureCandidates() != null) {
      for (int i = 0; i < meta.getEnclosureCandidates().size(); i++) {
        String candidate = meta.getEnclosureCandidates().get(i);
        TableItem item = wEnclosureCandidates.table.getItem(i);
        item.setText(1, Const.NVL(candidate, ""));
      }
    }

  }

  /**
   * Called when the user cancels the dialog.
   */
  private void cancel() {
    // The "TransformName" variable will be the return value for the open() method.
    // Setting to null to indicate that dialog was cancelled.
    transformName = null;
    // Restoring original "changed" flag on the meta object
    meta.setChanged(changed);
    // close the SWT dialog window
    dispose();
  }

  /**
   * Called when the user confirms the dialog
   */
  private void ok() {
    // The "TransformName" variable will be the return value for the open() method.
    // Setting to transform name from the dialog control
    transformName = wTransformName.getText();

    meta.setFileName(wFilename.getText());
    meta.setLimitRows(wLimit.getText());
    meta.setDefaultCharset(wDefaultCharset.getText());

    // delimiter candidates
    ArrayList<String> candidates = meta.getDelimiterCandidates();
    candidates.clear();
    int nrItems = wDelimiterCandidates.nrNonEmpty();

    for (int i = 0; i < nrItems; i++) {
      TableItem item = wDelimiterCandidates.getNonEmpty(i);
      candidates.add(item.getText(1));
    }

    // enclosure candidates
    candidates = meta.getEnclosureCandidates();
    candidates.clear();
    nrItems = wEnclosureCandidates.nrNonEmpty();

    for (int i = 0; i < nrItems; i++) {
      TableItem item = wEnclosureCandidates.getNonEmpty(i);
      candidates.add(item.getText(1));
    }

    // close the SWT dialog window
    dispose();
  }
}
