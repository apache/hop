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

package org.apache.hop.pipeline.transforms.changefileencoding;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transform.ComponentSelectionListener;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

import java.nio.charset.Charset;
import java.util.ArrayList;

public class ChangeFileEncodingDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = ChangeFileEncodingDialog.class; // For Translator

  private CCombo wFileName;

  private CCombo wTargetFileName;

  private ComboVar wTargetEncoding;

  private ComboVar wSourceEncoding;

  private Button wSourceAddResult;

  private Button wTargetAddResult;

  private Button wCreateParentFolder;

  private final ChangeFileEncodingMeta input;

  private boolean gotPreviousFields = false;

  public ChangeFileEncodingDialog( Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (ChangeFileEncodingMeta) in;
  }

  @Override
  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    ModifyListener lsMod = e -> input.setChanged();

    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "ChangeFileEncodingDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // Buttons at the very bottom
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOk.addListener( SWT.Selection, e -> ok() );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, e -> cancel() );
    setButtonPositions( new Button[] { wOk, wCancel }, margin, null);


    // TransformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "ChangeFileEncodingDialog.TransformName.Label" ) );
    props.setLook( wlTransformName );
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment( 0, 0 );
    fdlTransformName.right = new FormAttachment( middle, -margin );
    fdlTransformName.top = new FormAttachment( 0, margin );
    wlTransformName.setLayoutData( fdlTransformName );
    wTransformName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wTransformName.setText( transformName );
    props.setLook( wTransformName );
    wTransformName.addModifyListener( lsMod );
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment( middle, 0 );
    fdTransformName.top = new FormAttachment( 0, margin );
    fdTransformName.right = new FormAttachment( 100, 0 );
    wTransformName.setLayoutData( fdTransformName );

    // /////////////////////////////////
    // START OF SourceFile GROUP
    // /////////////////////////////////

    Group wSourceFileGroup = new Group(shell, SWT.SHADOW_NONE);
    props.setLook(wSourceFileGroup);
    wSourceFileGroup.setText( BaseMessages.getString( PKG, "ChangeFileEncodingDialog.Group.SourceFileGroup.Label" ) );

    FormLayout SourceFilegroupLayout = new FormLayout();
    SourceFilegroupLayout.marginWidth = 10;
    SourceFilegroupLayout.marginHeight = 10;
    wSourceFileGroup.setLayout( SourceFilegroupLayout );

    // filename field
    Label wlFileName = new Label(wSourceFileGroup, SWT.RIGHT);
    wlFileName.setText( BaseMessages.getString( PKG, "ChangeFileEncodingDialog.FileName.Label" ) );
    props.setLook(wlFileName);
    FormData fdlFileName = new FormData();
    fdlFileName.left = new FormAttachment( 0, 0 );
    fdlFileName.right = new FormAttachment( middle, -margin );
    fdlFileName.top = new FormAttachment( wTransformName, margin );
    wlFileName.setLayoutData(fdlFileName);

    wFileName = new CCombo(wSourceFileGroup, SWT.BORDER | SWT.READ_ONLY );
    wFileName.setEditable( true );
    props.setLook( wFileName );
    wFileName.addModifyListener( lsMod );
    FormData fdfileName = new FormData();
    fdfileName.left = new FormAttachment( middle, 0 );
    fdfileName.top = new FormAttachment( wTransformName, margin );
    fdfileName.right = new FormAttachment( 100, -margin );
    wFileName.setLayoutData(fdfileName);
    wFileName.addListener( SWT.FocusIn, e->get() );

    Label wlSourceEncoding = new Label(wSourceFileGroup, SWT.RIGHT);
    wlSourceEncoding.setText( BaseMessages.getString( PKG, "ChangeFileEncodingDialog.SourceEncoding.Label" ) );
    props.setLook(wlSourceEncoding);
    FormData fdlSourceEncoding = new FormData();
    fdlSourceEncoding.left = new FormAttachment( 0, 0 );
    fdlSourceEncoding.top = new FormAttachment( wFileName, margin );
    fdlSourceEncoding.right = new FormAttachment( middle, -margin );
    wlSourceEncoding.setLayoutData(fdlSourceEncoding);
    wSourceEncoding = new ComboVar( variables, wSourceFileGroup, SWT.BORDER | SWT.READ_ONLY );
    wSourceEncoding.setEditable( true );
    props.setLook( wSourceEncoding );
    wSourceEncoding.addModifyListener( lsMod );
    FormData fdSourceEncoding = new FormData();
    fdSourceEncoding.left = new FormAttachment( middle, 0 );
    fdSourceEncoding.top = new FormAttachment( wFileName, margin );
    fdSourceEncoding.right = new FormAttachment( 100, 0 );
    wSourceEncoding.setLayoutData(fdSourceEncoding);
    wSourceEncoding.addListener( SWT.FocusIn, e-> setEncodings( wSourceEncoding ));

    // Add filename to result filenames?
    Label wlSourceAddResult = new Label(wSourceFileGroup, SWT.RIGHT);
    wlSourceAddResult.setText( BaseMessages.getString( PKG, "ChangeFileEncodingDialog.AddSourceResult.Label" ) );
    props.setLook(wlSourceAddResult);
    FormData fdlSourceAddResult = new FormData();
    fdlSourceAddResult.left = new FormAttachment( 0, 0 );
    fdlSourceAddResult.top = new FormAttachment( wSourceEncoding, margin );
    fdlSourceAddResult.right = new FormAttachment( middle, -margin );
    wlSourceAddResult.setLayoutData(fdlSourceAddResult);
    wSourceAddResult = new Button(wSourceFileGroup, SWT.CHECK );
    props.setLook( wSourceAddResult );
    wSourceAddResult.setToolTipText( BaseMessages.getString( PKG, "ChangeFileEncodingDialog.AddSourceResult.Tooltip" ) );
    FormData fdSourceAddResult = new FormData();
    fdSourceAddResult.left = new FormAttachment( middle, 0 );
    fdSourceAddResult.top = new FormAttachment( wlSourceAddResult, 0, SWT.CENTER );
    wSourceAddResult.setLayoutData(fdSourceAddResult);
    wSourceAddResult.addSelectionListener( new ComponentSelectionListener( input ) );

    FormData fdSourceFileGroup = new FormData();
    fdSourceFileGroup.left = new FormAttachment( 0, margin );
    fdSourceFileGroup.top = new FormAttachment( wTransformName, margin );
    fdSourceFileGroup.right = new FormAttachment( 100, -margin );
    wSourceFileGroup.setLayoutData(fdSourceFileGroup);

    // ///////////////////////////////////////////////////////////
    // / END OF SourceFile GROUP
    // ///////////////////////////////////////////////////////////

    // /////////////////////////////////
    // START OF TargetFile GROUP
    // /////////////////////////////////

    Group wTargetFileGroup = new Group(shell, SWT.SHADOW_NONE);
    props.setLook(wTargetFileGroup);
    wTargetFileGroup.setText( BaseMessages.getString( PKG, "ChangeFileEncodingDialog.Group.TargetFileGroup.Label" ) );

    FormLayout TargetFilegroupLayout = new FormLayout();
    TargetFilegroupLayout.marginWidth = 10;
    TargetFilegroupLayout.marginHeight = 10;
    wTargetFileGroup.setLayout( TargetFilegroupLayout );

    // TargetFileName field
    Label wlTargetFileName = new Label(wTargetFileGroup, SWT.RIGHT);
    wlTargetFileName.setText( BaseMessages.getString( PKG, "ChangeFileEncodingDialog.TargetFileName.Label" ) );
    props.setLook(wlTargetFileName);
    FormData fdlTargetFileName = new FormData();
    fdlTargetFileName.left = new FormAttachment( 0, 0 );
    fdlTargetFileName.right = new FormAttachment( middle, -margin );
    fdlTargetFileName.top = new FormAttachment( wSourceEncoding, margin );
    wlTargetFileName.setLayoutData(fdlTargetFileName);

    wTargetFileName = new CCombo(wTargetFileGroup, SWT.BORDER | SWT.READ_ONLY );
    wTargetFileName.setEditable( true );
    props.setLook( wTargetFileName );
    wTargetFileName.addModifyListener( lsMod );
    FormData fdTargetFileName = new FormData();
    fdTargetFileName.left = new FormAttachment( middle, 0 );
    fdTargetFileName.top = new FormAttachment( wSourceEncoding, margin );
    fdTargetFileName.right = new FormAttachment( 100, -margin );
    wTargetFileName.setLayoutData(fdTargetFileName);
    wTargetFileName.addFocusListener( new FocusListener() {
      public void focusLost( FocusEvent e ) {
      }

      public void focusGained( FocusEvent e ) {
        get();
      }
    } );

    // Create parent folder
    Label wlCreateParentFolder = new Label(wTargetFileGroup, SWT.RIGHT);
    wlCreateParentFolder.setText( BaseMessages.getString( PKG, "ChangeFileEncodingDialog.CreateParentFolder.Label" ) );
    props.setLook(wlCreateParentFolder);
    FormData fdlCreateParentFolder = new FormData();
    fdlCreateParentFolder.left = new FormAttachment( 0, 0 );
    fdlCreateParentFolder.top = new FormAttachment( wTargetFileName, margin );
    fdlCreateParentFolder.right = new FormAttachment( middle, -margin );
    wlCreateParentFolder.setLayoutData(fdlCreateParentFolder);
    wCreateParentFolder = new Button(wTargetFileGroup, SWT.CHECK );
    props.setLook( wCreateParentFolder );
    wCreateParentFolder.setToolTipText( BaseMessages.getString( PKG, "ChangeFileEncodingDialog.CreateParentFolder.Tooltip" ) );
    FormData fdCreateParentFolder = new FormData();
    fdCreateParentFolder.left = new FormAttachment( middle, 0 );
    fdCreateParentFolder.top = new FormAttachment( wlCreateParentFolder, 0, SWT.CENTER );
    wCreateParentFolder.setLayoutData(fdCreateParentFolder);
    wCreateParentFolder.addSelectionListener( new ComponentSelectionListener( input ) );

    Label wlTargetEncoding = new Label(wTargetFileGroup, SWT.RIGHT);
    wlTargetEncoding.setText( BaseMessages.getString( PKG, "ChangeFileEncodingDialog.TargetEncoding.Label" ) );
    props.setLook(wlTargetEncoding);
    FormData fdlTargetEncoding = new FormData();
    fdlTargetEncoding.left = new FormAttachment( 0, 0 );
    fdlTargetEncoding.top = new FormAttachment( wCreateParentFolder, margin );
    fdlTargetEncoding.right = new FormAttachment( middle, -margin );
    wlTargetEncoding.setLayoutData(fdlTargetEncoding);
    wTargetEncoding = new ComboVar( variables, wTargetFileGroup, SWT.BORDER | SWT.READ_ONLY );
    wTargetEncoding.setEditable( true );
    props.setLook( wTargetEncoding );
    wTargetEncoding.addModifyListener( lsMod );
    FormData fdTargetEncoding = new FormData();
    fdTargetEncoding.left = new FormAttachment( middle, 0 );
    fdTargetEncoding.top = new FormAttachment( wCreateParentFolder, margin );
    fdTargetEncoding.right = new FormAttachment( 100, 0 );
    wTargetEncoding.setLayoutData(fdTargetEncoding);
    wTargetEncoding.addListener( SWT.FocusIn, e-> setEncodings( wTargetEncoding ));

    // Add filename to result filenames?
    Label wlTargetAddResult = new Label(wTargetFileGroup, SWT.RIGHT);
    wlTargetAddResult.setText( BaseMessages.getString( PKG, "ChangeFileEncodingDialog.AddTargetResult.Label" ) );
    props.setLook(wlTargetAddResult);
    FormData fdlTargetAddResult = new FormData();
    fdlTargetAddResult.left = new FormAttachment( 0, 0 );
    fdlTargetAddResult.top = new FormAttachment( wTargetEncoding, margin );
    fdlTargetAddResult.right = new FormAttachment( middle, -margin );
    wlTargetAddResult.setLayoutData(fdlTargetAddResult);
    wTargetAddResult = new Button(wTargetFileGroup, SWT.CHECK );
    props.setLook( wTargetAddResult );
    wTargetAddResult
      .setToolTipText( BaseMessages.getString( PKG, "ChangeFileEncodingDialog.AddTargetResult.Tooltip" ) );
    FormData fdTargetAddResult = new FormData();
    fdTargetAddResult.left = new FormAttachment( middle, 0 );
    fdTargetAddResult.top = new FormAttachment( wlTargetAddResult, 0, SWT.CENTER );
    wTargetAddResult.setLayoutData(fdTargetAddResult);
    wTargetAddResult.addSelectionListener( new ComponentSelectionListener( input ) );

    FormData fdTargetFileGroup = new FormData();
    fdTargetFileGroup.left = new FormAttachment( 0, margin );
    fdTargetFileGroup.top = new FormAttachment(wSourceFileGroup, margin );
    fdTargetFileGroup.right = new FormAttachment( 100, -margin );
    fdTargetFileGroup.bottom = new FormAttachment(wOk, -2*margin);
    wTargetFileGroup.setLayoutData(fdTargetFileGroup);

    // ///////////////////////////////////////////////////////////
    // / END OF TargetFile GROUP
    // ///////////////////////////////////////////////////////////


    // Add listeners

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wTransformName.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    // Set the shell size, based upon previous time...
    setSize();

    getData();

    input.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformName;
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    if ( isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "ChangeFileEncodingDialog.Log.GettingKeyInfo" ) );
    }

    if ( input.getDynamicFilenameField() != null ) {
      wFileName.setText( input.getDynamicFilenameField() );
    }
    if ( input.getTargetFilenameField() != null ) {
      wTargetFileName.setText( input.getTargetFilenameField() );
    }
    if ( input.getTargetEncoding() != null ) {
      wTargetEncoding.setText( input.getTargetEncoding() );
    }
    if ( input.getSourceEncoding() != null ) {
      wSourceEncoding.setText( input.getSourceEncoding() );
    }

    wSourceAddResult.setSelection( input.addSourceResultFilenames() );
    wTargetAddResult.setSelection( input.addSourceResultFilenames() );
    wCreateParentFolder.setSelection( input.isCreateParentFolder() );

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    input.setChanged( changed );
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( wTransformName.getText() ) ) {
      return;
    }
    input.setDynamicFilenameField( wFileName.getText() );
    input.setTargetFilenameField( wTargetFileName.getText() );
    input.setSourceEncoding( wSourceEncoding.getText() );
    input.setTargetEncoding( wTargetEncoding.getText() );
    input.setaddSourceResultFilenames( wSourceAddResult.getSelection() );
    input.setaddTargetResultFilenames( wTargetAddResult.getSelection() );
    input.setCreateParentFolder( wCreateParentFolder.getSelection() );

    transformName = wTransformName.getText(); // return value

    dispose();
  }

  private void get() {
    if ( !gotPreviousFields ) {
      try {
        String filefield = wFileName.getText();
        String targetfilefield = wTargetFileName.getText();
        wFileName.removeAll();
        wTargetFileName.removeAll();
        IRowMeta r = pipelineMeta.getPrevTransformFields( variables, transformName );
        if ( r != null ) {
          wFileName.setItems( r.getFieldNames() );
          wTargetFileName.setItems( r.getFieldNames() );
        }
        if ( filefield != null ) {
          wFileName.setText( filefield );
        }
        if ( targetfilefield != null ) {
          wTargetFileName.setText( targetfilefield );
        }
      } catch ( HopException ke ) {
        new ErrorDialog( shell, BaseMessages.getString( PKG, "ChangeFileEncodingDialog.FailedToGetFields.DialogTitle" ),
          BaseMessages.getString( PKG, "ChangeFileEncodingDialog.FailedToGetFields.DialogMessage" ), ke );
      }
      gotPreviousFields = true;
    }
  }

  private void setEncodings( ComboVar var ) {
    // Encoding of the text file:
    String encoding = Const.NVL( var.getText(), Const.getEnvironmentVariable( "file.encoding", "UTF-8" ) );
    var.removeAll();
    ArrayList<Charset> values = new ArrayList<>(Charset.availableCharsets().values());
    for (Charset charSet : values) {
      var.add(charSet.displayName());
    }

    // Now select the default!
    int idx = Const.indexOfString( encoding, var.getItems() );
    if ( idx >= 0 ) {
      var.select( idx );
    }
  }
}
