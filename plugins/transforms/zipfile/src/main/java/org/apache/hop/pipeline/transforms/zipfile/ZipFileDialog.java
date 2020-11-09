/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.pipeline.transforms.zipfile;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

public class ZipFileDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = ZipFileMeta.class; // Needed by Translator

  private CCombo wSourceFileNameField;

  private CCombo wTargetFileNameField;

  private Button wAddResult;

  private Button wOverwriteZipEntry;

  private Button wCreateParentFolder;

  private Button wKeepFolders;

  private final ZipFileMeta input;

  private Label wlBaseFolderField;
  private CCombo wBaseFolderField;

  private CCombo wOperation;

  private Label wlMoveToFolderField;
  private CCombo wMoveToFolderField;

  private boolean gotPreviousFields = false;

  public ZipFileDialog( Shell parent, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (ZipFileMeta) in;
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    ModifyListener lsMod = e -> input.setChanged();

    SelectionAdapter lsSel = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        input.setChanged();

      }
    };

    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "ZipFileDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // TransformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "ZipFileDialog.TransformName.Label" ) );
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

    // ///////////////////////////////
    // START OF Settings GROUP //
    // ///////////////////////////////

    Group wSettingsGroup = new Group(shell, SWT.SHADOW_NONE);
    props.setLook(wSettingsGroup);
    wSettingsGroup.setText( BaseMessages.getString( PKG, "ZipFileDialog.wSettingsGroup.Label" ) );

    FormLayout settingGroupLayout = new FormLayout();
    settingGroupLayout.marginWidth = 10;
    settingGroupLayout.marginHeight = 10;
    wSettingsGroup.setLayout( settingGroupLayout );

    // Create target parent folder?
    Label wlCreateParentFolder = new Label(wSettingsGroup, SWT.RIGHT);
    wlCreateParentFolder.setText( BaseMessages.getString( PKG, "ZipFileDialog.CreateParentFolder.Label" ) );
    props.setLook(wlCreateParentFolder);
    FormData fdlCreateParentFolder = new FormData();
    fdlCreateParentFolder.left = new FormAttachment( 0, 0 );
    fdlCreateParentFolder.top = new FormAttachment( wTransformName, margin );
    fdlCreateParentFolder.right = new FormAttachment( middle, -margin );
    wlCreateParentFolder.setLayoutData(fdlCreateParentFolder);
    wCreateParentFolder = new Button(wSettingsGroup, SWT.CHECK );
    props.setLook( wCreateParentFolder );
    wCreateParentFolder.setToolTipText( BaseMessages.getString( PKG, "ZipFileDialog.CreateParentFolder.Tooltip" ) );
    FormData fdCreateParentFolder = new FormData();
    fdCreateParentFolder.left = new FormAttachment( middle, 0 );
    fdCreateParentFolder.top = new FormAttachment( wlCreateParentFolder, 0, SWT.CENTER );
    wCreateParentFolder.setLayoutData(fdCreateParentFolder);
    wCreateParentFolder.addSelectionListener( lsSel );

    // Overwrite target file?
    Label wlOverwriteTarget = new Label(wSettingsGroup, SWT.RIGHT);
    wlOverwriteTarget.setText( BaseMessages.getString( PKG, "ZipFileDialog.OverwriteTarget.Label" ) );
    props.setLook(wlOverwriteTarget);
    FormData fdlOverwriteTarget = new FormData();
    fdlOverwriteTarget.left = new FormAttachment( 0, 0 );
    fdlOverwriteTarget.top = new FormAttachment( wCreateParentFolder, margin );
    fdlOverwriteTarget.right = new FormAttachment( middle, -margin );
    wlOverwriteTarget.setLayoutData(fdlOverwriteTarget);
    wOverwriteZipEntry = new Button(wSettingsGroup, SWT.CHECK );
    props.setLook( wOverwriteZipEntry );
    wOverwriteZipEntry.setToolTipText( BaseMessages.getString( PKG, "ZipFileDialog.OverwriteTarget.Tooltip" ) );
    FormData fdOverwriteTarget = new FormData();
    fdOverwriteTarget.left = new FormAttachment( middle, 0 );
    fdOverwriteTarget.top = new FormAttachment( wlOverwriteTarget, 0, SWT.CENTER );
    wOverwriteZipEntry.setLayoutData(fdOverwriteTarget);
    wOverwriteZipEntry.addSelectionListener( lsSel );

    // Add Target filename to result filenames?
    Label wlAddResult = new Label(wSettingsGroup, SWT.RIGHT);
    wlAddResult.setText( BaseMessages.getString( PKG, "ZipFileDialog.AddResult.Label" ) );
    props.setLook(wlAddResult);
    FormData fdlAddResult = new FormData();
    fdlAddResult.left = new FormAttachment( 0, 0 );
    fdlAddResult.top = new FormAttachment( wOverwriteZipEntry, margin );
    fdlAddResult.right = new FormAttachment( middle, -margin );
    wlAddResult.setLayoutData(fdlAddResult);
    wAddResult = new Button(wSettingsGroup, SWT.CHECK );
    props.setLook( wAddResult );
    wAddResult.setToolTipText( BaseMessages.getString( PKG, "ZipFileDialog.AddResult.Tooltip" ) );
    FormData fdAddResult = new FormData();
    fdAddResult.left = new FormAttachment( middle, 0 );
    fdAddResult.top = new FormAttachment( wlAddResult, 0, SWT.CENTER );
    wAddResult.setLayoutData(fdAddResult);
    wAddResult.addSelectionListener( lsSel );

    FormData fdSettingsGroup = new FormData();
    fdSettingsGroup.left = new FormAttachment( 0, margin );
    fdSettingsGroup.top = new FormAttachment( wTransformName, margin );
    fdSettingsGroup.right = new FormAttachment( 100, -margin );
    wSettingsGroup.setLayoutData(fdSettingsGroup);

    // ///////////////////////////////
    // END OF Settings Fields GROUP //
    // ///////////////////////////////

    // SourceFileNameField field
    Label wlSourceFileNameField = new Label(shell, SWT.RIGHT);
    wlSourceFileNameField.setText( BaseMessages.getString( PKG, "ZipFileDialog.SourceFileNameField.Label" ) );
    props.setLook(wlSourceFileNameField);
    FormData fdlSourceFileNameField = new FormData();
    fdlSourceFileNameField.left = new FormAttachment( 0, 0 );
    fdlSourceFileNameField.right = new FormAttachment( middle, -margin );
    fdlSourceFileNameField.top = new FormAttachment(wSettingsGroup, 2 * margin );
    wlSourceFileNameField.setLayoutData(fdlSourceFileNameField);

    wSourceFileNameField = new CCombo( shell, SWT.BORDER | SWT.READ_ONLY );
    props.setLook( wSourceFileNameField );
    wSourceFileNameField.setEditable( true );
    wSourceFileNameField.addModifyListener( lsMod );
    FormData fdSourceFileNameField = new FormData();
    fdSourceFileNameField.left = new FormAttachment( middle, 0 );
    fdSourceFileNameField.top = new FormAttachment(wSettingsGroup, 2 * margin );
    fdSourceFileNameField.right = new FormAttachment( 100, -margin );
    wSourceFileNameField.setLayoutData(fdSourceFileNameField);
    wSourceFileNameField.addFocusListener( new FocusListener() {
      public void focusLost( org.eclipse.swt.events.FocusEvent e ) {
      }

      public void focusGained( org.eclipse.swt.events.FocusEvent e ) {
        get();
      }
    } );
    // TargetFileNameField field
    Label wlTargetFileNameField = new Label(shell, SWT.RIGHT);
    wlTargetFileNameField.setText( BaseMessages.getString( PKG, "ZipFileDialog.TargetFileNameField.Label" ) );
    props.setLook(wlTargetFileNameField);
    FormData fdlTargetFileNameField = new FormData();
    fdlTargetFileNameField.left = new FormAttachment( 0, 0 );
    fdlTargetFileNameField.right = new FormAttachment( middle, -margin );
    fdlTargetFileNameField.top = new FormAttachment( wSourceFileNameField, margin );
    wlTargetFileNameField.setLayoutData(fdlTargetFileNameField);

    wTargetFileNameField = new CCombo( shell, SWT.BORDER | SWT.READ_ONLY );
    wTargetFileNameField.setEditable( true );
    props.setLook( wTargetFileNameField );
    wTargetFileNameField.addModifyListener( lsMod );
    FormData fdTargetFileNameField = new FormData();
    fdTargetFileNameField.left = new FormAttachment( middle, 0 );
    fdTargetFileNameField.top = new FormAttachment( wSourceFileNameField, margin );
    fdTargetFileNameField.right = new FormAttachment( 100, -margin );
    wTargetFileNameField.setLayoutData(fdTargetFileNameField);
    wTargetFileNameField.addFocusListener( new FocusListener() {
      public void focusLost( org.eclipse.swt.events.FocusEvent e ) {
      }

      public void focusGained( org.eclipse.swt.events.FocusEvent e ) {
        get();
      }
    } );

    Label wlKeepFolders = new Label(shell, SWT.RIGHT);
    wlKeepFolders.setText( BaseMessages.getString( PKG, "ZipFileDialog.KeepFolders.Label" ) );
    props.setLook(wlKeepFolders);
    FormData fdlKeepFolders = new FormData();
    fdlKeepFolders.left = new FormAttachment( 0, 0 );
    fdlKeepFolders.top = new FormAttachment( wTargetFileNameField, margin );
    fdlKeepFolders.right = new FormAttachment( middle, -margin );
    wlKeepFolders.setLayoutData(fdlKeepFolders);
    wKeepFolders = new Button( shell, SWT.CHECK );
    props.setLook( wKeepFolders );
    wKeepFolders.setToolTipText( BaseMessages.getString( PKG, "ZipFileDialog.KeepFolders.Tooltip" ) );
    FormData fdKeepFolders = new FormData();
    fdKeepFolders.left = new FormAttachment( middle, 0 );
    fdKeepFolders.top = new FormAttachment( wTargetFileNameField, margin );
    wKeepFolders.setLayoutData(fdKeepFolders);
    wKeepFolders.addSelectionListener( lsSel );
    wKeepFolders.addSelectionListener( new SelectionAdapter() {

      @Override
      public void widgetSelected( SelectionEvent arg0 ) {
        keepFolder();

      }

    } );

    // BaseFolderField field
    wlBaseFolderField = new Label( shell, SWT.RIGHT );
    wlBaseFolderField.setText( BaseMessages.getString( PKG, "ZipFileDialog.BaseFolderField.Label" ) );
    props.setLook( wlBaseFolderField );
    FormData fdlBaseFolderField = new FormData();
    fdlBaseFolderField.left = new FormAttachment( 0, 0 );
    fdlBaseFolderField.right = new FormAttachment( middle, -margin );
    fdlBaseFolderField.top = new FormAttachment( wKeepFolders, margin );
    wlBaseFolderField.setLayoutData(fdlBaseFolderField);

    wBaseFolderField = new CCombo( shell, SWT.BORDER | SWT.READ_ONLY );
    wBaseFolderField.setEditable( true );
    props.setLook( wBaseFolderField );
    wBaseFolderField.addModifyListener( lsMod );
    FormData fdBaseFolderField = new FormData();
    fdBaseFolderField.left = new FormAttachment( middle, 0 );
    fdBaseFolderField.top = new FormAttachment( wKeepFolders, margin );
    fdBaseFolderField.right = new FormAttachment( 100, -margin );
    wBaseFolderField.setLayoutData(fdBaseFolderField);
    wBaseFolderField.addFocusListener( new FocusListener() {
      public void focusLost( org.eclipse.swt.events.FocusEvent e ) {
      }

      public void focusGained( org.eclipse.swt.events.FocusEvent e ) {
        get();
      }
    } );

    // Operation
    Label wlOperation = new Label(shell, SWT.RIGHT);
    wlOperation.setText( BaseMessages.getString( PKG, "ZipFileDialog.Operation.Label" ) );
    props.setLook(wlOperation);
    FormData fdlOperation = new FormData();
    fdlOperation.left = new FormAttachment( 0, 0 );
    fdlOperation.right = new FormAttachment( middle, -margin );
    fdlOperation.top = new FormAttachment( wBaseFolderField, margin );
    wlOperation.setLayoutData(fdlOperation);

    wOperation = new CCombo( shell, SWT.BORDER | SWT.READ_ONLY );
    props.setLook( wOperation );
    wOperation.addModifyListener( lsMod );
    FormData fdOperation = new FormData();
    fdOperation.left = new FormAttachment( middle, 0 );
    fdOperation.top = new FormAttachment( wBaseFolderField, margin );
    fdOperation.right = new FormAttachment( 100, -margin );
    wOperation.setLayoutData(fdOperation);
    wOperation.setItems( ZipFileMeta.operationTypeDesc );
    wOperation.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        updateOperation();

      }
    } );

    // MoveToFolderField field
    wlMoveToFolderField = new Label( shell, SWT.RIGHT );
    wlMoveToFolderField.setText( BaseMessages.getString( PKG, "ZipFileDialog.MoveToFolderField.Label" ) );
    props.setLook( wlMoveToFolderField );
    FormData fdlMoveToFolderField = new FormData();
    fdlMoveToFolderField.left = new FormAttachment( 0, 0 );
    fdlMoveToFolderField.right = new FormAttachment( middle, -margin );
    fdlMoveToFolderField.top = new FormAttachment( wOperation, margin );
    wlMoveToFolderField.setLayoutData(fdlMoveToFolderField);

    wMoveToFolderField = new CCombo( shell, SWT.BORDER | SWT.READ_ONLY );
    wMoveToFolderField.setEditable( true );
    props.setLook( wMoveToFolderField );
    wMoveToFolderField.addModifyListener( lsMod );
    FormData fdMoveToFolderField = new FormData();
    fdMoveToFolderField.left = new FormAttachment( middle, 0 );
    fdMoveToFolderField.top = new FormAttachment( wOperation, margin );
    fdMoveToFolderField.right = new FormAttachment( 100, -margin );
    wMoveToFolderField.setLayoutData(fdMoveToFolderField);
    wMoveToFolderField.addFocusListener( new FocusListener() {
      public void focusLost( org.eclipse.swt.events.FocusEvent e ) {
      }

      public void focusGained( org.eclipse.swt.events.FocusEvent e ) {
        get();
      }
    } );

    // THE BUTTONS
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOk, wCancel }, margin, wMoveToFolderField );

    // Add listeners
    lsOk = e -> ok();

    lsCancel = e -> cancel();

    wOk.addListener( SWT.Selection, lsOk );
    wCancel.addListener( SWT.Selection, lsCancel );

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
    keepFolder();
    updateOperation();
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
    if ( log.isDebug() ) {
      log.logDebug( toString(), BaseMessages.getString( PKG, "ZipFileDialog.Log.GettingKeyInfo" ) );
    }
    if ( input.getBaseFolderField() != null ) {
      wBaseFolderField.setText( input.getBaseFolderField() );
    }
    if ( input.getDynamicSourceFileNameField() != null ) {
      wSourceFileNameField.setText( input.getDynamicSourceFileNameField() );
    }
    if ( input.getDynamicTargetFileNameField() != null ) {
      wTargetFileNameField.setText( input.getDynamicTargetFileNameField() );
    }
    wOperation.setText( ZipFileMeta.getOperationTypeDesc( input.getOperationType() ) );
    if ( input.getMoveToFolderField() != null ) {
      wMoveToFolderField.setText( input.getMoveToFolderField() );
    }

    wAddResult.setSelection( input.isaddTargetFileNametoResult() );
    wOverwriteZipEntry.setSelection( input.isOverwriteZipEntry() );
    wCreateParentFolder.setSelection( input.isCreateParentFolder() );
    wKeepFolders.setSelection( input.isKeepSouceFolder() );

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
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "System.Error.TransformNameMissing.Message" ) );
      mb.setText( BaseMessages.getString( PKG, "System.Error.TransformNameMissing.Title" ) );
      mb.open();
      return;
    }
    input.setBaseFolderField( wBaseFolderField.getText() );
    input.setDynamicSourceFileNameField( wSourceFileNameField.getText() );
    input.setDynamicTargetFileNameField( wTargetFileNameField.getText() );
    input.setaddTargetFileNametoResult( wAddResult.getSelection() );
    input.setOverwriteZipEntry( wOverwriteZipEntry.getSelection() );
    input.setCreateParentFolder( wCreateParentFolder.getSelection() );
    input.setKeepSouceFolder( wKeepFolders.getSelection() );
    input.setOperationType( ZipFileMeta.getOperationTypeByDesc( wOperation.getText() ) );
    input.setMoveToFolderField( wMoveToFolderField.getText() );
    transformName = wTransformName.getText(); // return value

    dispose();
  }

  private void keepFolder() {
    wlBaseFolderField.setEnabled( wKeepFolders.getSelection() );
    wBaseFolderField.setEnabled( wKeepFolders.getSelection() );
  }

  private void get() {
    if ( !gotPreviousFields ) {
      gotPreviousFields = true;
      String source = wSourceFileNameField.getText();
      String target = wTargetFileNameField.getText();
      String base = wBaseFolderField.getText();

      try {

        wSourceFileNameField.removeAll();
        wTargetFileNameField.removeAll();
        wBaseFolderField.removeAll();
        IRowMeta r = pipelineMeta.getPrevTransformFields( transformName );
        if ( r != null ) {
          String[] fields = r.getFieldNames();
          wSourceFileNameField.setItems( fields );
          wTargetFileNameField.setItems( fields );
          wBaseFolderField.setItems( fields );
        }
      } catch ( HopException ke ) {
        new ErrorDialog(
          shell, BaseMessages.getString( PKG, "ZipFileDialog.FailedToGetFields.DialogTitle" ), BaseMessages
          .getString( PKG, "ZipFileDialog.FailedToGetFields.DialogMessage" ), ke );
      } finally {
        if ( source != null ) {
          wSourceFileNameField.setText( source );
        }
        if ( target != null ) {
          wTargetFileNameField.setText( target );
        }
        if ( base != null ) {
          wBaseFolderField.setText( base );
        }
      }
    }
  }

  private void updateOperation() {
    wlMoveToFolderField
      .setEnabled( ZipFileMeta.getOperationTypeByDesc( wOperation.getText() ) == ZipFileMeta.OPERATION_TYPE_MOVE );
    wMoveToFolderField
      .setEnabled( ZipFileMeta.getOperationTypeByDesc( wOperation.getText() ) == ZipFileMeta.OPERATION_TYPE_MOVE );

  }
}
