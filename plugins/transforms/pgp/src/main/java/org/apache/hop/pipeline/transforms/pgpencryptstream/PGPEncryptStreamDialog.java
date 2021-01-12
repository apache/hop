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

package org.apache.hop.pipeline.transforms.pgpencryptstream;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.*;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

public class PGPEncryptStreamDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = PGPEncryptStreamMeta.class; // For Translator
  private boolean gotPreviousFields = false;

  private TextVar wGPGLocation;

  private Label wlKeyName;
  private TextVar wKeyName;

  private CCombo wStreamFieldName;

  private TextVar wResult;
  private final PGPEncryptStreamMeta input;

  private Button wKeyNameFromField;

  private Label wlKeyNameFieldName;
  private CCombo wKeyNameFieldName;

  public PGPEncryptStreamDialog( Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (PGPEncryptStreamMeta) in;
  }

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
    shell.setText( BaseMessages.getString( PKG, "PGPEncryptStreamDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // THE BUTTONS at the bottom
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOk.addListener( SWT.Selection, e -> ok() );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, e -> cancel() );
    setButtonPositions( new Button[] { wOk, wCancel }, margin, null );

    // TransformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "PGPEncryptStreamDialog.TransformName.Label" ) );
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
    // START OF GPG Fields GROUP //
    // ///////////////////////////////

    Group wGPGGroup = new Group(shell, SWT.SHADOW_NONE);
    props.setLook(wGPGGroup);
    wGPGGroup.setText( BaseMessages.getString( PKG, "PGPEncryptStreamDialog.GPGGroup.Label" ) );

    FormLayout GPGGroupgroupLayout = new FormLayout();
    GPGGroupgroupLayout.marginWidth = 10;
    GPGGroupgroupLayout.marginHeight = 10;
    wGPGGroup.setLayout( GPGGroupgroupLayout );

    // GPGLocation fieldname ...
    Label wlGPGLocation = new Label(wGPGGroup, SWT.RIGHT);
    wlGPGLocation.setText( BaseMessages.getString( PKG, "PGPEncryptStreamDialog.GPGLocationField.Label" ) );
    props.setLook(wlGPGLocation);
    FormData fdlGPGLocation = new FormData();
    fdlGPGLocation.left = new FormAttachment( 0, 0 );
    fdlGPGLocation.right = new FormAttachment( middle, -margin );
    fdlGPGLocation.top = new FormAttachment( wTransformName, margin * 2 );
    wlGPGLocation.setLayoutData(fdlGPGLocation);

    // Browse Source files button ...
    Button wbbGpgExe = new Button(wGPGGroup, SWT.PUSH | SWT.CENTER);
    props.setLook(wbbGpgExe);
    wbbGpgExe.setText( BaseMessages.getString( PKG, "PGPEncryptStreamDialog.BrowseFiles.Label" ) );
    FormData fdbbGpgExe = new FormData();
    fdbbGpgExe.right = new FormAttachment( 100, -margin );
    fdbbGpgExe.top = new FormAttachment( wTransformName, margin );
    wbbGpgExe.setLayoutData(fdbbGpgExe);


    if ( wbbGpgExe != null ) {
      // Listen to the browse button next to the file name
      //
      wbbGpgExe.addListener( SWT.Selection, e-> BaseDialog.presentFileDialog( shell, wGPGLocation, variables,
              new String[] {"*" },
              new String[] {
                      BaseMessages.getString( PKG, "System.FileType.AllFiles" ) },
              true )
      );
    }

    wGPGLocation = new TextVar( variables, wGPGGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wGPGLocation.setToolTipText( BaseMessages.getString( PKG, "PGPEncryptStreamDialog.GPGLocationField.Tooltip" ) );
    props.setLook( wGPGLocation );
    wGPGLocation.addModifyListener( lsMod );
    FormData fdGPGLocation = new FormData();
    fdGPGLocation.left = new FormAttachment( middle, 0 );
    fdGPGLocation.top = new FormAttachment( wTransformName, margin * 2 );
    fdGPGLocation.right = new FormAttachment(wbbGpgExe, -margin );
    wGPGLocation.setLayoutData(fdGPGLocation);

    // KeyName fieldname ...
    wlKeyName = new Label(wGPGGroup, SWT.RIGHT );
    wlKeyName.setText( BaseMessages.getString( PKG, "PGPEncryptStreamDialog.KeyNameField.Label" ) );
    props.setLook( wlKeyName );
    FormData fdlKeyName = new FormData();
    fdlKeyName.left = new FormAttachment( 0, 0 );
    fdlKeyName.right = new FormAttachment( middle, -margin );
    fdlKeyName.top = new FormAttachment( wGPGLocation, margin );
    wlKeyName.setLayoutData(fdlKeyName);

    wKeyName = new TextVar( variables, wGPGGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wKeyName.setToolTipText( BaseMessages.getString( PKG, "PGPEncryptStreamDialog.KeyNameField.Tooltip" ) );
    props.setLook( wKeyName );
    wKeyName.addModifyListener( lsMod );
    FormData fdKeyName = new FormData();
    fdKeyName.left = new FormAttachment( middle, 0 );
    fdKeyName.top = new FormAttachment( wGPGLocation, margin );
    fdKeyName.right = new FormAttachment( 100, 0 );
    wKeyName.setLayoutData(fdKeyName);

    Label wlKeyNameFromField = new Label(wGPGGroup, SWT.RIGHT);
    wlKeyNameFromField.setText( BaseMessages.getString( PKG, "PGPEncryptStreamDialog.KeyNameFromField.Label" ) );
    props.setLook(wlKeyNameFromField);
    FormData fdlKeyNameFromField = new FormData();
    fdlKeyNameFromField.left = new FormAttachment( 0, 0 );
    fdlKeyNameFromField.top = new FormAttachment( wKeyName, margin );
    fdlKeyNameFromField.right = new FormAttachment( middle, -margin );
    wlKeyNameFromField.setLayoutData(fdlKeyNameFromField);
    wKeyNameFromField = new Button(wGPGGroup, SWT.CHECK );
    props.setLook( wKeyNameFromField );
    wKeyNameFromField.setToolTipText( BaseMessages.getString( PKG, "PGPEncryptStreamDialog.KeyNameFromField.Tooltip" ) );
    FormData fdKeyNameFromField = new FormData();
    fdKeyNameFromField.left = new FormAttachment( middle, 0 );
    fdKeyNameFromField.top = new FormAttachment( wlKeyNameFromField, 0, SWT.CENTER );
    wKeyNameFromField.setLayoutData(fdKeyNameFromField);

    wKeyNameFromField.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        keyNameFromField();
        input.setChanged();
      }
    } );

    // Stream field
    wlKeyNameFieldName = new Label(wGPGGroup, SWT.RIGHT );
    wlKeyNameFieldName.setText( BaseMessages.getString( PKG, "PGPEncryptStreamDialog.KeyNameFieldName.Label" ) );
    props.setLook( wlKeyNameFieldName );
    FormData fdlKeyNameFieldName = new FormData();
    fdlKeyNameFieldName.left = new FormAttachment( 0, 0 );
    fdlKeyNameFieldName.right = new FormAttachment( middle, -margin );
    fdlKeyNameFieldName.top = new FormAttachment( wKeyNameFromField, margin );
    wlKeyNameFieldName.setLayoutData(fdlKeyNameFieldName);

    wKeyNameFieldName = new CCombo(wGPGGroup, SWT.BORDER | SWT.READ_ONLY );
    props.setLook( wKeyNameFieldName );
    wKeyNameFieldName.addModifyListener( lsMod );
    FormData fdKeyNameFieldName = new FormData();
    fdKeyNameFieldName.left = new FormAttachment( middle, 0 );
    fdKeyNameFieldName.top = new FormAttachment( wKeyNameFromField, margin );
    fdKeyNameFieldName.right = new FormAttachment( 100, -margin );
    wKeyNameFieldName.setLayoutData(fdKeyNameFieldName);
    wKeyNameFieldName.addFocusListener( new FocusListener() {
      public void focusLost( FocusEvent e ) {
      }

      public void focusGained( FocusEvent e ) {
        Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
        shell.setCursor( busy );
        get();
        shell.setCursor( null );
        busy.dispose();
      }
    } );

    FormData fdGPGGroup = new FormData();
    fdGPGGroup.left = new FormAttachment( 0, margin );
    fdGPGGroup.top = new FormAttachment( wTransformName, margin );
    fdGPGGroup.right = new FormAttachment( 100, -margin );
    wGPGGroup.setLayoutData(fdGPGGroup);

    // ///////////////////////////////
    // END OF GPG GROUP //
    // ///////////////////////////////

    // Stream field
    Label wlStreamFieldName = new Label(shell, SWT.RIGHT);
    wlStreamFieldName.setText( BaseMessages.getString( PKG, "PGPEncryptStreamDialog.StreamFieldName.Label" ) );
    props.setLook(wlStreamFieldName);
    FormData fdlStreamFieldName = new FormData();
    fdlStreamFieldName.left = new FormAttachment( 0, 0 );
    fdlStreamFieldName.right = new FormAttachment( middle, -margin );
    fdlStreamFieldName.top = new FormAttachment(wGPGGroup, 2 * margin );
    wlStreamFieldName.setLayoutData(fdlStreamFieldName);

    wStreamFieldName = new CCombo( shell, SWT.BORDER | SWT.READ_ONLY );
    props.setLook( wStreamFieldName );
    wStreamFieldName.addModifyListener( lsMod );
    FormData fdStreamFieldName = new FormData();
    fdStreamFieldName.left = new FormAttachment( middle, 0 );
    fdStreamFieldName.top = new FormAttachment(wGPGGroup, 2 * margin );
    fdStreamFieldName.right = new FormAttachment( 100, -margin );
    wStreamFieldName.setLayoutData(fdStreamFieldName);
    wStreamFieldName.addFocusListener( new FocusListener() {
      public void focusLost( FocusEvent e ) {
      }

      public void focusGained( FocusEvent e ) {
        Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
        shell.setCursor( busy );
        get();
        shell.setCursor( null );
        busy.dispose();
      }
    } );

    // Result fieldname ...
    Label wlResult = new Label(shell, SWT.RIGHT);
    wlResult.setText( BaseMessages.getString( PKG, "PGPEncryptStreamDialog.ResultField.Label" ) );
    props.setLook(wlResult);
    FormData fdlResult = new FormData();
    fdlResult.left = new FormAttachment( 0, 0 );
    fdlResult.right = new FormAttachment( middle, -margin );
    fdlResult.top = new FormAttachment( wStreamFieldName, margin * 2 );
    wlResult.setLayoutData(fdlResult);

    wResult = new TextVar( variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wResult.setToolTipText( BaseMessages.getString( PKG, "PGPEncryptStreamDialog.ResultField.Tooltip" ) );
    props.setLook( wResult );
    wResult.addModifyListener( lsMod );
    FormData fdResult = new FormData();
    fdResult.left = new FormAttachment( middle, 0 );
    fdResult.top = new FormAttachment( wStreamFieldName, margin * 2 );
    fdResult.right = new FormAttachment( 100, 0 );
    wResult.setLayoutData(fdResult);

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
    keyNameFromField();
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
    if ( input.getGPGLocation() != null ) {
      wGPGLocation.setText( input.getGPGLocation() );
    }
    if ( input.getStreamField() != null ) {
      wStreamFieldName.setText( input.getStreamField() );
    }
    if ( input.getResultFieldName() != null ) {
      wResult.setText( input.getResultFieldName() );
    }
    if ( input.getKeyName() != null ) {
      wKeyName.setText( input.getKeyName() );
    }
    wKeyNameFromField.setSelection( input.isKeynameInField() );
    if ( input.getKeynameFieldName() != null ) {
      wKeyNameFieldName.setText( input.getKeynameFieldName() );
    }

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
    input.setStreamField( wStreamFieldName.getText() );
    input.setGPGPLocation( wGPGLocation.getText() );
    input.setKeyName( wKeyName.getText() );
    input.setResultFieldName( wResult.getText() );
    input.setKeynameInField( wKeyNameFromField.getSelection() );
    input.setKeynameFieldName( wKeyNameFieldName.getText() );
    transformName = wTransformName.getText(); // return value

    dispose();
  }

  private void keyNameFromField() {
    wlKeyName.setEnabled( !wKeyNameFromField.getSelection() );
    wKeyName.setEnabled( !wKeyNameFromField.getSelection() );
    wlKeyNameFieldName.setEnabled( wKeyNameFromField.getSelection() );
    wKeyNameFieldName.setEnabled( wKeyNameFromField.getSelection() );
  }

  private void get() {
    if ( !gotPreviousFields ) {
      try {
        String fieldvalue = wStreamFieldName.getText();
        wStreamFieldName.removeAll();
        String Keyfieldvalue = wKeyNameFieldName.getText();
        wKeyNameFieldName.removeAll();
        IRowMeta r = pipelineMeta.getPrevTransformFields( variables, transformName );
        if ( r != null ) {
          wStreamFieldName.setItems( r.getFieldNames() );
          wKeyNameFieldName.setItems( r.getFieldNames() );
        }
        if ( fieldvalue != null ) {
          wStreamFieldName.setText( fieldvalue );
        }
        if ( Keyfieldvalue != null ) {
          wKeyNameFieldName.setText( Keyfieldvalue );
        }
        gotPreviousFields = true;
      } catch ( HopException ke ) {
        new ErrorDialog( shell,
          BaseMessages.getString( PKG, "PGPEncryptStreamDialog.FailedToGetFields.DialogTitle" ),
          BaseMessages.getString( PKG, "PGPEncryptStreamDialog.FailedToGetFields.DialogMessage" ), ke );
      }
    }
  }
}
