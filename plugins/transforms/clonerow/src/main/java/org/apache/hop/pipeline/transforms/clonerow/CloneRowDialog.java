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

package org.apache.hop.pipeline.transforms.clonerow;

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

public class CloneRowDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = CloneRowDialog.class; // For Translator

  private final CloneRowMeta input;

  // nr clones
  private Label wlnrClone;
  private TextVar wnrClone;

  private Label wlcloneFlagField;
  private Label wlCloneNumField;
  private TextVar wcloneFlagField, wCloneNumField;

  private Button waddCloneFlag, waddCloneNum;

  private Label wlNrCloneField;
  private CCombo wNrCloneField;

  private Button wIsNrCloneInField;

  private boolean gotPreviousFields = false;

  public CloneRowDialog( Shell parent, IVariables variables, Object in, PipelineMeta tr, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, tr, sname );
    input = (CloneRowMeta) in;
  }

  @Override
  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX );
    props.setLook( shell );
    setShellImage( shell, input );

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "CloneRowDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // Some buttons
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOk.addListener( SWT.Selection, e -> ok() );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, e -> cancel() );
    setButtonPositions( new Button[] { wOk, wCancel }, margin, null);

    // TransformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "CloneRowDialog.TransformName.Label" ) );
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

    // Number of clones line
    wlnrClone = new Label( shell, SWT.RIGHT );
    wlnrClone.setText( BaseMessages.getString( PKG, "CloneRowDialog.nrClone.Label" ) );
    props.setLook( wlnrClone );
    FormData fdlnrClone = new FormData();
    fdlnrClone.left = new FormAttachment( 0, 0 );
    fdlnrClone.right = new FormAttachment( middle, -margin );
    fdlnrClone.top = new FormAttachment( wTransformName, margin * 2 );
    wlnrClone.setLayoutData(fdlnrClone);

    wnrClone = new TextVar( variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wnrClone );
    wnrClone.setToolTipText( BaseMessages.getString( PKG, "CloneRowDialog.nrClone.Tooltip" ) );
    wnrClone.addModifyListener( lsMod );
    FormData fdnrClone = new FormData();
    fdnrClone.left = new FormAttachment( middle, 0 );
    fdnrClone.top = new FormAttachment(wlnrClone, 0, SWT.CENTER );
    fdnrClone.right = new FormAttachment( 100, 0 );
    wnrClone.setLayoutData(fdnrClone);

    // Is Nr clones defined in a Field
    Label wlIsNrCloneInField = new Label(shell, SWT.RIGHT);
    wlIsNrCloneInField.setText( BaseMessages.getString( PKG, "CloneRowDialog.isNrCloneInField.Label" ) );
    props.setLook(wlIsNrCloneInField);
    FormData fdlisNrCloneInField = new FormData();
    fdlisNrCloneInField.left = new FormAttachment( 0, 0 );
    fdlisNrCloneInField.top = new FormAttachment( wnrClone, margin );
    fdlisNrCloneInField.right = new FormAttachment( middle, -margin );
    wlIsNrCloneInField.setLayoutData(fdlisNrCloneInField);

    wIsNrCloneInField = new Button( shell, SWT.CHECK );
    props.setLook( wIsNrCloneInField );
    wIsNrCloneInField.setToolTipText( BaseMessages.getString( PKG, "CloneRowDialog.isNrCloneInField.Tooltip" ) );
    FormData fdisNrCloneInField = new FormData();
    fdisNrCloneInField.left = new FormAttachment( middle, 0 );
    fdisNrCloneInField.top = new FormAttachment( wlIsNrCloneInField, 0, SWT.CENTER );
    wIsNrCloneInField.setLayoutData(fdisNrCloneInField);
    SelectionAdapter lisNrCloneInField = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        ActiveisNrCloneInField();
        input.setChanged();
      }
    };
    wIsNrCloneInField.addSelectionListener( lisNrCloneInField );

    // Filename field
    wlNrCloneField = new Label( shell, SWT.RIGHT );
    wlNrCloneField.setText( BaseMessages.getString( PKG, "CloneRowDialog.wlNrCloneField.Label" ) );
    props.setLook( wlNrCloneField );
    FormData fdlNrCloneField = new FormData();
    fdlNrCloneField.left = new FormAttachment( 0, 0 );
    fdlNrCloneField.top = new FormAttachment( wIsNrCloneInField, margin );
    fdlNrCloneField.right = new FormAttachment( middle, -margin );
    wlNrCloneField.setLayoutData(fdlNrCloneField);

    wNrCloneField = new CCombo( shell, SWT.BORDER | SWT.READ_ONLY );
    wNrCloneField.setEditable( true );
    props.setLook( wNrCloneField );
    wNrCloneField.addModifyListener( lsMod );
    FormData fdNrCloneField = new FormData();
    fdNrCloneField.left = new FormAttachment( middle, 0 );
    fdNrCloneField.top = new FormAttachment( wlNrCloneField, 0, SWT.CENTER );
    fdNrCloneField.right = new FormAttachment( 100, 0 );
    wNrCloneField.setLayoutData(fdNrCloneField);
    wNrCloneField.addFocusListener( new FocusListener() {
      public void focusLost( FocusEvent e ) {
      }

      public void focusGained( FocusEvent e ) {
        Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
        shell.setCursor( busy );
        setisNrCloneInField();
        shell.setCursor( null );
        busy.dispose();
      }
    } );

    // ///////////////////////////////
    // START OF Origin files GROUP //
    // ///////////////////////////////

    Group wOutpuFields = new Group(shell, SWT.SHADOW_NONE);
    props.setLook(wOutpuFields);
    wOutpuFields.setText( BaseMessages.getString( PKG, "CloneRowDialog.wOutpuFields.Label" ) );

    FormLayout OutpuFieldsgroupLayout = new FormLayout();
    OutpuFieldsgroupLayout.marginWidth = 10;
    OutpuFieldsgroupLayout.marginHeight = 10;
    wOutpuFields.setLayout( OutpuFieldsgroupLayout );

    // add clone flag?
    Label wladdCloneFlag = new Label(wOutpuFields, SWT.RIGHT);
    wladdCloneFlag.setText( BaseMessages.getString( PKG, "CloneRowDialog.addCloneFlag.Label" ) );
    props.setLook(wladdCloneFlag);
    FormData fdladdCloneFlag = new FormData();
    fdladdCloneFlag.left = new FormAttachment( 0, 0 );
    fdladdCloneFlag.top = new FormAttachment( wNrCloneField, 2 * margin );
    fdladdCloneFlag.right = new FormAttachment( middle, -margin );
    wladdCloneFlag.setLayoutData(fdladdCloneFlag);
    waddCloneFlag = new Button(wOutpuFields, SWT.CHECK );
    waddCloneFlag.setToolTipText( BaseMessages.getString( PKG, "CloneRowDialog.addCloneFlag.Tooltip" ) );
    props.setLook( waddCloneFlag );
    FormData fdaddCloneFlag = new FormData();
    fdaddCloneFlag.left = new FormAttachment( middle, 0 );
    fdaddCloneFlag.top = new FormAttachment( wladdCloneFlag, 0, SWT.CENTER );
    fdaddCloneFlag.right = new FormAttachment( 100, 0 );
    waddCloneFlag.setLayoutData(fdaddCloneFlag);
    SelectionAdapter lsSelR = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        input.setChanged();
        activeaddCloneFlag();
      }
    };
    waddCloneFlag.addSelectionListener( lsSelR );

    // clone falg field line
    wlcloneFlagField = new Label(wOutpuFields, SWT.RIGHT );
    wlcloneFlagField.setText( BaseMessages.getString( PKG, "CloneRowDialog.cloneFlagField.Label" ) );
    props.setLook( wlcloneFlagField );
    FormData fdlcloneFlagField = new FormData();
    fdlcloneFlagField.left = new FormAttachment( 0, 0 );
    fdlcloneFlagField.right = new FormAttachment( middle, -margin );
    fdlcloneFlagField.top = new FormAttachment( waddCloneFlag, margin * 2 );
    wlcloneFlagField.setLayoutData(fdlcloneFlagField);

    wcloneFlagField = new TextVar( variables, wOutpuFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wcloneFlagField );
    wcloneFlagField.setToolTipText( BaseMessages.getString( PKG, "CloneRowDialog.cloneFlagField.Tooltip" ) );
    wcloneFlagField.addModifyListener( lsMod );
    FormData fdcloneFlagField = new FormData();
    fdcloneFlagField.left = new FormAttachment( middle, 0 );
    fdcloneFlagField.top = new FormAttachment( waddCloneFlag, margin * 2 );
    fdcloneFlagField.right = new FormAttachment( 100, 0 );
    wcloneFlagField.setLayoutData(fdcloneFlagField);

    // add clone num?
    Label wladdCloneNum = new Label(wOutpuFields, SWT.RIGHT);
    wladdCloneNum.setText( BaseMessages.getString( PKG, "CloneRowDialog.addCloneNum.Label" ) );
    props.setLook(wladdCloneNum);
    FormData fdladdCloneNum = new FormData();
    fdladdCloneNum.left = new FormAttachment( 0, 0 );
    fdladdCloneNum.top = new FormAttachment( wcloneFlagField, margin );
    fdladdCloneNum.right = new FormAttachment( middle, -margin );
    wladdCloneNum.setLayoutData(fdladdCloneNum);
    waddCloneNum = new Button(wOutpuFields, SWT.CHECK );
    waddCloneNum.setToolTipText( BaseMessages.getString( PKG, "CloneRowDialog.addCloneNum.Tooltip" ) );
    props.setLook( waddCloneNum );
    FormData fdaddCloneNum = new FormData();
    fdaddCloneNum.left = new FormAttachment( middle, 0 );
    fdaddCloneNum.top = new FormAttachment( wladdCloneNum, 0, SWT.CENTER );
    fdaddCloneNum.right = new FormAttachment( 100, 0 );
    waddCloneNum.setLayoutData(fdaddCloneNum);
    waddCloneNum.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        input.setChanged();
        activeaddCloneNum();
      }
    } );

    // clone num field line
    wlCloneNumField = new Label(wOutpuFields, SWT.RIGHT );
    wlCloneNumField.setText( BaseMessages.getString( PKG, "CloneRowDialog.cloneNumField.Label" ) );
    props.setLook( wlCloneNumField );
    fdlcloneFlagField = new FormData();
    fdlcloneFlagField.left = new FormAttachment( 0, 0 );
    fdlcloneFlagField.right = new FormAttachment( middle, -margin );
    fdlcloneFlagField.top = new FormAttachment( waddCloneNum, margin );
    wlCloneNumField.setLayoutData(fdlcloneFlagField);

    wCloneNumField = new TextVar( variables, wOutpuFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wCloneNumField );
    wCloneNumField.setToolTipText( BaseMessages.getString( PKG, "CloneRowDialog.cloneNumField.Tooltip" ) );
    wCloneNumField.addModifyListener( lsMod );
    FormData fdCloneNumField = new FormData();
    fdCloneNumField.left = new FormAttachment( middle, 0 );
    fdCloneNumField.top = new FormAttachment( waddCloneNum, margin );
    fdCloneNumField.right = new FormAttachment( 100, 0 );
    wCloneNumField.setLayoutData(fdCloneNumField);

    FormData fdOutpuFields = new FormData();
    fdOutpuFields.left = new FormAttachment( 0, margin );
    fdOutpuFields.top = new FormAttachment( wNrCloneField, 2 * margin );
    fdOutpuFields.right = new FormAttachment( 100, -margin );
    fdOutpuFields.bottom = new FormAttachment(wOk, -2*margin);
    wOutpuFields.setLayoutData(fdOutpuFields);

    // ///////////////////////////////////////////////////////////
    // / END OF Origin files GROUP
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
    activeaddCloneFlag();
    ActiveisNrCloneInField();
    activeaddCloneNum();
    input.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformName;
  }

  private void setisNrCloneInField() {
    if ( !gotPreviousFields ) {
      try {
        String field = wNrCloneField.getText();
        wNrCloneField.removeAll();
        IRowMeta r = pipelineMeta.getPrevTransformFields( variables, transformName );
        if ( r != null ) {
          wNrCloneField.setItems( r.getFieldNames() );
        }
        if ( field != null ) {
          wNrCloneField.setText( field );
        }
      } catch ( HopException ke ) {
        new ErrorDialog(
          shell, BaseMessages.getString( PKG, "CloneRowDialog.FailedToGetFields.DialogTitle" ), BaseMessages
          .getString( PKG, "CloneRowDialog.FailedToGetFields.DialogMessage" ), ke );
      }
      gotPreviousFields = true;
    }
  }

  private void ActiveisNrCloneInField() {
    wlNrCloneField.setEnabled( wIsNrCloneInField.getSelection() );
    wNrCloneField.setEnabled( wIsNrCloneInField.getSelection() );
    wlnrClone.setEnabled( !wIsNrCloneInField.getSelection() );
    wnrClone.setEnabled( !wIsNrCloneInField.getSelection() );
  }

  private void activeaddCloneFlag() {
    wlcloneFlagField.setEnabled( waddCloneFlag.getSelection() );
    wcloneFlagField.setEnabled( waddCloneFlag.getSelection() );
  }

  private void activeaddCloneNum() {
    wlCloneNumField.setEnabled( waddCloneNum.getSelection() );
    wCloneNumField.setEnabled( waddCloneNum.getSelection() );
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    if ( input.getNrClones() != null ) {
      wnrClone.setText( input.getNrClones() );
    }
    waddCloneFlag.setSelection( input.isAddCloneFlag() );
    if ( input.getCloneFlagField() != null ) {
      wcloneFlagField.setText( input.getCloneFlagField() );
    }
    wIsNrCloneInField.setSelection( input.isNrCloneInField() );
    if ( input.getNrCloneField() != null ) {
      wNrCloneField.setText( input.getNrCloneField() );
    }
    waddCloneNum.setSelection( input.isAddCloneNum() );
    if ( input.getCloneNumField() != null ) {
      wCloneNumField.setText( input.getCloneNumField() );
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

    transformName = wTransformName.getText(); // return value
    input.setNrClones( wnrClone.getText() );
    input.setAddCloneFlag( waddCloneFlag.getSelection() );
    input.setCloneFlagField( wcloneFlagField.getText() );
    input.setNrCloneInField( wIsNrCloneInField.getSelection() );
    input.setNrCloneField( wNrCloneField.getText() );
    input.setAddCloneNum( waddCloneNum.getSelection() );
    input.setCloneNumField( wCloneNumField.getText() );
    dispose();
  }
}
