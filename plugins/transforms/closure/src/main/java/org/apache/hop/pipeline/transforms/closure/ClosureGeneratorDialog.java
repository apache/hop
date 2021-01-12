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

package org.apache.hop.pipeline.transforms.closure;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transform.ComponentSelectionListener;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

public class ClosureGeneratorDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = ClosureGeneratorDialog.class; // For Translator

  private Button wRootZero;

  private CCombo wParent;

  private CCombo wChild;

  private Text wDistance;

  private final ClosureGeneratorMeta input;

  private IRowMeta inputFields;

  public ClosureGeneratorDialog( Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (ClosureGeneratorMeta) in;
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
    shell.setText( BaseMessages.getString( PKG, "ClosureGeneratorDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // TransformName line
    //
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "ClosureGeneratorDialog.TransformName" ) );
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

    // Parent ...
    //
    Label wlParent = new Label(shell, SWT.RIGHT);
    wlParent.setText( BaseMessages.getString( PKG, "ClosureGeneratorDialog.ParentField.Label" ) );
    props.setLook(wlParent);
    FormData fdlParent = new FormData();
    fdlParent.left = new FormAttachment( 0, 0 );
    fdlParent.right = new FormAttachment( middle, -margin );
    fdlParent.top = new FormAttachment( wTransformName, margin );
    wlParent.setLayoutData(fdlParent);

    wParent = new CCombo( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wParent );
    wParent.addModifyListener( lsMod );
    FormData fdParent = new FormData();
    fdParent.left = new FormAttachment( middle, 0 );
    fdParent.right = new FormAttachment( 100, 0 );
    fdParent.top = new FormAttachment( wTransformName, margin );
    wParent.setLayoutData(fdParent);

    // Child ...
    //
    Label wlChild = new Label(shell, SWT.RIGHT);
    wlChild.setText( BaseMessages.getString( PKG, "ClosureGeneratorDialog.ChildField.Label" ) );
    props.setLook(wlChild);
    FormData fdlChild = new FormData();
    fdlChild.left = new FormAttachment( 0, 0 );
    fdlChild.right = new FormAttachment( middle, -margin );
    fdlChild.top = new FormAttachment( wParent, margin );
    wlChild.setLayoutData(fdlChild);

    wChild = new CCombo( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wChild );
    wChild.addModifyListener( lsMod );
    FormData fdChild = new FormData();
    fdChild.left = new FormAttachment( middle, 0 );
    fdChild.right = new FormAttachment( 100, 0 );
    fdChild.top = new FormAttachment( wParent, margin );
    wChild.setLayoutData(fdChild);

    // Distance ...
    //
    Label wlDistance = new Label(shell, SWT.RIGHT);
    wlDistance.setText( BaseMessages.getString( PKG, "ClosureGeneratorDialog.DistanceField.Label" ) );
    props.setLook(wlDistance);
    FormData fdlDistance = new FormData();
    fdlDistance.left = new FormAttachment( 0, 0 );
    fdlDistance.right = new FormAttachment( middle, -margin );
    fdlDistance.top = new FormAttachment( wChild, margin );
    wlDistance.setLayoutData(fdlDistance);

    wDistance = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wDistance );
    wDistance.addModifyListener( lsMod );
    FormData fdDistance = new FormData();
    fdDistance.left = new FormAttachment( middle, 0 );
    fdDistance.right = new FormAttachment( 100, 0 );
    fdDistance.top = new FormAttachment( wChild, margin );
    wDistance.setLayoutData(fdDistance);

    // Root is zero(Integer)?
    //
    Label wlRootZero = new Label(shell, SWT.RIGHT);
    wlRootZero.setText( BaseMessages.getString( PKG, "ClosureGeneratorDialog.RootZero.Label" ) );
    props.setLook(wlRootZero);
    FormData fdlRootZero = new FormData();
    fdlRootZero.left = new FormAttachment( 0, 0 );
    fdlRootZero.right = new FormAttachment( middle, -margin );
    fdlRootZero.top = new FormAttachment( wDistance, margin );
    wlRootZero.setLayoutData(fdlRootZero);

    wRootZero = new Button( shell, SWT.CHECK );
    props.setLook( wRootZero );
    FormData fdRootZero = new FormData();
    fdRootZero.left = new FormAttachment( middle, 0 );
    fdRootZero.right = new FormAttachment( 100, 0 );
    fdRootZero.top = new FormAttachment( wlRootZero, 0, SWT.CENTER );
    wRootZero.setLayoutData(fdRootZero);
    wRootZero.addSelectionListener( new ComponentSelectionListener( input ) );

    // Search the fields in the background
    //
    final Runnable runnable = () -> {
      TransformMeta transformMeta = pipelineMeta.findTransform( transformName );
      if ( transformMeta != null ) {
        try {
          inputFields = pipelineMeta.getPrevTransformFields( variables, transformMeta );
          setComboBoxes();
        } catch ( HopException e ) {
          logError( BaseMessages.getString( PKG, "ClosureGeneratorDialog.Log.UnableToFindInput" ) );
        }
      }
    };
    new Thread( runnable ).start();

    // Some buttons
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOk, wCancel, }, margin, null );

    // Add listeners
    lsCancel = e -> cancel();
    lsOk = e -> ok();

    wCancel.addListener( SWT.Selection, lsCancel );
    wOk.addListener( SWT.Selection, lsOk );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wTransformName.addSelectionListener( lsDef );
    wDistance.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();
    input.setChanged( changed );

    // Set the shell size, based upon previous time...
    setSize();

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformName;
  }

  protected void setComboBoxes() {
    shell.getDisplay().syncExec( () -> {
      if ( inputFields != null ) {
        String[] fieldNames = inputFields.getFieldNames();
        wParent.setItems( fieldNames );
        wChild.setItems( fieldNames );
      }
    } );
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    if ( input.getParentIdFieldName() != null ) {
      wParent.setText( input.getParentIdFieldName() );
    }
    if ( input.getChildIdFieldName() != null ) {
      wChild.setText( input.getChildIdFieldName() );
    }
    if ( input.getDistanceFieldName() != null ) {
      wDistance.setText( input.getDistanceFieldName() );
    }
    wRootZero.setSelection( input.isRootIdZero() );

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    input.setChanged( changed );
    dispose();
  }

  private void getInfo( ClosureGeneratorMeta meta ) {
    meta.setParentIdFieldName( wParent.getText() );
    meta.setChildIdFieldName( wChild.getText() );
    meta.setDistanceFieldName( wDistance.getText() );
    meta.setRootIdZero( wRootZero.getSelection() );
  }

  private void ok() {
    if ( Utils.isEmpty( wTransformName.getText() ) ) {
      return;
    }

    transformName = wTransformName.getText(); // return value
    getInfo( input );

    dispose();
  }
}
