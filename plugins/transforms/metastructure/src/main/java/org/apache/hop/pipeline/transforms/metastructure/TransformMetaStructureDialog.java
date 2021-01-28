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

package org.apache.hop.pipeline.transforms.metastructure;

import org.apache.hop.core.variables.IVariables;
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
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;

public class TransformMetaStructureDialog extends BaseTransformDialog implements ITransformDialog {
  private static Class<?> PKG = TransformMetaStructureMeta.class; // for i18n purposes, needed by Translator2!!

  private final TransformMetaStructureMeta input;

  private Button wOutputRowcount;
  private TextVar wRowCountField;

  public TransformMetaStructureDialog( Shell parent, IVariables variables, Object in, PipelineMeta tr, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, tr, sname );
    input = (TransformMetaStructureMeta) in;
  }

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
    shell.setText( BaseMessages.getString( PKG, "TransformMetaStructureDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // TransformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "TransformMetaStructureDialog.TransformName.Label" ) );
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

    // Rowcout Output
    Label wlOutputRowcount = new Label( shell, SWT.RIGHT );
    wlOutputRowcount.setText( BaseMessages.getString( PKG, "TransformMetaStructureDialog.outputRowcount.Label" ) );
    props.setLook( wlOutputRowcount );
    FormData fdlOutputRowcount = new FormData();
    fdlOutputRowcount.left = new FormAttachment( 0, 0 );
    fdlOutputRowcount.top = new FormAttachment( wTransformName, margin );
    fdlOutputRowcount.right = new FormAttachment( middle, -margin );
    wlOutputRowcount.setLayoutData( fdlOutputRowcount );
    wOutputRowcount = new Button( shell, SWT.CHECK );
    props.setLook( wOutputRowcount );
    FormData fdOutputRowcount = new FormData();
    fdOutputRowcount.left = new FormAttachment( middle, 0 );
    fdOutputRowcount.top = new FormAttachment( wlOutputRowcount, 0, SWT.CENTER );
    fdOutputRowcount.right = new FormAttachment( 100, 0 );
    wOutputRowcount.setLayoutData( fdOutputRowcount );
    wOutputRowcount.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();

        if ( wOutputRowcount.getSelection() ) {
          wRowCountField.setEnabled( true );
        } else {
          wRowCountField.setEnabled( false );
        }
      }
    } );

    // Row count Field
    Label wlRowCountField = new Label( shell, SWT.RIGHT );
    wlRowCountField.setText( BaseMessages.getString( PKG, "TransformMetaStructureDialog.RowcountField.Label" ) );
    props.setLook( wlRowCountField );
    FormData fdlRowCountField = new FormData();
    fdlRowCountField.left = new FormAttachment( 0, 0 );
    fdlRowCountField.right = new FormAttachment( middle, -margin );
    fdlRowCountField.top = new FormAttachment( wlOutputRowcount, 2*margin );
    wlRowCountField.setLayoutData( fdlRowCountField );

    wRowCountField = new TextVar( variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wRowCountField );
    wRowCountField.addModifyListener( lsMod );
    FormData fdRowCountField = new FormData();
    fdRowCountField.left = new FormAttachment( middle, 0 );
    fdRowCountField.top = new FormAttachment( wlRowCountField, 0, SWT.CENTER  );
    fdRowCountField.right = new FormAttachment( 100, -margin );
    wRowCountField.setLayoutData( fdRowCountField );
    wRowCountField.setEnabled( false );

    // Some buttons
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOk.addListener( SWT.Selection, e->ok() );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, e->cancel() );
    setButtonPositions( new Button[] { wOk, wCancel }, margin, wRowCountField );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wTransformName.addSelectionListener( lsDef );
    wRowCountField.addSelectionListener( lsDef );

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
    if ( input.getRowcountField() != null ) {
      wRowCountField.setText( input.getRowcountField() );
    }

    if ( input.isOutputRowcount() ) {
      wRowCountField.setEnabled( true );
    }

    wOutputRowcount.setSelection( input.isOutputRowcount() );

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

    getInfo( input );

    dispose();
  }

  private void getInfo( TransformMetaStructureMeta tfoi ) {
    tfoi.setOutputRowcount( wOutputRowcount.getSelection() );
    tfoi.setRowcountField( wRowCountField.getText() );
  }
}
