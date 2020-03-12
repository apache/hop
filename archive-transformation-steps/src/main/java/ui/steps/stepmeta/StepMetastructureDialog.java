/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.ui.trans.steps.stepmeta;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.BaseStepMeta;
import org.apache.hop.trans.step.StepDialogInterface;
import org.apache.hop.trans.steps.stepmeta.StepMetastructureMeta;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.trans.step.BaseStepDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyEvent;
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
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

public class StepMetastructureDialog extends BaseStepDialog implements StepDialogInterface {
  private static Class<?> PKG = StepMetastructureMeta.class; // for i18n purposes, needed by Translator2!!

  private StepMetastructureMeta input;

  private Label wlOutputRowcount;
  private Button wOutputRowcount;
  private FormData fdlOutputRowcount, fdOutputRowcount;

  private Label wlRowcountField;
  private TextVar wRowcountField;
  private FormData fdlRowcountField, fdRowcountField;

  public StepMetastructureDialog( Shell parent, Object in, TransMeta tr, String sname ) {
    super( parent, (BaseStepMeta) in, tr, sname );
    input = (StepMetastructureMeta) in;
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX );
    props.setLook( shell );
    setShellImage( shell, input );

    ModifyListener lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        input.setChanged();
      }
    };
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "StepMetastructureDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // Stepname line
    wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname.setText( BaseMessages.getString( PKG, "StepMetastructureDialog.Stepname.Label" ) );
    props.setLook( wlStepname );
    fdlStepname = new FormData();
    fdlStepname.left = new FormAttachment( 0, 0 );
    fdlStepname.right = new FormAttachment( middle, -margin );
    fdlStepname.top = new FormAttachment( 0, margin );
    wlStepname.setLayoutData( fdlStepname );
    wStepname = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wStepname.setText( stepname );
    props.setLook( wStepname );
    wStepname.addModifyListener( lsMod );
    fdStepname = new FormData();
    fdStepname.left = new FormAttachment( middle, 0 );
    fdStepname.top = new FormAttachment( 0, margin );
    fdStepname.right = new FormAttachment( 100, 0 );
    wStepname.setLayoutData( fdStepname );

    // Rowcout Output
    wlOutputRowcount = new Label( shell, SWT.RIGHT );
    wlOutputRowcount.setText( BaseMessages.getString( PKG, "StepMetastructureDialog.outputRowcount.Label" ) );
    props.setLook( wlOutputRowcount );
    fdlOutputRowcount = new FormData();
    fdlOutputRowcount.left = new FormAttachment( 0, 0 );
    fdlOutputRowcount.top = new FormAttachment( wStepname, margin );
    fdlOutputRowcount.right = new FormAttachment( middle, -margin );
    wlOutputRowcount.setLayoutData( fdlOutputRowcount );
    wOutputRowcount = new Button( shell, SWT.CHECK );
    props.setLook( wOutputRowcount );
    fdOutputRowcount = new FormData();
    fdOutputRowcount.left = new FormAttachment( middle, 0 );
    fdOutputRowcount.top = new FormAttachment( wStepname, margin );
    fdOutputRowcount.right = new FormAttachment( 100, 0 );
    wOutputRowcount.setLayoutData( fdOutputRowcount );
    wOutputRowcount.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();

        if ( wOutputRowcount.getSelection() ) {
          wRowcountField.setEnabled( true );
        } else {
          wRowcountField.setEnabled( false );
        }
      }
    } );

    // Rowcout Field
    wlRowcountField = new Label( shell, SWT.RIGHT );
    wlRowcountField.setText( BaseMessages.getString( PKG, "StepMetastructureDialog.RowcountField.Label" ) );
    props.setLook( wlRowcountField );
    fdlRowcountField = new FormData();
    fdlRowcountField.left = new FormAttachment( 0, 0 );
    fdlRowcountField.right = new FormAttachment( middle, -margin );
    fdlRowcountField.top = new FormAttachment( wOutputRowcount, margin );
    wlRowcountField.setLayoutData( fdlRowcountField );

    wRowcountField = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wRowcountField );
    wRowcountField.addModifyListener( lsMod );
    fdRowcountField = new FormData();
    fdRowcountField.left = new FormAttachment( middle, 0 );
    fdRowcountField.top = new FormAttachment( wOutputRowcount, margin );
    fdRowcountField.right = new FormAttachment( 100, -margin );
    wRowcountField.setLayoutData( fdRowcountField );
    wRowcountField.setEnabled( false );

    // Some buttons
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOK, wCancel }, margin, wRowcountField );

    // Add listeners
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };
    lsOK = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };

    wCancel.addListener( SWT.Selection, lsCancel );
    wOK.addListener( SWT.Selection, lsOK );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wStepname.addSelectionListener( lsDef );

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
    return stepname;
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    if ( input.getRowcountField() != null ) {
      wRowcountField.setText( input.getRowcountField() );
    }

    if ( input.isOutputRowcount() ) {
      wRowcountField.setEnabled( true );
    }

    wOutputRowcount.setSelection( input.isOutputRowcount() );

    wStepname.selectAll();
    wStepname.setFocus();
  }

  private void cancel() {
    stepname = null;
    input.setChanged( changed );
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( wStepname.getText() ) ) {
      return;
    }

    stepname = wStepname.getText(); // return value

    getInfo( input );

    dispose();
  }

  private void getInfo( StepMetastructureMeta tfoi ) {
    tfoi.setOutputRowcount( wOutputRowcount.getSelection() );
    tfoi.setRowcountField( wRowcountField.getText() );
  }
}
