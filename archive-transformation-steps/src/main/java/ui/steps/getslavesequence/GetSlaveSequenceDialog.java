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

package org.apache.hop.ui.trans.steps.getslavesequence;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.BaseStepMeta;
import org.apache.hop.trans.step.StepDialogInterface;
import org.apache.hop.trans.steps.getslavesequence.GetSlaveSequenceMeta;
import org.apache.hop.ui.core.widget.ComboVar;
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

public class GetSlaveSequenceDialog extends BaseStepDialog implements StepDialogInterface {
  private static Class<?> PKG = GetSlaveSequenceMeta.class; // for i18n purposes, needed by Translator2!!

  private Label wlValuename;
  private Text wValuename;

  private Label wlSlaveServer;
  private ComboVar wSlaveServer;

  private Label wlSeqname;
  private TextVar wSeqname;

  private Label wlIncrement;
  private TextVar wIncrement;

  private GetSlaveSequenceMeta input;

  public GetSlaveSequenceDialog( Shell parent, Object in, TransMeta transMeta, String sname ) {
    super( parent, (BaseStepMeta) in, transMeta, sname );
    input = (GetSlaveSequenceMeta) in;
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
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
    shell.setText( BaseMessages.getString( PKG, "GetSequenceDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // Stepname line
    wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname.setText( BaseMessages.getString( PKG, "GetSequenceDialog.StepName.Label" ) );
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

    // Valuename line
    wlValuename = new Label( shell, SWT.RIGHT );
    wlValuename.setText( BaseMessages.getString( PKG, "GetSequenceDialog.Valuename.Label" ) );
    props.setLook( wlValuename );
    FormData fdlValuename = new FormData();
    fdlValuename.left = new FormAttachment( 0, 0 );
    fdlValuename.top = new FormAttachment( wStepname, margin );
    fdlValuename.right = new FormAttachment( middle, -margin );
    wlValuename.setLayoutData( fdlValuename );
    wValuename = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wValuename.setText( "" );
    props.setLook( wValuename );
    wValuename.addModifyListener( lsMod );
    FormData fdValuename = new FormData();
    fdValuename.left = new FormAttachment( middle, 0 );
    fdValuename.top = new FormAttachment( wStepname, margin );
    fdValuename.right = new FormAttachment( 100, 0 );
    wValuename.setLayoutData( fdValuename );

    // Connection line
    //
    wlSlaveServer = new Label( shell, SWT.RIGHT );
    wlSlaveServer.setText( BaseMessages.getString( PKG, "GetSequenceDialog.SlaveServer.Label" ) );
    props.setLook( wlSlaveServer );
    FormData fdlSlaveServer = new FormData();
    fdlSlaveServer.left = new FormAttachment( 0, 0 );
    fdlSlaveServer.top = new FormAttachment( wValuename, margin );
    fdlSlaveServer.right = new FormAttachment( middle, -margin );
    wlSlaveServer.setLayoutData( fdlSlaveServer );
    wSlaveServer = new ComboVar( transMeta, shell, SWT.LEFT | SWT.SINGLE | SWT.BORDER );
    wSlaveServer.setItems( transMeta.getSlaveServerNames() );
    FormData fdSlaveServer = new FormData();
    fdSlaveServer.left = new FormAttachment( middle, 0 );
    fdSlaveServer.top = new FormAttachment( wValuename, margin );
    fdSlaveServer.right = new FormAttachment( 100, 0 );
    wSlaveServer.setLayoutData( fdSlaveServer );

    // Seqname line
    wlSeqname = new Label( shell, SWT.RIGHT );
    wlSeqname.setText( BaseMessages.getString( PKG, "GetSequenceDialog.Seqname.Label" ) );
    props.setLook( wlSeqname );
    FormData fdlSeqname = new FormData();
    fdlSeqname.left = new FormAttachment( 0, 0 );
    fdlSeqname.right = new FormAttachment( middle, -margin );
    fdlSeqname.top = new FormAttachment( wSlaveServer, margin );
    wlSeqname.setLayoutData( fdlSeqname );

    wSeqname = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wSeqname.setText( "" );
    props.setLook( wSeqname );
    wSeqname.addModifyListener( lsMod );
    FormData fdSeqname = new FormData();
    fdSeqname.left = new FormAttachment( middle, 0 );
    fdSeqname.top = new FormAttachment( wSlaveServer, margin );
    fdSeqname.right = new FormAttachment( 100, 0 );
    wSeqname.setLayoutData( fdSeqname );

    // Increment line
    wlIncrement = new Label( shell, SWT.RIGHT );
    wlIncrement.setText( BaseMessages.getString( PKG, "GetSequenceDialog.Increment.Label" ) );
    props.setLook( wlIncrement );
    FormData fdlIncrement = new FormData();
    fdlIncrement.left = new FormAttachment( 0, 0 );
    fdlIncrement.right = new FormAttachment( middle, -margin );
    fdlIncrement.top = new FormAttachment( wSeqname, margin );
    wlIncrement.setLayoutData( fdlIncrement );

    wIncrement = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wIncrement.setText( "" );
    props.setLook( wIncrement );
    wIncrement.addModifyListener( lsMod );
    FormData fdIncrement = new FormData();
    fdIncrement.left = new FormAttachment( middle, 0 );
    fdIncrement.top = new FormAttachment( wSeqname, margin );
    fdIncrement.right = new FormAttachment( 100, 0 );
    wIncrement.setLayoutData( fdIncrement );

    // THE BUTTONS
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOK, wCancel }, margin, wIncrement );

    // Add listeners
    lsOK = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };

    wOK.addListener( SWT.Selection, lsOK );
    wCancel.addListener( SWT.Selection, lsCancel );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wStepname.addSelectionListener( lsDef );
    wValuename.addSelectionListener( lsDef );
    wSeqname.addSelectionListener( lsDef );
    wIncrement.addSelectionListener( lsDef );

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
    logDebug( BaseMessages.getString( PKG, "GetSequenceDialog.Log.GettingKeyInfo" ) );

    wValuename.setText( Const.NVL( input.getValuename(), "" ) );
    wSlaveServer.setText( Const.NVL( input.getSlaveServerName(), "" ) );
    wSeqname.setText( Const.NVL( input.getSequenceName(), "" ) );
    wIncrement.setText( Const.NVL( input.getIncrement(), "" ) );

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

    input.setSlaveServerName( wSlaveServer.getText() );
    input.setValuename( wValuename.getText() );
    input.setSequenceName( wSeqname.getText() );
    input.setIncrement( wIncrement.getText() );

    dispose();
  }

}
