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

package org.apache.hop.ui.trans.steps.webserviceavailable;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.BaseStepMeta;
import org.apache.hop.trans.step.StepDialogInterface;
import org.apache.hop.trans.steps.webserviceavailable.WebServiceAvailableMeta;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.trans.step.BaseStepDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.FocusListener;
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

public class WebServiceAvailableDialog extends BaseStepDialog implements StepDialogInterface {
  private static Class<?> PKG = WebServiceAvailableMeta.class; // for i18n purposes, needed by Translator2!!

  private Label wlURL;
  private CCombo wURL;
  private FormData fdlURL, fdURL;

  private Label wlResult;
  private TextVar wResult;
  private FormData fdlResult, fdResult;

  private Label wlConnectTimeOut;
  private TextVar wConnectTimeOut;
  private FormData fdlConnectTimeOut, fdConnectTimeOut;

  private Label wlReadTimeOut;
  private TextVar wReadTimeOut;
  private FormData fdlReadTimeOut, fdReadTimeOut;

  private WebServiceAvailableMeta input;

  private boolean gotPreviousFields = false;

  public WebServiceAvailableDialog( Shell parent, Object in, TransMeta transMeta, String sname ) {
    super( parent, (BaseStepMeta) in, transMeta, sname );
    input = (WebServiceAvailableMeta) in;
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
    shell.setText( BaseMessages.getString( PKG, "WebServiceAvailableDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // Stepname line
    wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname.setText( BaseMessages.getString( PKG, "WebServiceAvailableDialog.Stepname.Label" ) );
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

    // filename field
    wlURL = new Label( shell, SWT.RIGHT );
    wlURL.setText( BaseMessages.getString( PKG, "WebServiceAvailableDialog.URL.Label" ) );
    props.setLook( wlURL );
    fdlURL = new FormData();
    fdlURL.left = new FormAttachment( 0, 0 );
    fdlURL.right = new FormAttachment( middle, -margin );
    fdlURL.top = new FormAttachment( wStepname, margin );
    wlURL.setLayoutData( fdlURL );

    wURL = new CCombo( shell, SWT.BORDER | SWT.READ_ONLY );
    wURL.setEditable( true );
    props.setLook( wURL );
    wURL.addModifyListener( lsMod );
    fdURL = new FormData();
    fdURL.left = new FormAttachment( middle, 0 );
    fdURL.top = new FormAttachment( wStepname, margin );
    fdURL.right = new FormAttachment( 100, -margin );
    wURL.setLayoutData( fdURL );
    wURL.addFocusListener( new FocusListener() {
      public void focusLost( org.eclipse.swt.events.FocusEvent e ) {
      }

      public void focusGained( org.eclipse.swt.events.FocusEvent e ) {
        get();
      }
    } );

    // connect timeout line
    wlConnectTimeOut = new Label( shell, SWT.RIGHT );
    wlConnectTimeOut.setText( BaseMessages.getString( PKG, "WebServiceAvailableDialog.ConnectTimeOut.Label" ) );
    props.setLook( wlConnectTimeOut );
    fdlConnectTimeOut = new FormData();
    fdlConnectTimeOut.left = new FormAttachment( 0, 0 );
    fdlConnectTimeOut.top = new FormAttachment( wURL, margin );
    fdlConnectTimeOut.right = new FormAttachment( middle, -margin );
    wlConnectTimeOut.setLayoutData( fdlConnectTimeOut );

    wConnectTimeOut = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wConnectTimeOut.setToolTipText( BaseMessages.getString(
      PKG, "WebServiceAvailableDialog.ConnectTimeOut.Tooltip" ) );
    props.setLook( wConnectTimeOut );
    wConnectTimeOut.addModifyListener( lsMod );
    fdConnectTimeOut = new FormData();
    fdConnectTimeOut.left = new FormAttachment( middle, 0 );
    fdConnectTimeOut.top = new FormAttachment( wURL, margin );
    fdConnectTimeOut.right = new FormAttachment( 100, -margin );
    wConnectTimeOut.setLayoutData( fdConnectTimeOut );

    // Whenever something changes, set the tooltip to the expanded version:
    wConnectTimeOut.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        wConnectTimeOut.setToolTipText( transMeta.environmentSubstitute( wConnectTimeOut.getText() ) );
      }
    } );

    // Read timeout line
    wlReadTimeOut = new Label( shell, SWT.RIGHT );
    wlReadTimeOut.setText( BaseMessages.getString( PKG, "WebServiceAvailableDialog.ReadTimeOut.Label" ) );
    props.setLook( wlReadTimeOut );
    fdlReadTimeOut = new FormData();
    fdlReadTimeOut.left = new FormAttachment( 0, 0 );
    fdlReadTimeOut.top = new FormAttachment( wConnectTimeOut, margin );
    fdlReadTimeOut.right = new FormAttachment( middle, -margin );
    wlReadTimeOut.setLayoutData( fdlReadTimeOut );

    wReadTimeOut = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wReadTimeOut.setToolTipText( BaseMessages.getString( PKG, "WebServiceAvailableDialog.ReadTimeOut.Tooltip" ) );
    props.setLook( wReadTimeOut );
    wReadTimeOut.addModifyListener( lsMod );
    fdReadTimeOut = new FormData();
    fdReadTimeOut.left = new FormAttachment( middle, 0 );
    fdReadTimeOut.top = new FormAttachment( wConnectTimeOut, margin );
    fdReadTimeOut.right = new FormAttachment( 100, -margin );
    wReadTimeOut.setLayoutData( fdReadTimeOut );

    // Whenever something changes, set the tooltip to the expanded version:
    wReadTimeOut.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        wReadTimeOut.setToolTipText( transMeta.environmentSubstitute( wReadTimeOut.getText() ) );
      }
    } );

    // Result fieldname ...
    wlResult = new Label( shell, SWT.RIGHT );
    wlResult.setText( BaseMessages.getString( PKG, "WebServiceAvailableDialog.ResultField.Label" ) );
    props.setLook( wlResult );
    fdlResult = new FormData();
    fdlResult.left = new FormAttachment( 0, 0 );
    fdlResult.right = new FormAttachment( middle, -margin );
    fdlResult.top = new FormAttachment( wReadTimeOut, margin * 2 );
    wlResult.setLayoutData( fdlResult );

    wResult = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wResult.setToolTipText( BaseMessages.getString( PKG, "WebServiceAvailableDialog.ResultField.Tooltip" ) );
    props.setLook( wResult );
    wResult.addModifyListener( lsMod );
    fdResult = new FormData();
    fdResult.left = new FormAttachment( middle, 0 );
    fdResult.top = new FormAttachment( wReadTimeOut, margin * 2 );
    fdResult.right = new FormAttachment( 100, 0 );
    wResult.setLayoutData( fdResult );

    // THE BUTTONS
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOK, wCancel }, margin, wResult );

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
    if ( isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "WebServiceAvailableDialog.Log.GettingKeyInfo" ) );
    }

    if ( input.getURLField() != null ) {
      wURL.setText( input.getURLField() );
    }
    if ( input.getConnectTimeOut() != null ) {
      wConnectTimeOut.setText( input.getConnectTimeOut() );
    }
    if ( input.getReadTimeOut() != null ) {
      wReadTimeOut.setText( input.getReadTimeOut() );
    }
    if ( input.getResultFieldName() != null ) {
      wResult.setText( input.getResultFieldName() );
    }

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
    input.setURLField( wURL.getText() );
    input.setConnectTimeOut( wConnectTimeOut.getText() );
    input.setReadTimeOut( wReadTimeOut.getText() );
    input.setResultFieldName( wResult.getText() );
    stepname = wStepname.getText(); // return value

    dispose();
  }

  private void get() {
    if ( !gotPreviousFields ) {
      try {
        String filefield = wURL.getText();
        wURL.removeAll();
        RowMetaInterface r = transMeta.getPrevStepFields( stepname );
        if ( r != null ) {
          wURL.setItems( r.getFieldNames() );
        }
        if ( filefield != null ) {
          wURL.setText( filefield );
        }
      } catch ( HopException ke ) {
        new ErrorDialog( shell,
          BaseMessages.getString( PKG, "WebServiceAvailableDialog.FailedToGetFields.DialogTitle" ),
          BaseMessages.getString( PKG, "WebServiceAvailableDialog.FailedToGetFields.DialogMessage" ), ke );
      }
      gotPreviousFields = true;
    }
  }
}
