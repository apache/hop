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

package org.apache.hop.ui.trans.steps.pgpdecryptstream;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.BaseStepMeta;
import org.apache.hop.trans.step.StepDialogInterface;
import org.apache.hop.trans.steps.pgpdecryptstream.PGPDecryptStreamMeta;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.PasswordTextVar;
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
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

public class PGPDecryptStreamDialog extends BaseStepDialog implements StepDialogInterface {
  private static Class<?> PKG = PGPDecryptStreamMeta.class; // for i18n purposes, needed by Translator2!!
  private boolean gotPreviousFields = false;

  private Label wlGPGLocation;
  private TextVar wGPGLocation;
  private FormData fdlGPGLocation, fdGPGLocation;

  private Label wlPassphrase;
  private TextVar wPassphrase;
  private FormData fdlPassphrase, fdPassphrase;

  private Label wlStreamFieldName;
  private CCombo wStreamFieldName;
  private FormData fdlStreamFieldName, fdStreamFieldName;

  private TextVar wResult;
  private FormData fdResult, fdlResult;
  private Label wlResult;

  private Button wbbGpgExe;
  private FormData fdbbGpgExe;

  private PGPDecryptStreamMeta input;

  private Group wGPGGroup;
  private FormData fdGPGGroup;

  private Button wPassphraseFromField;
  private FormData fdPassphraseFromField, fdlPassphraseFromField;
  private Label wlPassphraseFromField;

  private Label wlPassphraseFieldName;
  private CCombo wPassphraseFieldName;
  private FormData fdlPassphraseFieldName, fdPassphraseFieldName;

  private static final String[] FILETYPES = new String[] { BaseMessages.getString(
    PKG, "PGPDecryptStreamDialog.Filetype.All" ) };

  public PGPDecryptStreamDialog( Shell parent, Object in, TransMeta transMeta, String sname ) {
    super( parent, (BaseStepMeta) in, transMeta, sname );
    input = (PGPDecryptStreamMeta) in;
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
    shell.setText( BaseMessages.getString( PKG, "PGPDecryptStreamDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // Stepname line
    wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname.setText( BaseMessages.getString( PKG, "PGPDecryptStreamDialog.Stepname.Label" ) );
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

    // ///////////////////////////////
    // START OF GPG Fields GROUP //
    // ///////////////////////////////

    wGPGGroup = new Group( shell, SWT.SHADOW_NONE );
    props.setLook( wGPGGroup );
    wGPGGroup.setText( BaseMessages.getString( PKG, "PGPDecryptStreamDialog.GPGGroup.Label" ) );

    FormLayout GPGGroupgroupLayout = new FormLayout();
    GPGGroupgroupLayout.marginWidth = 10;
    GPGGroupgroupLayout.marginHeight = 10;
    wGPGGroup.setLayout( GPGGroupgroupLayout );

    // GPGLocation fieldname ...
    wlGPGLocation = new Label( wGPGGroup, SWT.RIGHT );
    wlGPGLocation.setText( BaseMessages.getString( PKG, "PGPDecryptStreamDialog.GPGLocationField.Label" ) );
    props.setLook( wlGPGLocation );
    fdlGPGLocation = new FormData();
    fdlGPGLocation.left = new FormAttachment( 0, 0 );
    fdlGPGLocation.right = new FormAttachment( middle, -margin );
    fdlGPGLocation.top = new FormAttachment( wStepname, margin * 2 );
    wlGPGLocation.setLayoutData( fdlGPGLocation );

    // Browse Source files button ...
    wbbGpgExe = new Button( wGPGGroup, SWT.PUSH | SWT.CENTER );
    props.setLook( wbbGpgExe );
    wbbGpgExe.setText( BaseMessages.getString( PKG, "PGPDecryptStreamDialog.BrowseFiles.Label" ) );
    fdbbGpgExe = new FormData();
    fdbbGpgExe.right = new FormAttachment( 100, -margin );
    fdbbGpgExe.top = new FormAttachment( wStepname, margin );
    wbbGpgExe.setLayoutData( fdbbGpgExe );

    wbbGpgExe.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        FileDialog dialog = new FileDialog( shell, SWT.OPEN );
        dialog.setFilterExtensions( new String[] { "*" } );
        if ( wGPGLocation.getText() != null ) {
          dialog.setFileName( transMeta.environmentSubstitute( wGPGLocation.getText() ) );
        }
        dialog.setFilterNames( FILETYPES );
        if ( dialog.open() != null ) {
          wGPGLocation.setText( dialog.getFilterPath() + Const.FILE_SEPARATOR + dialog.getFileName() );
        }
      }
    } );

    wGPGLocation = new TextVar( transMeta, wGPGGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wGPGLocation.setToolTipText( BaseMessages.getString( PKG, "PGPDecryptStreamDialog.GPGLocationField.Tooltip" ) );
    props.setLook( wGPGLocation );
    wGPGLocation.addModifyListener( lsMod );
    fdGPGLocation = new FormData();
    fdGPGLocation.left = new FormAttachment( middle, 0 );
    fdGPGLocation.top = new FormAttachment( wStepname, margin * 2 );
    fdGPGLocation.right = new FormAttachment( wbbGpgExe, -margin );
    wGPGLocation.setLayoutData( fdGPGLocation );

    // Passphrase fieldname ...
    wlPassphrase = new Label( wGPGGroup, SWT.RIGHT );
    wlPassphrase.setText( BaseMessages.getString( PKG, "PGPDecryptStreamDialog.PassphraseField.Label" ) );
    props.setLook( wlPassphrase );
    fdlPassphrase = new FormData();
    fdlPassphrase.left = new FormAttachment( 0, 0 );
    fdlPassphrase.right = new FormAttachment( middle, -margin );
    fdlPassphrase.top = new FormAttachment( wGPGLocation, margin );
    wlPassphrase.setLayoutData( fdlPassphrase );

    wPassphrase = new PasswordTextVar( transMeta, wGPGGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wPassphrase.setToolTipText( BaseMessages.getString( PKG, "PGPDecryptStreamDialog.PassphraseField.Tooltip" ) );
    props.setLook( wPassphrase );
    wPassphrase.addModifyListener( lsMod );
    fdPassphrase = new FormData();
    fdPassphrase.left = new FormAttachment( middle, 0 );
    fdPassphrase.top = new FormAttachment( wGPGLocation, margin );
    fdPassphrase.right = new FormAttachment( 100, 0 );
    wPassphrase.setLayoutData( fdPassphrase );

    wlPassphraseFromField = new Label( wGPGGroup, SWT.RIGHT );
    wlPassphraseFromField.setText( BaseMessages
      .getString( PKG, "PGPDecryptStreamDialog.PassphraseFromField.Label" ) );
    props.setLook( wlPassphraseFromField );
    fdlPassphraseFromField = new FormData();
    fdlPassphraseFromField.left = new FormAttachment( 0, 0 );
    fdlPassphraseFromField.top = new FormAttachment( wPassphrase, margin );
    fdlPassphraseFromField.right = new FormAttachment( middle, -margin );
    wlPassphraseFromField.setLayoutData( fdlPassphraseFromField );
    wPassphraseFromField = new Button( wGPGGroup, SWT.CHECK );
    props.setLook( wPassphraseFromField );
    wPassphraseFromField.setToolTipText( BaseMessages.getString(
      PKG, "PGPDecryptStreamDialog.PassphraseFromField.Tooltip" ) );
    fdPassphraseFromField = new FormData();
    fdPassphraseFromField.left = new FormAttachment( middle, 0 );
    fdPassphraseFromField.top = new FormAttachment( wPassphrase, margin );
    wPassphraseFromField.setLayoutData( fdPassphraseFromField );

    wPassphraseFromField.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        PassphraseFromField();
      }
    } );

    // Passphrase field
    wlPassphraseFieldName = new Label( wGPGGroup, SWT.RIGHT );
    wlPassphraseFieldName.setText( BaseMessages
      .getString( PKG, "PGPDecryptStreamDialog.PassphraseFieldName.Label" ) );
    props.setLook( wlPassphraseFieldName );
    fdlPassphraseFieldName = new FormData();
    fdlPassphraseFieldName.left = new FormAttachment( 0, 0 );
    fdlPassphraseFieldName.right = new FormAttachment( middle, -margin );
    fdlPassphraseFieldName.top = new FormAttachment( wPassphraseFromField, margin );
    wlPassphraseFieldName.setLayoutData( fdlPassphraseFieldName );

    wPassphraseFieldName = new CCombo( wGPGGroup, SWT.BORDER | SWT.READ_ONLY );
    props.setLook( wPassphraseFieldName );
    wPassphraseFieldName.addModifyListener( lsMod );
    fdPassphraseFieldName = new FormData();
    fdPassphraseFieldName.left = new FormAttachment( middle, 0 );
    fdPassphraseFieldName.top = new FormAttachment( wPassphraseFromField, margin );
    fdPassphraseFieldName.right = new FormAttachment( 100, -margin );
    wPassphraseFieldName.setLayoutData( fdPassphraseFieldName );
    wPassphraseFieldName.addFocusListener( new FocusListener() {
      public void focusLost( org.eclipse.swt.events.FocusEvent e ) {
      }

      public void focusGained( org.eclipse.swt.events.FocusEvent e ) {
        Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
        shell.setCursor( busy );
        get();
        shell.setCursor( null );
        busy.dispose();
      }
    } );

    fdGPGGroup = new FormData();
    fdGPGGroup.left = new FormAttachment( 0, margin );
    fdGPGGroup.top = new FormAttachment( wStepname, margin );
    fdGPGGroup.right = new FormAttachment( 100, -margin );
    wGPGGroup.setLayoutData( fdGPGGroup );

    // ///////////////////////////////
    // END OF GPG GROUP //
    // ///////////////////////////////

    // Stream field
    wlStreamFieldName = new Label( shell, SWT.RIGHT );
    wlStreamFieldName.setText( BaseMessages.getString( PKG, "PGPDecryptStreamDialog.StreamFieldName.Label" ) );
    props.setLook( wlStreamFieldName );
    fdlStreamFieldName = new FormData();
    fdlStreamFieldName.left = new FormAttachment( 0, 0 );
    fdlStreamFieldName.right = new FormAttachment( middle, -margin );
    fdlStreamFieldName.top = new FormAttachment( wGPGGroup, 2 * margin );
    wlStreamFieldName.setLayoutData( fdlStreamFieldName );

    wStreamFieldName = new CCombo( shell, SWT.BORDER | SWT.READ_ONLY );
    props.setLook( wStreamFieldName );
    wStreamFieldName.addModifyListener( lsMod );
    fdStreamFieldName = new FormData();
    fdStreamFieldName.left = new FormAttachment( middle, 0 );
    fdStreamFieldName.top = new FormAttachment( wGPGGroup, 2 * margin );
    fdStreamFieldName.right = new FormAttachment( 100, -margin );
    wStreamFieldName.setLayoutData( fdStreamFieldName );
    wStreamFieldName.addFocusListener( new FocusListener() {
      public void focusLost( org.eclipse.swt.events.FocusEvent e ) {
      }

      public void focusGained( org.eclipse.swt.events.FocusEvent e ) {
        Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
        shell.setCursor( busy );
        get();
        shell.setCursor( null );
        busy.dispose();
      }
    } );

    // Result fieldname ...
    wlResult = new Label( shell, SWT.RIGHT );
    wlResult.setText( BaseMessages.getString( PKG, "PGPDecryptStreamDialog.ResultField.Label" ) );
    props.setLook( wlResult );
    fdlResult = new FormData();
    fdlResult.left = new FormAttachment( 0, 0 );
    fdlResult.right = new FormAttachment( middle, -margin );
    fdlResult.top = new FormAttachment( wStreamFieldName, margin * 2 );
    wlResult.setLayoutData( fdlResult );

    wResult = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wResult.setToolTipText( BaseMessages.getString( PKG, "PGPDecryptStreamDialog.ResultField.Tooltip" ) );
    props.setLook( wResult );
    wResult.addModifyListener( lsMod );
    fdResult = new FormData();
    fdResult.left = new FormAttachment( middle, 0 );
    fdResult.top = new FormAttachment( wStreamFieldName, margin * 2 );
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
    PassphraseFromField();
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
    if ( input.getGPGLocation() != null ) {
      wGPGLocation.setText( input.getGPGLocation() );
    }
    if ( input.getStreamField() != null ) {
      wStreamFieldName.setText( input.getStreamField() );
    }
    if ( input.getResultFieldName() != null ) {
      wResult.setText( input.getResultFieldName() );
    }
    if ( input.getPassphrase() != null ) {
      wPassphrase.setText( input.getPassphrase() );
    }
    wPassphraseFromField.setSelection( input.isPassphraseFromField() );
    if ( input.getPassphraseFieldName() != null ) {
      wPassphraseFieldName.setText( input.getPassphraseFieldName() );
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
    input.setStreamField( wStreamFieldName.getText() );
    input.setGPGLocation( wGPGLocation.getText() );
    input.setPassphrase( wPassphrase.getText() );
    input.setResultFieldName( wResult.getText() );
    input.setPassphraseFromField( wPassphraseFromField.getSelection() );
    input.setPassphraseFieldName( wPassphraseFieldName.getText() );
    stepname = wStepname.getText(); // return value

    dispose();
  }

  private void PassphraseFromField() {
    wlPassphrase.setEnabled( !wPassphraseFromField.getSelection() );
    wPassphrase.setEnabled( !wPassphraseFromField.getSelection() );
    wlPassphraseFromField.setEnabled( wPassphraseFromField.getSelection() );
    wPassphraseFromField.setEnabled( wPassphraseFromField.getSelection() );
  }

  private void get() {
    if ( !gotPreviousFields ) {
      try {
        String fieldvalue = wStreamFieldName.getText();
        String passphrasefieldvalue = wPassphraseFieldName.getText();
        wStreamFieldName.removeAll();
        wPassphraseFieldName.removeAll();
        RowMetaInterface r = transMeta.getPrevStepFields( stepname );
        if ( r != null ) {
          String[] fields = r.getFieldNames();
          wStreamFieldName.setItems( fields );
          wPassphraseFieldName.setItems( fields );
        }
        if ( fieldvalue != null ) {
          wStreamFieldName.setText( fieldvalue );
        }
        if ( passphrasefieldvalue != null ) {
          wPassphraseFieldName.setText( passphrasefieldvalue );
        }
        gotPreviousFields = true;
      } catch ( HopException ke ) {
        new ErrorDialog( shell,
          BaseMessages.getString( PKG, "PGPDecryptStreamDialog.FailedToGetFields.DialogTitle" ),
          BaseMessages.getString( PKG, "PGPDecryptStreamDialog.FailedToGetFields.DialogMessage" ), ke );
      }
    }
  }
}
