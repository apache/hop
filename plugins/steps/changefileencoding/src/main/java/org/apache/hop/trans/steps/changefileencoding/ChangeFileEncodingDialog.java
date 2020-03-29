/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.trans.steps.changefileencoding;

import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.PluginDialog;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.BaseStepMeta;
import org.apache.hop.trans.step.StepDialogInterface;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.trans.step.BaseStepDialog;
import org.apache.hop.ui.trans.step.ComponentSelectionListener;
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
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

import java.nio.charset.Charset;
import java.util.ArrayList;

@PluginDialog( id = "ChangeFileEncoding", image = "ChangeFileEncoding.svg", pluginType = PluginDialog.PluginType.STEP,
  documentationUrl = "http://wiki.pentaho.com/display/EAI/Change+file+encoding" )
public class ChangeFileEncodingDialog extends BaseStepDialog implements StepDialogInterface {
  private static final Class<?> PKG = ChangeFileEncodingDialog.class; // for i18n purposes, needed by Translator2!!

  private Label wlFileName;
  private CCombo wFileName;
  private FormData fdlFileName, fdfileName;

  private Label wlTargetFileName;
  private CCombo wTargetFileName;
  private FormData fdlTargetFileName, fdTargetFileName;

  private Label wlTargetEncoding;
  private ComboVar wTargetEncoding;
  private FormData fdlTargetEncoding, fdTargetEncoding;

  private Label wlSourceEncoding;
  private ComboVar wSourceEncoding;
  private FormData fdlSourceEncoding, fdSourceEncoding;

  private Button wSourceAddResult;
  private FormData fdSourceAddResult, fdlSourceAddResult;
  private Label wlSourceAddResult;

  private Button wTargetAddResult;
  private FormData fdTargetAddResult, fdlTargetAddResult;
  private Label wlTargetAddResult;

  private Button wCreateParentFolder;
  private FormData fdCreateParentFolder, fdlCreateParentFolder;
  private Label wlCreateParentFolder;

  private ChangeFileEncodingMeta input;

  private boolean gotPreviousFields = false;

  private Group wSourceFileGroup;
  private FormData fdSourceFileGroup;

  private Group wTargetFileGroup;
  private FormData fdTargetFileGroup;

  public ChangeFileEncodingDialog( Shell parent, Object in, TransMeta transMeta, String sname ) {
    super( parent, (BaseStepMeta) in, transMeta, sname );
    input = (ChangeFileEncodingMeta) in;
  }

  @Override
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
    shell.setText( BaseMessages.getString( PKG, "ChangeFileEncodingDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // Stepname line
    wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname.setText( BaseMessages.getString( PKG, "ChangeFileEncodingDialog.Stepname.Label" ) );
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

    // /////////////////////////////////
    // START OF SourceFile GROUP
    // /////////////////////////////////

    wSourceFileGroup = new Group( shell, SWT.SHADOW_NONE );
    props.setLook( wSourceFileGroup );
    wSourceFileGroup.setText( BaseMessages.getString( PKG, "ChangeFileEncodingDialog.Group.SourceFileGroup.Label" ) );

    FormLayout SourceFilegroupLayout = new FormLayout();
    SourceFilegroupLayout.marginWidth = 10;
    SourceFilegroupLayout.marginHeight = 10;
    wSourceFileGroup.setLayout( SourceFilegroupLayout );

    // filename field
    wlFileName = new Label( wSourceFileGroup, SWT.RIGHT );
    wlFileName.setText( BaseMessages.getString( PKG, "ChangeFileEncodingDialog.FileName.Label" ) );
    props.setLook( wlFileName );
    fdlFileName = new FormData();
    fdlFileName.left = new FormAttachment( 0, 0 );
    fdlFileName.right = new FormAttachment( middle, -margin );
    fdlFileName.top = new FormAttachment( wStepname, margin );
    wlFileName.setLayoutData( fdlFileName );

    wFileName = new CCombo( wSourceFileGroup, SWT.BORDER | SWT.READ_ONLY );
    wFileName.setEditable( true );
    props.setLook( wFileName );
    wFileName.addModifyListener( lsMod );
    fdfileName = new FormData();
    fdfileName.left = new FormAttachment( middle, 0 );
    fdfileName.top = new FormAttachment( wStepname, margin );
    fdfileName.right = new FormAttachment( 100, -margin );
    wFileName.setLayoutData( fdfileName );
    wFileName.addFocusListener( new FocusListener() {
      public void focusLost( org.eclipse.swt.events.FocusEvent e ) {
      }

      public void focusGained( org.eclipse.swt.events.FocusEvent e ) {
        get();
      }
    } );

    wlSourceEncoding = new Label( wSourceFileGroup, SWT.RIGHT );
    wlSourceEncoding.setText( BaseMessages.getString( PKG, "ChangeFileEncodingDialog.SourceEncoding.Label" ) );
    props.setLook( wlSourceEncoding );
    fdlSourceEncoding = new FormData();
    fdlSourceEncoding.left = new FormAttachment( 0, 0 );
    fdlSourceEncoding.top = new FormAttachment( wFileName, margin );
    fdlSourceEncoding.right = new FormAttachment( middle, -margin );
    wlSourceEncoding.setLayoutData( fdlSourceEncoding );
    wSourceEncoding = new ComboVar( transMeta, wSourceFileGroup, SWT.BORDER | SWT.READ_ONLY );
    wSourceEncoding.setEditable( true );
    props.setLook( wSourceEncoding );
    wSourceEncoding.addModifyListener( lsMod );
    fdSourceEncoding = new FormData();
    fdSourceEncoding.left = new FormAttachment( middle, 0 );
    fdSourceEncoding.top = new FormAttachment( wFileName, margin );
    fdSourceEncoding.right = new FormAttachment( 100, 0 );
    wSourceEncoding.setLayoutData( fdSourceEncoding );
    wSourceEncoding.addFocusListener( new FocusListener() {
      public void focusLost( org.eclipse.swt.events.FocusEvent e ) {
      }

      public void focusGained( org.eclipse.swt.events.FocusEvent e ) {
        setEncodings( wSourceEncoding );
      }
    } );

    // Add filename to result filenames?
    wlSourceAddResult = new Label( wSourceFileGroup, SWT.RIGHT );
    wlSourceAddResult.setText( BaseMessages.getString( PKG, "ChangeFileEncodingDialog.AddSourceResult.Label" ) );
    props.setLook( wlSourceAddResult );
    fdlSourceAddResult = new FormData();
    fdlSourceAddResult.left = new FormAttachment( 0, 0 );
    fdlSourceAddResult.top = new FormAttachment( wSourceEncoding, margin );
    fdlSourceAddResult.right = new FormAttachment( middle, -margin );
    wlSourceAddResult.setLayoutData( fdlSourceAddResult );
    wSourceAddResult = new Button( wSourceFileGroup, SWT.CHECK );
    props.setLook( wSourceAddResult );
    wSourceAddResult
      .setToolTipText( BaseMessages.getString( PKG, "ChangeFileEncodingDialog.AddSourceResult.Tooltip" ) );
    fdSourceAddResult = new FormData();
    fdSourceAddResult.left = new FormAttachment( middle, 0 );
    fdSourceAddResult.top = new FormAttachment( wSourceEncoding, margin );
    wSourceAddResult.setLayoutData( fdSourceAddResult );
    wSourceAddResult.addSelectionListener( new ComponentSelectionListener( input ) );

    fdSourceFileGroup = new FormData();
    fdSourceFileGroup.left = new FormAttachment( 0, margin );
    fdSourceFileGroup.top = new FormAttachment( wStepname, margin );
    fdSourceFileGroup.right = new FormAttachment( 100, -margin );
    wSourceFileGroup.setLayoutData( fdSourceFileGroup );

    // ///////////////////////////////////////////////////////////
    // / END OF SourceFile GROUP
    // ///////////////////////////////////////////////////////////

    // /////////////////////////////////
    // START OF TargetFile GROUP
    // /////////////////////////////////

    wTargetFileGroup = new Group( shell, SWT.SHADOW_NONE );
    props.setLook( wTargetFileGroup );
    wTargetFileGroup.setText( BaseMessages.getString( PKG, "ChangeFileEncodingDialog.Group.TargetFileGroup.Label" ) );

    FormLayout TargetFilegroupLayout = new FormLayout();
    TargetFilegroupLayout.marginWidth = 10;
    TargetFilegroupLayout.marginHeight = 10;
    wTargetFileGroup.setLayout( TargetFilegroupLayout );

    // TargetFileName field
    wlTargetFileName = new Label( wTargetFileGroup, SWT.RIGHT );
    wlTargetFileName.setText( BaseMessages.getString( PKG, "ChangeFileEncodingDialog.TargetFileName.Label" ) );
    props.setLook( wlTargetFileName );
    fdlTargetFileName = new FormData();
    fdlTargetFileName.left = new FormAttachment( 0, 0 );
    fdlTargetFileName.right = new FormAttachment( middle, -margin );
    fdlTargetFileName.top = new FormAttachment( wSourceEncoding, margin );
    wlTargetFileName.setLayoutData( fdlTargetFileName );

    wTargetFileName = new CCombo( wTargetFileGroup, SWT.BORDER | SWT.READ_ONLY );
    wTargetFileName.setEditable( true );
    props.setLook( wTargetFileName );
    wTargetFileName.addModifyListener( lsMod );
    fdTargetFileName = new FormData();
    fdTargetFileName.left = new FormAttachment( middle, 0 );
    fdTargetFileName.top = new FormAttachment( wSourceEncoding, margin );
    fdTargetFileName.right = new FormAttachment( 100, -margin );
    wTargetFileName.setLayoutData( fdTargetFileName );
    wTargetFileName.addFocusListener( new FocusListener() {
      public void focusLost( org.eclipse.swt.events.FocusEvent e ) {
      }

      public void focusGained( org.eclipse.swt.events.FocusEvent e ) {
        get();
      }
    } );

    // Create parent folder
    wlCreateParentFolder = new Label( wTargetFileGroup, SWT.RIGHT );
    wlCreateParentFolder.setText( BaseMessages.getString( PKG, "ChangeFileEncodingDialog.CreateParentFolder.Label" ) );
    props.setLook( wlCreateParentFolder );
    fdlCreateParentFolder = new FormData();
    fdlCreateParentFolder.left = new FormAttachment( 0, 0 );
    fdlCreateParentFolder.top = new FormAttachment( wTargetFileName, margin );
    fdlCreateParentFolder.right = new FormAttachment( middle, -margin );
    wlCreateParentFolder.setLayoutData( fdlCreateParentFolder );
    wCreateParentFolder = new Button( wTargetFileGroup, SWT.CHECK );
    props.setLook( wCreateParentFolder );
    wCreateParentFolder
      .setToolTipText( BaseMessages.getString( PKG, "ChangeFileEncodingDialog.CreateParentFolder.Tooltip" ) );
    fdCreateParentFolder = new FormData();
    fdCreateParentFolder.left = new FormAttachment( middle, 0 );
    fdCreateParentFolder.top = new FormAttachment( wTargetFileName, margin );
    wCreateParentFolder.setLayoutData( fdCreateParentFolder );
    wCreateParentFolder.addSelectionListener( new ComponentSelectionListener( input ) );

    wlTargetEncoding = new Label( wTargetFileGroup, SWT.RIGHT );
    wlTargetEncoding.setText( BaseMessages.getString( PKG, "ChangeFileEncodingDialog.TargetEncoding.Label" ) );
    props.setLook( wlTargetEncoding );
    fdlTargetEncoding = new FormData();
    fdlTargetEncoding.left = new FormAttachment( 0, 0 );
    fdlTargetEncoding.top = new FormAttachment( wCreateParentFolder, margin );
    fdlTargetEncoding.right = new FormAttachment( middle, -margin );
    wlTargetEncoding.setLayoutData( fdlTargetEncoding );
    wTargetEncoding = new ComboVar( transMeta, wTargetFileGroup, SWT.BORDER | SWT.READ_ONLY );
    wTargetEncoding.setEditable( true );
    props.setLook( wTargetEncoding );
    wTargetEncoding.addModifyListener( lsMod );
    fdTargetEncoding = new FormData();
    fdTargetEncoding.left = new FormAttachment( middle, 0 );
    fdTargetEncoding.top = new FormAttachment( wCreateParentFolder, margin );
    fdTargetEncoding.right = new FormAttachment( 100, 0 );
    wTargetEncoding.setLayoutData( fdTargetEncoding );
    wTargetEncoding.addFocusListener( new FocusListener() {
      public void focusLost( org.eclipse.swt.events.FocusEvent e ) {
      }

      public void focusGained( org.eclipse.swt.events.FocusEvent e ) {
        setEncodings( wTargetEncoding );
      }
    } );
    // Add filename to result filenames?
    wlTargetAddResult = new Label( wTargetFileGroup, SWT.RIGHT );
    wlTargetAddResult.setText( BaseMessages.getString( PKG, "ChangeFileEncodingDialog.AddTargetResult.Label" ) );
    props.setLook( wlTargetAddResult );
    fdlTargetAddResult = new FormData();
    fdlTargetAddResult.left = new FormAttachment( 0, 0 );
    fdlTargetAddResult.top = new FormAttachment( wTargetEncoding, margin );
    fdlTargetAddResult.right = new FormAttachment( middle, -margin );
    wlTargetAddResult.setLayoutData( fdlTargetAddResult );
    wTargetAddResult = new Button( wTargetFileGroup, SWT.CHECK );
    props.setLook( wTargetAddResult );
    wTargetAddResult
      .setToolTipText( BaseMessages.getString( PKG, "ChangeFileEncodingDialog.AddTargetResult.Tooltip" ) );
    fdTargetAddResult = new FormData();
    fdTargetAddResult.left = new FormAttachment( middle, 0 );
    fdTargetAddResult.top = new FormAttachment( wTargetEncoding, margin );
    wTargetAddResult.setLayoutData( fdTargetAddResult );
    wTargetAddResult.addSelectionListener( new ComponentSelectionListener( input ) );

    fdTargetFileGroup = new FormData();
    fdTargetFileGroup.left = new FormAttachment( 0, margin );
    fdTargetFileGroup.top = new FormAttachment( wSourceFileGroup, margin );
    fdTargetFileGroup.right = new FormAttachment( 100, -margin );
    wTargetFileGroup.setLayoutData( fdTargetFileGroup );

    // ///////////////////////////////////////////////////////////
    // / END OF TargetFile GROUP
    // ///////////////////////////////////////////////////////////

    // THE BUTTONS
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] {
      wOK, wCancel }, margin, wTargetFileGroup );

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
    input.setDynamicFilenameField( wFileName.getText() );
    input.setTargetFilenameField( wTargetFileName.getText() );
    input.setSourceEncoding( wSourceEncoding.getText() );
    input.setTargetEncoding( wTargetEncoding.getText() );
    input.setaddSourceResultFilenames( wSourceAddResult.getSelection() );
    input.setaddTargetResultFilenames( wTargetAddResult.getSelection() );
    input.setCreateParentFolder( wCreateParentFolder.getSelection() );

    stepname = wStepname.getText(); // return value

    dispose();
  }

  private void get() {
    if ( !gotPreviousFields ) {
      try {
        String filefield = wFileName.getText();
        String targetfilefield = wTargetFileName.getText();
        wFileName.removeAll();
        wTargetFileName.removeAll();
        RowMetaInterface r = transMeta.getPrevStepFields( stepname );
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
    ArrayList<Charset> values = new ArrayList<Charset>( Charset.availableCharsets().values() );
    for ( int i = 0; i < values.size(); i++ ) {
      Charset charSet = values.get( i );
      var.add( charSet.displayName() );
    }

    // Now select the default!
    int idx = Const.indexOfString( encoding, var.getItems() );
    if ( idx >= 0 ) {
      var.select( idx );
    }
  }
}
