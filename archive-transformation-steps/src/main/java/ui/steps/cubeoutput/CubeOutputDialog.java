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

package org.apache.hop.ui.trans.steps.cubeoutput;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.BaseStepMeta;
import org.apache.hop.trans.step.StepDialogInterface;
import org.apache.hop.trans.steps.cubeoutput.CubeOutputMeta;
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
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

public class CubeOutputDialog extends BaseStepDialog implements StepDialogInterface {
  private static Class<?> PKG = CubeOutputMeta.class; // for i18n purposes, needed by Translator2!!

  private Label wlFilename;
  private Button wbFilename;
  private TextVar wFilename;
  private FormData fdlFilename, fdbFilename, fdFilename;
  private Label wlAddToResult;
  private Button wAddToResult;
  private FormData fdlAddToResult, fdAddToResult;

  private Label wlDoNotOpenNewFileInit;
  private Button wDoNotOpenNewFileInit;
  private FormData fdlDoNotOpenNewFileInit, fdDoNotOpenNewFileInit;

  private CubeOutputMeta input;

  public CubeOutputDialog( Shell parent, Object in, TransMeta transMeta, String sname ) {
    super( parent, (BaseStepMeta) in, transMeta, sname );
    input = (CubeOutputMeta) in;
    this.transMeta = transMeta;
    if ( sname != null ) {
      stepname = sname;
    } else {
      stepname = BaseMessages.getString( PKG, "CubeOutputDialog.DefaultStepName" );
    }
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
    shell.setText( BaseMessages.getString( PKG, "CubeOutputDialog.Shell.Text" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // Stepname line
    wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname.setText( BaseMessages.getString( PKG, "CubeOutputDialog.Stepname.Label" ) );
    props.setLook( wlStepname );
    fdlStepname = new FormData();
    fdlStepname.left = new FormAttachment( 0, 0 );
    fdlStepname.top = new FormAttachment( 0, margin );
    fdlStepname.right = new FormAttachment( middle, -margin );
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

    // Filename line
    wlFilename = new Label( shell, SWT.RIGHT );
    wlFilename.setText( BaseMessages.getString( PKG, "CubeOutputDialog.Filename.Label" ) );
    props.setLook( wlFilename );
    fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment( 0, 0 );
    fdlFilename.top = new FormAttachment( wStepname, margin + 5 );
    fdlFilename.right = new FormAttachment( middle, -margin );
    wlFilename.setLayoutData( fdlFilename );
    wbFilename = new Button( shell, SWT.PUSH | SWT.CENTER );
    props.setLook( wbFilename );
    wbFilename.setText( BaseMessages.getString( PKG, "CubeOutputDialog.Browse.Button" ) );
    fdbFilename = new FormData();
    fdbFilename.right = new FormAttachment( 100, 0 );
    fdbFilename.top = new FormAttachment( wStepname, margin + 5 );
    wbFilename.setLayoutData( fdbFilename );

    wFilename = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFilename );
    wFilename.addModifyListener( lsMod );
    fdFilename = new FormData();
    fdFilename.left = new FormAttachment( middle, 0 );
    fdFilename.top = new FormAttachment( wStepname, margin + 5 );
    fdFilename.right = new FormAttachment( wbFilename, -margin );
    wFilename.setLayoutData( fdFilename );

    // Open new File at Init
    wlDoNotOpenNewFileInit = new Label( shell, SWT.RIGHT );
    wlDoNotOpenNewFileInit.setText( BaseMessages.getString( PKG, "CubeOutputDialog.DoNotOpenNewFileInit.Label" ) );
    props.setLook( wlDoNotOpenNewFileInit );
    fdlDoNotOpenNewFileInit = new FormData();
    fdlDoNotOpenNewFileInit.left = new FormAttachment( 0, 0 );
    fdlDoNotOpenNewFileInit.top = new FormAttachment( wFilename, 2 * margin );
    fdlDoNotOpenNewFileInit.right = new FormAttachment( middle, -margin );
    wlDoNotOpenNewFileInit.setLayoutData( fdlDoNotOpenNewFileInit );
    wDoNotOpenNewFileInit = new Button( shell, SWT.CHECK );
    wDoNotOpenNewFileInit.setToolTipText( BaseMessages.getString(
      PKG, "CubeOutputDialog.DoNotOpenNewFileInit.Tooltip" ) );
    props.setLook( wDoNotOpenNewFileInit );
    fdDoNotOpenNewFileInit = new FormData();
    fdDoNotOpenNewFileInit.left = new FormAttachment( middle, 0 );
    fdDoNotOpenNewFileInit.top = new FormAttachment( wFilename, 2 * margin );
    fdDoNotOpenNewFileInit.right = new FormAttachment( 100, 0 );
    wDoNotOpenNewFileInit.setLayoutData( fdDoNotOpenNewFileInit );
    wDoNotOpenNewFileInit.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    // Add File to the result files name
    wlAddToResult = new Label( shell, SWT.RIGHT );
    wlAddToResult.setText( BaseMessages.getString( PKG, "CubeOutputDialog.AddFileToResult.Label" ) );
    props.setLook( wlAddToResult );
    fdlAddToResult = new FormData();
    fdlAddToResult.left = new FormAttachment( 0, 0 );
    fdlAddToResult.top = new FormAttachment( wDoNotOpenNewFileInit, margin );
    fdlAddToResult.right = new FormAttachment( middle, -margin );
    wlAddToResult.setLayoutData( fdlAddToResult );
    wAddToResult = new Button( shell, SWT.CHECK );
    wAddToResult.setToolTipText( BaseMessages.getString( PKG, "CubeOutputDialog.AddFileToResult.Tooltip" ) );
    props.setLook( wAddToResult );
    fdAddToResult = new FormData();
    fdAddToResult.left = new FormAttachment( middle, 0 );
    fdAddToResult.top = new FormAttachment( wDoNotOpenNewFileInit, margin );
    fdAddToResult.right = new FormAttachment( 100, 0 );
    wAddToResult.setLayoutData( fdAddToResult );
    SelectionAdapter lsSelR = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        input.setChanged();
      }
    };
    wAddToResult.addSelectionListener( lsSelR );

    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOK, wCancel }, margin, wAddToResult );

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
    wFilename.addSelectionListener( lsDef );

    wbFilename.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        FileDialog dialog = new FileDialog( shell, SWT.SAVE );
        dialog.setFilterExtensions( new String[] { "*.cube", "*" } );
        if ( wFilename.getText() != null ) {
          dialog.setFileName( wFilename.getText() );
        }
        dialog.setFilterNames( new String[] {
          BaseMessages.getString( PKG, "CubeOutputDialog.FilterNames.Options.CubeFiles" ),
          BaseMessages.getString( PKG, "CubeOutputDialog.FilterNames.Options.AllFiles" ) } );
        if ( dialog.open() != null ) {
          wFilename.setText( dialog.getFilterPath()
            + System.getProperty( "file.separator" ) + dialog.getFileName() );
        }
      }
    } );

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
    if ( input.getFilename() != null ) {
      wFilename.setText( input.getFilename() );
    }
    wDoNotOpenNewFileInit.setSelection( input.isDoNotOpenNewFileInit() );
    wAddToResult.setSelection( input.isAddToResultFiles() );

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
    input.setAddToResultFiles( wAddToResult.getSelection() );
    input.setDoNotOpenNewFileInit( wDoNotOpenNewFileInit.getSelection() );
    input.setFilename( wFilename.getText() );

    dispose();
  }
}
