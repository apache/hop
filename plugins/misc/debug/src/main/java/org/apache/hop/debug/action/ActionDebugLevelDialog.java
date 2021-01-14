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

package org.apache.hop.debug.action;

import org.apache.hop.core.Const;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

public class ActionDebugLevelDialog extends Dialog {
  private static final Class<?> PKG = ActionDebugLevelDialog.class; // For Translator

  private ActionDebugLevel input;
  private ActionDebugLevel debugLevel;

  private Shell shell;

  // Connection properties
  //
  private Combo wLogLevel;
  private Button wLoggingResult;
  private Button wLoggingVariables;
  private Button wLoggingRows;
  private Button wLoggingFiles;

  private Control lastControl;

  private PropsUi props;

  private int middle;
  private int margin;

  private boolean ok;

  public ActionDebugLevelDialog( Shell par, ActionDebugLevel debugLevel ) {
    super( par, SWT.NONE );
    this.input = debugLevel;
    props = PropsUi.getInstance();
    ok = false;

    this.debugLevel = input.clone();
  }

  public boolean open() {
    Shell parent = getParent();
    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    shell.setImage( GuiResource.getInstance().getImageServer() );

    middle = props.getMiddlePct();
    margin = Const.MARGIN + 2;

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setText( "Workflow entry debug Level" );
    shell.setLayout( formLayout );

    // The name
    Label wlName = new Label( shell, SWT.RIGHT );
    props.setLook( wlName );
    wlName.setText( "Log level " );
    FormData fdlName = new FormData();
    fdlName.top = new FormAttachment( 0, margin );
    fdlName.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlName.right = new FormAttachment( middle, -margin );
    wlName.setLayoutData( fdlName );
    wLogLevel = new Combo( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wLogLevel.setItems( LogLevel.getLogLevelDescriptions() );
    props.setLook( wLogLevel );
    FormData fdName = new FormData();
    fdName.top = new FormAttachment( wlName, 0, SWT.CENTER );
    fdName.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdName.right = new FormAttachment( 100, 0 );
    wLogLevel.setLayoutData( fdName );
    lastControl = wLogLevel;

    // Log result details?
    Label wlLoggingResult = new Label( shell, SWT.RIGHT );
    props.setLook( wlLoggingResult );
    wlLoggingResult.setText( "Log result?" );
    FormData fdlLoggingResult = new FormData();
    fdlLoggingResult.top = new FormAttachment( lastControl, margin );
    fdlLoggingResult.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlLoggingResult.right = new FormAttachment( middle, -margin );
    wlLoggingResult.setLayoutData( fdlLoggingResult );
    wLoggingResult = new Button( shell, SWT.CHECK | SWT.LEFT );
    props.setLook( wLoggingResult );
    FormData fdLoggingResult = new FormData();
    fdLoggingResult.top = new FormAttachment( wlLoggingResult, 0, SWT.CENTER );
    fdLoggingResult.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdLoggingResult.right = new FormAttachment( 100, 0 );
    wLoggingResult.setLayoutData( fdLoggingResult );
    lastControl = wLoggingResult;

    // Log result details?
    Label wlLoggingVariables = new Label( shell, SWT.RIGHT );
    props.setLook( wlLoggingVariables );
    wlLoggingVariables.setText( "Log variables?" );
    FormData fdlLoggingVariables = new FormData();
    fdlLoggingVariables.top = new FormAttachment( lastControl, margin );
    fdlLoggingVariables.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlLoggingVariables.right = new FormAttachment( middle, -margin );
    wlLoggingVariables.setLayoutData( fdlLoggingVariables );
    wLoggingVariables = new Button( shell, SWT.CHECK | SWT.LEFT );
    props.setLook( wLoggingVariables );
    FormData fdLoggingVariables = new FormData();
    fdLoggingVariables.top = new FormAttachment( wlLoggingVariables, 0, SWT.CENTER );
    fdLoggingVariables.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdLoggingVariables.right = new FormAttachment( 100, 0 );
    wLoggingVariables.setLayoutData( fdLoggingVariables );
    lastControl = wLoggingVariables;

    // Log result details?
    Label wlLoggingRows = new Label( shell, SWT.RIGHT );
    props.setLook( wlLoggingRows );
    wlLoggingRows.setText( "Log result rows?" );
    FormData fdlLoggingRows = new FormData();
    fdlLoggingRows.top = new FormAttachment( lastControl, margin );
    fdlLoggingRows.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlLoggingRows.right = new FormAttachment( middle, -margin );
    wlLoggingRows.setLayoutData( fdlLoggingRows );
    wLoggingRows = new Button( shell, SWT.CHECK | SWT.LEFT );
    props.setLook( wLoggingRows );
    FormData fdLoggingRows = new FormData();
    fdLoggingRows.top = new FormAttachment( wlLoggingRows, 0, SWT.CENTER );
    fdLoggingRows.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdLoggingRows.right = new FormAttachment( 100, 0 );
    wLoggingRows.setLayoutData( fdLoggingRows );
    lastControl = wLoggingRows;

    // Log result details?
    Label wlLoggingFiles = new Label( shell, SWT.RIGHT );
    props.setLook( wlLoggingFiles );
    wlLoggingFiles.setText( "Log result files?" );
    FormData fdlLoggingFiles = new FormData();
    fdlLoggingFiles.top = new FormAttachment( lastControl, margin );
    fdlLoggingFiles.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlLoggingFiles.right = new FormAttachment( middle, -margin );
    wlLoggingFiles.setLayoutData( fdlLoggingFiles );
    wLoggingFiles = new Button( shell, SWT.CHECK | SWT.LEFT );
    props.setLook( wLoggingFiles );
    FormData fdLoggingFiles = new FormData();
    fdLoggingFiles.top = new FormAttachment( wlLoggingFiles, 0, SWT.CENTER );
    fdLoggingFiles.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdLoggingFiles.right = new FormAttachment( 100, 0 );
    wLoggingFiles.setLayoutData( fdLoggingFiles );
    lastControl = wLoggingFiles;

    // Buttons
    Button wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOK.addListener( SWT.Selection, e -> ok() );

    Button wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, e -> cancel() );

    Button[] buttons = new Button[] { wOK, wCancel };
    BaseTransformDialog.positionBottomButtons( shell, buttons, margin * 2, lastControl );

    SelectionAdapter selAdapter = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wLogLevel.addSelectionListener( selAdapter );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();

    BaseTransformDialog.setSize( shell );

    shell.open();
    Display display = parent.getDisplay();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return ok;
  }

  public void dispose() {
    props.setScreen( new WindowProperty( shell ) );
    shell.dispose();
  }

  public void getData() {
    wLogLevel.setText( debugLevel.getLogLevel().getDescription() );
    wLoggingResult.setSelection( debugLevel.isLoggingResult() );
    wLoggingVariables.setSelection( debugLevel.isLoggingVariables() );
    wLoggingRows.setSelection( debugLevel.isLoggingResultRows() );
    wLoggingFiles.setSelection( debugLevel.isLoggingResultFiles() );

    wLogLevel.setFocus();
  }

  private void cancel() {
    ok = false;
    dispose();
  }

  public void ok() {
    getInfo( input );
    ok = true;
    dispose();
  }

  // Get dialog info in securityService
  private void getInfo( ActionDebugLevel level ) {
    int index = Const.indexOfString( wLogLevel.getText(), LogLevel.getLogLevelDescriptions() );
    level.setLogLevel( LogLevel.values()[ index ] );
    level.setLoggingResult( wLoggingResult.getSelection() );
    level.setLoggingVariables( wLoggingVariables.getSelection() );
    level.setLoggingResultRows( wLoggingRows.getSelection() );
    level.setLoggingResultFiles( wLoggingFiles.getSelection() );
  }

}
