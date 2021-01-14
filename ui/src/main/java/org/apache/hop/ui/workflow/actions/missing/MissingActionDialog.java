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

package org.apache.hop.ui.workflow.actions.missing;

import java.util.List;

import org.apache.hop.core.Const;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.IActionDialog;
import org.apache.hop.workflow.actions.missing.MissingAction;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

public class MissingActionDialog extends ActionDialog implements IActionDialog {
  private static final Class<?> PKG = MissingActionDialog.class; // For Translator

  private Shell shell;
  private List<MissingAction> missingActions;
  private int mode;
  private PropsUi props;

  /**
   * A reference to the action interface
   */
  protected IAction action;
  
  public static final int MISSING_ACTIONS = 1;
  public static final int MISSING_ACTION_ID = 2;

  public MissingActionDialog( Shell parent, List<MissingAction> missingActions ) {
    super( parent, null );
   
    this.missingActions = missingActions;
    this.mode = MISSING_ACTIONS;
  }

  public MissingActionDialog( Shell parent, IAction action, WorkflowMeta workflowMeta ) {
    super( parent, workflowMeta );
    this.action = action;
    this.mode = MISSING_ACTION_ID;
  }

  private String getErrorMessage( List<MissingAction> missingEntries, int mode ) {
    String message = "";
    if ( mode == MISSING_ACTIONS ) {
      StringBuilder entries = new StringBuilder();
      for ( MissingAction action : missingEntries ) {
        if ( missingEntries.indexOf( action ) == missingEntries.size() - 1 ) {
          entries.append( "- " + action.getName() + " - " + action.getMissingPluginId() + "\n\n" );
        } else {
          entries.append( "- " + action.getName() + " - " + action.getMissingPluginId() + "\n" );
        }
      }
      message = BaseMessages.getString( PKG, "MissingActionDialog.MissingActions", entries.toString() );
    }

    if ( mode == MISSING_ACTION_ID ) {
      message =
        BaseMessages.getString( PKG, "MissingActionDialog.MissingActionId", action.getName() + " - "
          + ( (MissingAction) action ).getMissingPluginId() );
    }
    return message;
  }

  public IAction open() {

    this.props = PropsUi.getInstance();
    
    Shell parent = this.getParent();
    Display display = parent.getDisplay();
    int margin = props.getMargin();

    shell =
      new Shell( parent, SWT.DIALOG_TRIM | SWT.CLOSE | SWT.ICON
        | SWT.APPLICATION_MODAL );

    props.setLook( shell );
    shell.setImage( GuiResource.getInstance().getImageHopUi() );

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginLeft = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setText( BaseMessages.getString( PKG, "MissingActionDialog.MissingPlugins" ) );
    shell.setLayout( formLayout );

    Label image = new Label( shell, SWT.NONE );
    props.setLook( image );
    Image icon = display.getSystemImage( SWT.ICON_QUESTION );
    image.setImage( icon );
    FormData imageData = new FormData();
    imageData.left = new FormAttachment( 0, 5 );
    imageData.right = new FormAttachment( 11, 0 );
    imageData.top = new FormAttachment( 0, 10 );
    image.setLayoutData( imageData );

    Label error = new Label( shell, SWT.WRAP );
    props.setLook( error );
    error.setText( getErrorMessage( missingActions, mode ) );
    FormData errorData = new FormData();
    errorData.left = new FormAttachment( image, 5 );
    errorData.right = new FormAttachment( 100, -5 );
    errorData.top = new FormAttachment( 0, 10 );
    error.setLayoutData( errorData );

    Label separator = new Label( shell, SWT.WRAP );
    props.setLook( separator );
    FormData separatorData = new FormData();
    separatorData.top = new FormAttachment( error, 10 );
    separator.setLayoutData( separatorData );

    Button closeButton = new Button( shell, SWT.PUSH );
    props.setLook( closeButton );
    FormData fdClose = new FormData();
    fdClose.right = new FormAttachment( 98 );
    fdClose.top = new FormAttachment( separator );
    closeButton.setLayoutData( fdClose );
    closeButton.setText( BaseMessages.getString( PKG, "MissingActionDialog.Close" ) );
    closeButton.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        shell.dispose();
        action = null;
      }
    } );

    FormData fdSearch = new FormData();
    if ( this.mode == MISSING_ACTIONS ) {
      Button openButton = new Button( shell, SWT.PUSH );
      props.setLook( openButton );
      FormData fdOpen = new FormData();
      fdOpen.right = new FormAttachment( closeButton, -5 );
      fdOpen.bottom = new FormAttachment( closeButton, 0, SWT.BOTTOM );
      openButton.setLayoutData( fdOpen );
      openButton.setText( BaseMessages.getString( PKG, "MissingActionDialog.OpenFile" ) );
      openButton.addSelectionListener( new SelectionAdapter() {
        public void widgetSelected( SelectionEvent e ) {
          shell.dispose();
          action = new MissingAction();
        }
      } );
      fdSearch.right = new FormAttachment( openButton, -5 );
      fdSearch.bottom = new FormAttachment( openButton, 0, SWT.BOTTOM );
    } else {
      fdSearch.right = new FormAttachment( closeButton, -5 );
      fdSearch.bottom = new FormAttachment( closeButton, 0, SWT.BOTTOM );
    }

    Button searchButton = new Button( shell, SWT.PUSH );
    props.setLook( searchButton );
    searchButton.setText( BaseMessages.getString( PKG, "MissingActionDialog.SearchMarketplace" ) );
    searchButton.setLayoutData( fdSearch );
    searchButton.setEnabled(false);
    searchButton.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        try {
          shell.dispose();
          // HopGui.getInstance().openMarketplace(); TODO : implement or replace marketplace
        } catch ( Exception ex ) {
          ex.printStackTrace();
        }
      }
    } );

    shell.pack();
    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return action;
  }
}
