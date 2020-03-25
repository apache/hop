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

package org.apache.hop.trans.steps.terafast;

import org.apache.hop.i18n.BaseMessages;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.program.Program;
import org.eclipse.swt.widgets.*;

/**
 * @author <a href="mailto:michael.gugerell@aschauer-edv.at">Michael Gugerell(asc145)</a>
 */
public class TeraFastAboutDialog {

  private static Class<?> PKG = TeraFastMeta.class; // for i18n purposes, needed by Translator2!!

  private static final int DEFAULT_INDENT = 20;

  private Shell dialog;
  private Link ascLink;
  private Label iconLabel;

  /**
   * @param shell the shell.
   */
  public TeraFastAboutDialog( final Shell shell ) {
    this.dialog = new Shell( shell, SWT.BORDER | SWT.CLOSE | SWT.APPLICATION_MODAL | SWT.SHEET );
    GridLayout gridLayout = new GridLayout();
    gridLayout.numColumns = 2;

    this.dialog.setLayout( gridLayout );
    this.dialog.setText( BaseMessages.getString( PKG, "TeraFastDialog.About.Shell.Title" ) );
    this.dialog.setImage( shell.getImage() );

    this.buildIconCell();
    this.buildPluginInfoCell();
    this.buildOkButton();

    this.dialog.pack();
    Rectangle shellBounds = shell.getBounds();
    Point dialogSize = this.dialog.getSize();

    this.dialog.setLocation( shellBounds.x + ( shellBounds.width - dialogSize.x ) / 2, shellBounds.y
      + ( shellBounds.height - dialogSize.y ) / 2 );
  }

  /**
   * open the dialog.
   */
  public void open() {
    this.dialog.open();
  }

  /**
   * build Ok Button.
   */
  protected void buildOkButton() {
    Button ok = new Button( this.dialog, SWT.PUSH );
    ok.setText( BaseMessages.getString( PKG, "TeraFastDialog.About.Plugin.Close" ) );
    GridData grdData = new GridData( GridData.HORIZONTAL_ALIGN_CENTER );
    grdData.horizontalSpan = 2;
    grdData.verticalIndent = DEFAULT_INDENT;
    grdData.horizontalIndent = DEFAULT_INDENT;
    ok.setLayoutData( grdData );

    ok.addListener( SWT.Selection, new Listener() {
      public void handleEvent( Event arg0 ) {
        dialog.dispose();
      }
    } );
  }

  /**
   * build icon cell
   */
  protected void buildIconCell() {
    Image icon = Display.getCurrent().getSystemImage( SWT.ICON_INFORMATION );
    this.iconLabel = new Label( this.dialog, SWT.NONE );
    this.iconLabel.setImage( icon );
    GridData grdData = new GridData();
    grdData.horizontalIndent = DEFAULT_INDENT;
    grdData.verticalIndent = DEFAULT_INDENT;
    this.iconLabel.setLayoutData( grdData );
  }

  /**
   * build plugin info cell.
   */
  protected void buildPluginInfoCell() {
    this.ascLink = new Link( this.dialog, SWT.NONE );
    this.ascLink.setText( BaseMessages.getString( PKG, "TeraFastDialog.About.Plugin.Info" ) );
    GridData grdData = new GridData();
    grdData.horizontalIndent = DEFAULT_INDENT;
    grdData.verticalIndent = DEFAULT_INDENT;
    this.ascLink.setLayoutData( grdData );

    this.ascLink.addListener( SWT.Selection, new Listener() {
      public void handleEvent( final Event event ) {
        Program.launch( event.text );
      }
    } );
  }
}
