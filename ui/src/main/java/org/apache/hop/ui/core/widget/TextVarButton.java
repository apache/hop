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

package org.apache.hop.ui.core.widget;

import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUI;
import org.apache.hop.ui.core.gui.GUIResource;
import org.eclipse.jface.fieldassist.ControlDecoration;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Text;

public class TextVarButton extends TextVar {

  public TextVarButton( IVariables variables, Composite composite, int flags ) {
    super( variables, composite, flags );
  }

  public TextVarButton( IVariables variables, Composite composite, int flags, String toolTipText ) {
    super( variables, composite, flags, toolTipText );
  }

  public TextVarButton( IVariables variables, Composite composite, int flags,
                        IGetCaretPosition getCaretPositionInterface, IInsertText insertTextInterface ) {
    super( variables, composite, flags, getCaretPositionInterface, insertTextInterface );
  }

  public TextVarButton( IVariables variables, Composite composite, int flags,
                        IGetCaretPosition getCaretPositionInterface, IInsertText insertTextInterface,
                        SelectionListener selectionListener ) {
    super( composite, variables, flags, getCaretPositionInterface, insertTextInterface, selectionListener );
  }

  public TextVarButton( IVariables variables, Composite composite, int flags, String toolTipText,
                        IGetCaretPosition getCaretPositionInterface, IInsertText insertTextInterface ) {
    super( variables, composite, flags, toolTipText, getCaretPositionInterface, insertTextInterface );
  }

  protected void initialize( IVariables variables, Composite composite, int flags, String toolTipText,
                             IGetCaretPosition getCaretPositionInterface, IInsertText insertTextInterface,
                             SelectionListener selectionListener ) {
    this.toolTipText = toolTipText;
    this.getCaretPositionInterface = getCaretPositionInterface;
    this.insertTextInterface = insertTextInterface;
    this.variables = variables;

    PropsUI.getInstance().setLook( this );

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = 0;
    formLayout.marginHeight = 0;
    formLayout.marginTop = 0;
    formLayout.marginBottom = 0;
    this.setLayout( formLayout );

    Button button = new Button( this, SWT.PUSH );
    PropsUI.getInstance().setLook( button );
    button.setText( "..." );
    FormData fdButton = new FormData();
    fdButton.top = new FormAttachment( 0, 0 );
    fdButton.right = new FormAttachment( 100, 0 );
    fdButton.bottom = new FormAttachment( 100 );
    fdButton.width = 30;
    button.setLayoutData( fdButton );
    if ( selectionListener != null ) {
      button.addSelectionListener( selectionListener );
    }

    wText = new Text( this, flags );
    controlDecoration = new ControlDecoration( wText, SWT.CENTER | SWT.RIGHT, this );
    Image image = GUIResource.getInstance().getImageVariable();
    controlDecoration.setImage( image );
    controlDecoration.setDescriptionText( BaseMessages.getString( PKG, "TextVar.tooltip.InsertVariable" ) );
    PropsUI.getInstance().setLook( controlDecoration.getControl() );

    modifyListenerTooltipText = getModifyListenerTooltipText( wText );
    wText.addModifyListener( modifyListenerTooltipText );

    controlSpaceKeyAdapter =
      new ControlSpaceKeyAdapter( variables, wText, getCaretPositionInterface, insertTextInterface );
    wText.addKeyListener( controlSpaceKeyAdapter );

    FormData fdText = new FormData();
    fdText.top = new FormAttachment( 0, 0 );
    fdText.left = new FormAttachment( 0, 0 );
    fdText.right = new FormAttachment( button, -image.getBounds().width );
    wText.setLayoutData( fdText );
  }

}
