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

package org.apache.hop.ui.core.widget;

import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.eclipse.jface.fieldassist.ControlDecoration;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Text;

public class TextVarButton extends TextVar {

  public TextVarButton( IVariables variables, Composite composite, int flags,
                        IGetCaretPosition getCaretPositionInterface, IInsertText insertTextInterface,
                        SelectionListener selectionListener ) {
    super( composite, variables, flags, getCaretPositionInterface, insertTextInterface, selectionListener );
  }

  protected void initialize( IVariables variables, Composite composite, int flags, String toolTipText,
                             IGetCaretPosition getCaretPositionInterface, IInsertText insertTextInterface,
                             SelectionListener selectionListener ) {
    this.toolTipText = toolTipText;
    this.getCaretPositionInterface = getCaretPositionInterface;
    this.insertTextInterface = insertTextInterface;
    this.variables = variables;

    PropsUi.getInstance().setLook( this );

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = 0;
    formLayout.marginHeight = 0;
    formLayout.marginTop = 0;
    formLayout.marginBottom = 0;
    this.setLayout( formLayout );

    Button button = new Button( this, SWT.PUSH );
    PropsUi.getInstance().setLook( button );
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

    // Add the variable $ image on the top right of the control
    //
    Label wlImage = new Label( this, SWT.NONE );
    wlImage.setImage( GuiResource.getInstance().getImageVariable() );
    wlImage.setToolTipText( BaseMessages.getString( PKG, "TextVar.tooltip.InsertVariable" ) );
    FormData fdlImage = new FormData();
    fdlImage.top = new FormAttachment( 0, 0 );
    fdlImage.right = new FormAttachment( button, 0 );
    wlImage.setLayoutData( fdlImage );

    // add a text field on it...
    wText = new Text( this, flags );
    FormData fdText = new FormData();
    fdText.top = new FormAttachment( 0, 0 );
    fdText.left = new FormAttachment( 0, 0 );
    fdText.right = new FormAttachment( wlImage, 0 );
    fdText.bottom = new FormAttachment( 100, 0 );
    wText.setLayoutData( fdText );

    modifyListenerTooltipText = getModifyListenerTooltipText( wText );
    wText.addModifyListener( modifyListenerTooltipText );

    controlSpaceKeyAdapter =
      new ControlSpaceKeyAdapter( variables, wText, getCaretPositionInterface, insertTextInterface );
    wText.addKeyListener( controlSpaceKeyAdapter );

  }

}
