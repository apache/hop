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
import org.apache.hop.ui.core.PropsUi;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.TraverseListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Text;

/**
 * Adds a line of text with a label and a variable to a composite (like a dialog shell)
 *
 * @author Matt
 * @since 17-may-2006
 */
public class LabelTextVar extends Composite {
  private static final PropsUi props = PropsUi.getInstance();

  private Label wLabel;
  private TextVar wText;

  public LabelTextVar( IVariables variables, Composite composite, String labelText, String toolTipText ) {
    this( variables, composite, SWT.NONE, labelText, toolTipText );
  }

  public LabelTextVar( IVariables variables, Composite composite, String labelText, String toolTipText, boolean passwordField ) {
    this( variables, composite, SWT.NONE, labelText, toolTipText, passwordField, true );
  }

  public LabelTextVar( IVariables variables, Composite composite, int flags, String labelText, String toolTipText ) {
    this( variables, composite, flags, labelText, toolTipText, false, true );
  }

  public LabelTextVar( IVariables variables, Composite composite, int flags, String labelText, String toolTipText,
                       boolean passwordField, boolean rightAligned ) {
    super( composite, SWT.NONE );
    props.setLook( this );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = 0;
    formLayout.marginHeight = 0;
    formLayout.marginTop = 0;
    formLayout.marginBottom = 0;

    this.setLayout( formLayout );

    int textFlags = SWT.SINGLE | SWT.LEFT | SWT.BORDER;
    if ( flags != SWT.NONE ) {
      textFlags = flags;
    }

    if ( passwordField ) {
      wText = new PasswordTextVar( variables, this, textFlags, toolTipText );
    } else {
      wText = new TextVar( variables, this, textFlags, toolTipText );
    }
    FormData fdText = new FormData();
    if (rightAligned) {
      fdText.left = new FormAttachment(middle, margin);
    } else {
      fdText.left = new FormAttachment(middle, 0);
    }
    fdText.right = new FormAttachment( 100, 0 );
    wText.setLayoutData( fdText );
    wText.getTextWidget().setToolTipText( toolTipText );

    wLabel = new Label( this, SWT.RIGHT );
    props.setLook( wLabel );
    wLabel.setText( labelText );
    FormData fdLabel = new FormData();
    fdLabel.left = new FormAttachment(0, 0);
    if (rightAligned) {
      fdLabel.right = new FormAttachment(middle, 0);
    } else {
      fdLabel.right = new FormAttachment(middle, -margin);
    }
    fdLabel.top = new FormAttachment( wText, 0, SWT.CENTER );
    wLabel.setLayoutData( fdLabel );
    wLabel.setToolTipText( toolTipText );
  }

  public void addModifyListener( ModifyListener lsMod ) {
    wText.addModifyListener( lsMod );
  }

  public void addSelectionListener( SelectionAdapter lsDef ) {
    wText.addSelectionListener( lsDef );
  }

  public void setText( String name ) {
    wText.setText( name );
  }

  public String getText() {
    return wText.getText();
  }

  public void setEchoChar( char c ) {
    wText.setEchoChar( c );
  }

  public void setEnabled( boolean flag ) {
    wText.setEnabled( flag );
    wLabel.setEnabled( flag );
  }

  public boolean setFocus() {
    return wText.setFocus();
  }

  public void addTraverseListener( TraverseListener tl ) {
    wText.addTraverseListener( tl );
  }

  public Text getTextWidget() {
    return wText.getTextWidget();
  }

  public Label getLabelWidget() {
    return wLabel;
  }
}
