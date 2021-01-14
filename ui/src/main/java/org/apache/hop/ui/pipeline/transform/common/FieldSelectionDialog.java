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

package org.apache.hop.ui.pipeline.transform.common;

import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

/**
 * A dialog with options to (re)load all fields, or only add new ones. Upon submitting the dialog, the "Sample data"
 * dialog is opened to load fields.
 */
public class FieldSelectionDialog extends BaseDialog {
  private static final Class<?> PKG = FieldSelectionDialog.class; // For Translator

  private int numFields;
  protected boolean reloadAllFields;

  private static final int WIDTH = 360;

  public FieldSelectionDialog( final Shell shell, final int numNewFields ) {
    super( shell, BaseMessages.getString( PKG, "FieldSelectionDialog.NewFieldsFound.Title" ), WIDTH );
    this.numFields = numNewFields;

    // Define buttons
    this.buttons.put( BaseMessages.getString( PKG, "System.Button.Cancel" ), event -> {
      cancel();
    } );
    this.buttons.put( BaseMessages.getString( PKG, "System.Button.OK" ), event -> {
      ok();
    } );
  }

  @Override
  protected Control buildBody() {

    final Label message = new Label( shell, SWT.WRAP | SWT.LEFT );
    message.setText( BaseMessages.getString( PKG, "FieldSelectionDialog.NewFieldsFound.Message", numFields ) );
    props.setLook( message );
    message.setLayoutData( new FormDataBuilder().top().left().right( 100, 0 ).result() );

    final Button newFieldsOnly = new Button( shell, SWT.RADIO );
    newFieldsOnly.setSelection( true );
    props.setLook( newFieldsOnly );
    newFieldsOnly.setText( BaseMessages.getString( PKG, "FieldSelectionDialog.AddNewOnly.Label" ) );
    newFieldsOnly.setLayoutData( new FormDataBuilder().top( message, MARGIN_SIZE ).left().result() );

    final Button clearAndAddAll = new Button( shell, SWT.RADIO );
    props.setLook( clearAndAddAll );
    clearAndAddAll.setText( BaseMessages.getString( PKG, "FieldSelectionDialog.ClearAndAddAll.Label" ) );
    clearAndAddAll.setLayoutData( new FormDataBuilder().top( newFieldsOnly, ELEMENT_SPACING ).left().result() );

    newFieldsOnly.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent event ) {
        newFieldsOnly.setSelection( true );
        clearAndAddAll.setSelection( false );
        reloadAllFields = false;
      }
    } );
    clearAndAddAll.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent event ) {
        clearAndAddAll.setSelection( true );
        newFieldsOnly.setSelection( false );
        reloadAllFields = true;
      }
    } );

    return clearAndAddAll;
  }

  /**
   * Override to provide specific behavior, other than just disposing the dialog.
   */
  protected void cancel() {
    dispose();
  }

  /**
   * Override to provide specific behavior, other than just disposing the dialog.
   */
  protected void ok() {
    dispose();
  }
}
