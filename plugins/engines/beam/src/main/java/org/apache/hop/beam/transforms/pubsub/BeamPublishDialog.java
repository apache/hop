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

package org.apache.hop.beam.transforms.pubsub;

import org.apache.hop.beam.core.BeamDefaults;
import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

public class BeamPublishDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = BeamPublish.class; // For Translator
  private final BeamPublishMeta input;

  int middle;
  int margin;

  private TextVar wTopic;
  private Combo wMessageType;
  private TextVar wMessageField;

  public BeamPublishDialog( Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (BeamPublishMeta) in;
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "BeamPublishDialog.DialogTitle" ) );

    middle = props.getMiddlePct();
    margin = Const.MARGIN;

    // TransformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "System.Label.TransformName" ) );
    props.setLook( wlTransformName );
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment( 0, 0 );
    fdlTransformName.top = new FormAttachment( 0, margin );
    fdlTransformName.right = new FormAttachment( middle, -margin );
    wlTransformName.setLayoutData( fdlTransformName );
    wTransformName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wTransformName.setText( transformName );
    props.setLook( wTransformName );
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment( middle, 0 );
    fdTransformName.top = new FormAttachment( wlTransformName, 0, SWT.CENTER );
    fdTransformName.right = new FormAttachment( 100, 0 );
    wTransformName.setLayoutData( fdTransformName );
    Control lastControl = wTransformName;

    Label wlTopic = new Label( shell, SWT.RIGHT );
    wlTopic.setText( BaseMessages.getString( PKG, "BeamPublishDialog.Topic" ) );
    props.setLook( wlTopic );
    FormData fdlTopic = new FormData();
    fdlTopic.left = new FormAttachment( 0, 0 );
    fdlTopic.top = new FormAttachment( lastControl, margin );
    fdlTopic.right = new FormAttachment( middle, -margin );
    wlTopic.setLayoutData( fdlTopic );
    wTopic = new TextVar( variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTopic );
    FormData fdTopic = new FormData();
    fdTopic.left = new FormAttachment( middle, 0 );
    fdTopic.top = new FormAttachment( wlTopic, 0, SWT.CENTER );
    fdTopic.right = new FormAttachment( 100, 0 );
    wTopic.setLayoutData( fdTopic );
    lastControl = wTopic;

    Label wlMessageType = new Label( shell, SWT.RIGHT );
    wlMessageType.setText( BaseMessages.getString( PKG, "BeamPublishDialog.MessageType" ) );
    props.setLook( wlMessageType );
    FormData fdlMessageType = new FormData();
    fdlMessageType.left = new FormAttachment( 0, 0 );
    fdlMessageType.top = new FormAttachment( lastControl, margin );
    fdlMessageType.right = new FormAttachment( middle, -margin );
    wlMessageType.setLayoutData( fdlMessageType );
    wMessageType = new Combo( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wMessageType );
    wMessageType.setItems( BeamDefaults.PUBSUB_MESSAGE_TYPES);
    FormData fdMessageType = new FormData();
    fdMessageType.left = new FormAttachment( middle, 0 );
    fdMessageType.top = new FormAttachment( wlMessageType, 0, SWT.CENTER );
    fdMessageType.right = new FormAttachment( 100, 0 );
    wMessageType.setLayoutData( fdMessageType );
    lastControl = wMessageType;

    Label wlMessageField = new Label( shell, SWT.RIGHT );
    wlMessageField.setText( BaseMessages.getString( PKG, "BeamPublishDialog.MessageField" ) );
    props.setLook( wlMessageField );
    FormData fdlMessageField = new FormData();
    fdlMessageField.left = new FormAttachment( 0, 0 );
    fdlMessageField.top = new FormAttachment( lastControl, margin );
    fdlMessageField.right = new FormAttachment( middle, -margin );
    wlMessageField.setLayoutData( fdlMessageField );
    wMessageField = new TextVar( variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wMessageField );
    FormData fdMessageField = new FormData();
    fdMessageField.left = new FormAttachment( middle, 0 );
    fdMessageField.top = new FormAttachment( wlMessageField, 0, SWT.CENTER );
    fdMessageField.right = new FormAttachment( 100, 0 );
    wMessageField.setLayoutData( fdMessageField );
    lastControl = wMessageField;

    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOk, wCancel }, margin, lastControl );


    wOk.addListener( SWT.Selection, e -> ok() );
    wCancel.addListener( SWT.Selection, e -> cancel() );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wTransformName.addSelectionListener( lsDef );
    wMessageType.addSelectionListener( lsDef );
    wTopic.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addListener( SWT.Close, e->cancel());

    getData();
    setSize();
    input.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformName;
  }


  /**
   * Populate the widgets.
   */
  public void getData() {
    wTransformName.setText( transformName );
    wTopic.setText( Const.NVL( input.getTopic(), "" ) );
    wMessageType.setText( Const.NVL( input.getMessageType(), "" ) );
    wMessageField.setText( Const.NVL( input.getMessageField(), "" ) );

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    input.setChanged( changed );
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( wTransformName.getText() ) ) {
      return;
    }

    getInfo( input );

    dispose();
  }

  private void getInfo( BeamPublishMeta in ) {
    transformName = wTransformName.getText(); // return value

    in.setTopic( wTopic.getText() );
    in.setMessageType( wMessageType.getText() );
    in.setMessageField( wMessageField.getText() );

    input.setChanged();
  }
}
