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

package org.apache.hop.beam.transforms.window;

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

public class BeamWindowDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = BeamWindowDialog.class; // For Translator
  private final BeamWindowMeta input;

  int middle;
  int margin;

  private Combo wWindowType;
  private TextVar wDuration;
  private TextVar wEvery;
  private TextVar wStartTimeField;
  private TextVar wEndTimeField;
  private TextVar wMaxTimeField;

  public BeamWindowDialog( Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (BeamWindowMeta) in;
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
    shell.setText( BaseMessages.getString( PKG, "BeamWindowDialog.DialogTitle" ) );

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

    Label wlWindowType = new Label( shell, SWT.RIGHT );
    wlWindowType.setText( BaseMessages.getString( PKG, "BeamWindowDialog.WindowType" ) );
    props.setLook( wlWindowType );
    FormData fdlWindowType = new FormData();
    fdlWindowType.left = new FormAttachment( 0, 0 );
    fdlWindowType.top = new FormAttachment( lastControl, margin );
    fdlWindowType.right = new FormAttachment( middle, -margin );
    wlWindowType.setLayoutData( fdlWindowType );
    wWindowType = new Combo( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wWindowType );
    wWindowType.setItems( BeamDefaults.WINDOW_TYPES );
    FormData fdWindowType = new FormData();
    fdWindowType.left = new FormAttachment( middle, 0 );
    fdWindowType.top = new FormAttachment( wlWindowType, 0, SWT.CENTER );
    fdWindowType.right = new FormAttachment( 100, 0 );
    wWindowType.setLayoutData( fdWindowType );
    lastControl = wWindowType;

    Label wlDuration = new Label( shell, SWT.RIGHT );
    wlDuration.setText( BaseMessages.getString( PKG, "BeamWindowDialog.Duration" ) );
    props.setLook( wlDuration );
    FormData fdlDuration = new FormData();
    fdlDuration.left = new FormAttachment( 0, 0 );
    fdlDuration.top = new FormAttachment( lastControl, margin );
    fdlDuration.right = new FormAttachment( middle, -margin );
    wlDuration.setLayoutData( fdlDuration );
    wDuration = new TextVar( variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wDuration );
    FormData fdDuration = new FormData();
    fdDuration.left = new FormAttachment( middle, 0 );
    fdDuration.top = new FormAttachment( wlDuration, 0, SWT.CENTER );
    fdDuration.right = new FormAttachment( 100, 0 );
    wDuration.setLayoutData( fdDuration );
    lastControl = wDuration;

    Label wlEvery = new Label( shell, SWT.RIGHT );
    wlEvery.setText( BaseMessages.getString( PKG, "BeamWindowDialog.Every" ) );
    props.setLook( wlEvery );
    FormData fdlEvery = new FormData();
    fdlEvery.left = new FormAttachment( 0, 0 );
    fdlEvery.top = new FormAttachment( lastControl, margin );
    fdlEvery.right = new FormAttachment( middle, -margin );
    wlEvery.setLayoutData( fdlEvery );
    wEvery = new TextVar( variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wEvery );
    FormData fdEvery = new FormData();
    fdEvery.left = new FormAttachment( middle, 0 );
    fdEvery.top = new FormAttachment( wlEvery, 0, SWT.CENTER );
    fdEvery.right = new FormAttachment( 100, 0 );
    wEvery.setLayoutData( fdEvery );
    lastControl = wEvery;


    Label wlStartTimeField = new Label( shell, SWT.RIGHT );
    wlStartTimeField.setText( BaseMessages.getString( PKG, "BeamWindowDialog.StartTimeField" ) );
    props.setLook( wlStartTimeField );
    FormData fdlStartTimeField = new FormData();
    fdlStartTimeField.left = new FormAttachment( 0, 0 );
    fdlStartTimeField.top = new FormAttachment( lastControl, margin );
    fdlStartTimeField.right = new FormAttachment( middle, -margin );
    wlStartTimeField.setLayoutData( fdlStartTimeField );
    wStartTimeField = new TextVar( variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wStartTimeField );
    FormData fdStartTimeField = new FormData();
    fdStartTimeField.left = new FormAttachment( middle, 0 );
    fdStartTimeField.top = new FormAttachment( wlStartTimeField, 0, SWT.CENTER );
    fdStartTimeField.right = new FormAttachment( 100, 0 );
    wStartTimeField.setLayoutData( fdStartTimeField );
    lastControl = wStartTimeField;

    Label wlEndTimeField = new Label( shell, SWT.RIGHT );
    wlEndTimeField.setText( BaseMessages.getString( PKG, "BeamWindowDialog.EndTimeField" ) );
    props.setLook( wlEndTimeField );
    FormData fdlEndTimeField = new FormData();
    fdlEndTimeField.left = new FormAttachment( 0, 0 );
    fdlEndTimeField.top = new FormAttachment( lastControl, margin );
    fdlEndTimeField.right = new FormAttachment( middle, -margin );
    wlEndTimeField.setLayoutData( fdlEndTimeField );
    wEndTimeField = new TextVar( variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wEndTimeField );
    FormData fdEndTimeField = new FormData();
    fdEndTimeField.left = new FormAttachment( middle, 0 );
    fdEndTimeField.top = new FormAttachment( wlEndTimeField, 0, SWT.CENTER );
    fdEndTimeField.right = new FormAttachment( 100, 0 );
    wEndTimeField.setLayoutData( fdEndTimeField );
    lastControl = wEndTimeField;

    Label wlMaxTimeField = new Label( shell, SWT.RIGHT );
    wlMaxTimeField.setText( BaseMessages.getString( PKG, "BeamWindowDialog.MaxTimeField" ) );
    props.setLook( wlMaxTimeField );
    FormData fdlMaxTimeField = new FormData();
    fdlMaxTimeField.left = new FormAttachment( 0, 0 );
    fdlMaxTimeField.top = new FormAttachment( lastControl, margin );
    fdlMaxTimeField.right = new FormAttachment( middle, -margin );
    wlMaxTimeField.setLayoutData( fdlMaxTimeField );
    wMaxTimeField = new TextVar( variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wMaxTimeField );
    FormData fdMaxTimeField = new FormData();
    fdMaxTimeField.left = new FormAttachment( middle, 0 );
    fdMaxTimeField.top = new FormAttachment( wlMaxTimeField, 0, SWT.CENTER );
    fdMaxTimeField.right = new FormAttachment( 100, 0 );
    wMaxTimeField.setLayoutData( fdMaxTimeField );
    lastControl = wMaxTimeField;
    
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
    wWindowType.addSelectionListener( lsDef );
    wDuration.addSelectionListener( lsDef );
    wEvery.addSelectionListener( lsDef );
    wStartTimeField.addSelectionListener( lsDef );
    wEndTimeField.addSelectionListener( lsDef );
    wMaxTimeField.addSelectionListener( lsDef );

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
    wWindowType.setText( Const.NVL( input.getWindowType(), "" ) );
    wDuration.setText( Const.NVL( input.getDuration(), "" ) );
    wEvery.setText( Const.NVL( input.getEvery(), "" ) );
    wStartTimeField.setText( Const.NVL( input.getStartWindowField(), "" ) );
    wEndTimeField.setText( Const.NVL( input.getEndWindowField(), "" ) );
    wMaxTimeField.setText( Const.NVL( input.getMaxWindowField(), "" ) );

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

  private void getInfo( BeamWindowMeta in ) {
    transformName = wTransformName.getText(); // return value

    in.setWindowType( wWindowType.getText() );
    in.setDuration( wDuration.getText() );
    in.setEvery( wEvery.getText() );
    in.setStartWindowField( wStartTimeField.getText() );
    in.setEndWindowField( wEndTimeField.getText() );
    in.setMaxWindowField( wMaxTimeField.getText() );

    input.setChanged();
  }
}
