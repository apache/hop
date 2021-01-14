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

package org.apache.hop.pipeline.transforms.samplerows;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.widget.LabelTextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
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
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.apache.hop.core.variables.IVariables;

public class SampleRowsDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = SampleRowsMeta.class; // For Translator

  private LabelTextVar wLinesRange;
  private FormData fdLinesRange, fdLineNumberField;
  private LabelTextVar wLineNumberField;
  private SampleRowsMeta input;

  public SampleRowsDialog( Shell parent, IVariables variables, Object in, PipelineMeta tr, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, tr, sname );
    input = (SampleRowsMeta) in;
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX );
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
    shell.setText( BaseMessages.getString( PKG, "SampleRowsDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // Some buttons at the bottom
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOk.addListener( SWT.Selection, e -> ok() );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, e -> cancel() );
    setButtonPositions( new Button[] { wOk, wCancel }, margin, null );

    // TransformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "SampleRowsDialog.TransformName.Label" ) );
    props.setLook( wlTransformName );
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment( 0, 0 );
    fdlTransformName.right = new FormAttachment( middle, -margin );
    fdlTransformName.top = new FormAttachment( 0, margin );
    wlTransformName.setLayoutData( fdlTransformName );
    wTransformName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wTransformName.setText( transformName );
    props.setLook( wTransformName );
    wTransformName.addModifyListener( lsMod );
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment( middle, 0 );
    fdTransformName.top = new FormAttachment( 0, margin );
    fdTransformName.right = new FormAttachment( 100, 0 );
    wTransformName.setLayoutData( fdTransformName );

    // Lines ragne
    wLinesRange =
      new LabelTextVar(
        variables, shell, BaseMessages.getString( PKG, "SampleRowsDialog.LinesRange.Label" ), BaseMessages
        .getString( PKG, "SampleRowsDialog.LinesRange.Tooltip" ) );
    props.setLook( wLinesRange );
    wLinesRange.addModifyListener( lsMod );
    fdLinesRange = new FormData();
    fdLinesRange.left = new FormAttachment( 0, -margin );
    fdLinesRange.top = new FormAttachment( wTransformName, 2 * margin );
    fdLinesRange.right = new FormAttachment( 100, -margin );
    wLinesRange.setLayoutData( fdLinesRange );

    // Add line number to output?
    wLineNumberField =
      new LabelTextVar(
        variables, shell, BaseMessages.getString( PKG, "SampleRowsDialog.LineNumberField.Label" ),
        BaseMessages.getString( PKG, "SampleRowsDialog.LineNumberField.Tooltip" ) );
    props.setLook( wLinesRange );
    wLineNumberField.addModifyListener( lsMod );
    fdLineNumberField = new FormData();
    fdLineNumberField.left = new FormAttachment( 0, -margin );
    fdLineNumberField.top = new FormAttachment( wLinesRange, 2 * margin );
    fdLineNumberField.right = new FormAttachment( 100, -margin );
    wLineNumberField.setLayoutData( fdLineNumberField );


    // Add listeners
    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wTransformName.addSelectionListener( lsDef );

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
    return transformName;
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    if ( input.getLinesRange() != null ) {
      wLinesRange.setText( Const.NVL( input.getLinesRange(), SampleRowsMeta.DEFAULT_RANGE ) );
    }
    if ( input.getLineNumberField() != null ) {
      wLineNumberField.setText( input.getLineNumberField() );
    }

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

    // Get the information for the dialog into the input structure.
    getInfo( input );

    dispose();
  }

  private void getInfo( SampleRowsMeta inf ) {
    inf.setLinesRange( Const.NVL( wLinesRange.getText(), SampleRowsMeta.DEFAULT_RANGE ) );
    inf.setLineNumberField( wLineNumberField.getText() );
    transformName = wTransformName.getText(); // return value
  }
}
