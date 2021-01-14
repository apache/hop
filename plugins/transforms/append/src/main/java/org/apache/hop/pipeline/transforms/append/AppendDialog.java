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

package org.apache.hop.pipeline.transforms.append;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.errorhandling.IStream;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

import java.util.List;

/**
 * Dialog for the append transform.
 *
 * @author Sven Boden
 */

public class AppendDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = AppendDialog.class; // For Translator

  private CCombo wHeadHop;

  private CCombo wTailHop;

  private final AppendMeta input;

  public AppendDialog( Shell parent, IVariables variables, Object in, PipelineMeta tr, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, tr, sname );
    input = (AppendMeta) in;
  }

  @Override
  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX );
    props.setLook( shell );
    setShellImage( shell, input );

    ModifyListener lsMod = e -> input.setChanged();
    backupChanged = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "AppendDialog.Shell.Label" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // TransformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "AppendDialog.TransformName.Label" ) );
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

    // Get the previous transforms...
    String[] previousTransforms = pipelineMeta.getPrevTransformNames( transformName );

    Label wlHeadHop = new Label(shell, SWT.RIGHT);
    wlHeadHop.setText( BaseMessages.getString( PKG, "AppendDialog.HeadHop.Label" ) );
    props.setLook(wlHeadHop);
    FormData fdlHeadHop = new FormData();
    fdlHeadHop.left = new FormAttachment( 0, 0 );
    fdlHeadHop.right = new FormAttachment( middle, -margin );
    fdlHeadHop.top = new FormAttachment( wTransformName, margin );
    wlHeadHop.setLayoutData(fdlHeadHop);
    wHeadHop = new CCombo( shell, SWT.BORDER );
    props.setLook( wHeadHop );

    if ( previousTransforms != null ) {
      wHeadHop.setItems( previousTransforms );
    }

    wHeadHop.addModifyListener( lsMod );
    FormData fdHeadHop = new FormData();
    fdHeadHop.left = new FormAttachment( middle, 0 );
    fdHeadHop.top = new FormAttachment( wTransformName, margin );
    fdHeadHop.right = new FormAttachment( 100, 0 );
    wHeadHop.setLayoutData(fdHeadHop);

    Label wlTailHop = new Label(shell, SWT.RIGHT);
    wlTailHop.setText( BaseMessages.getString( PKG, "AppendDialog.TailHop.Label" ) );
    props.setLook(wlTailHop);
    FormData fdlTailHop = new FormData();
    fdlTailHop.left = new FormAttachment( 0, 0 );
    fdlTailHop.right = new FormAttachment( middle, -margin );
    fdlTailHop.top = new FormAttachment( wHeadHop, margin );
    wlTailHop.setLayoutData(fdlTailHop);
    wTailHop = new CCombo( shell, SWT.BORDER );
    props.setLook( wTailHop );

    if ( previousTransforms != null ) {
      wTailHop.setItems( previousTransforms );
    }

    wTailHop.addModifyListener( lsMod );
    FormData fdTailHop = new FormData();
    fdTailHop.top = new FormAttachment( wHeadHop, margin );
    fdTailHop.left = new FormAttachment( middle, 0 );
    fdTailHop.right = new FormAttachment( 100, 0 );
    wTailHop.setLayoutData(fdTailHop);

    // Some buttons
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOk, wCancel }, margin, wTailHop );

    // Add listeners
    lsCancel = e -> cancel();
    lsOk = e -> ok();

    wCancel.addListener( SWT.Selection, lsCancel );
    wOk.addListener( SWT.Selection, lsOk );

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
    input.setChanged( backupChanged );

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
    List<IStream> infoStreams = input.getTransformIOMeta().getInfoStreams();
    IStream headStream = infoStreams.get( 0 );
    IStream tailStream = infoStreams.get( 1 );

    wHeadHop.setText( Const.NVL( headStream.getTransformName(), "" ) );
    wTailHop.setText( Const.NVL( tailStream.getTransformName(), "" ) );

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    input.setChanged( backupChanged );
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( wTransformName.getText() ) ) {
      return;
    }

    List<IStream> infoStreams = input.getTransformIOMeta().getInfoStreams();
    IStream headStream = infoStreams.get( 0 );
    IStream tailStream = infoStreams.get( 1 );

    headStream.setTransformMeta( pipelineMeta.findTransform( wHeadHop.getText() ) );
    tailStream.setTransformMeta( pipelineMeta.findTransform( wTailHop.getText() ) );

    transformName = wTransformName.getText(); // return value

    dispose();
  }
}
