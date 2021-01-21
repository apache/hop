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

package org.apache.hop.pipeline.transforms.filterrows;

import org.apache.hop.core.Condition;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.errorhandling.IStream;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ConditionEditor;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

import java.util.List;

public class FilterRowsDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = FilterRowsMeta.class; // For Translator

  private CCombo wTrueTo;

  private CCombo wFalseTo;

  private ConditionEditor wCondition;

  private final FilterRowsMeta input;
  private final Condition condition;

  private Condition backupCondition;

  public FilterRowsDialog( Shell parent, IVariables variables, Object in, PipelineMeta tr, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, tr, sname );
    input = (FilterRowsMeta) in;

    condition = (Condition) input.getCondition().clone();

  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX );
    props.setLook( shell );
    setShellImage( shell, input );

    ModifyListener lsMod = e -> input.setChanged();
    backupChanged = input.hasChanged();
    backupCondition = (Condition) condition.clone();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "FilterRowsDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // TransformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "FilterRowsDialog.TransformName.Label" ) );
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

    // Send 'True' data to...
    Label wlTrueTo = new Label(shell, SWT.RIGHT);
    wlTrueTo.setText( BaseMessages.getString( PKG, "FilterRowsDialog.SendTrueTo.Label" ) );
    props.setLook(wlTrueTo);
    FormData fdlTrueTo = new FormData();
    fdlTrueTo.left = new FormAttachment( 0, 0 );
    fdlTrueTo.right = new FormAttachment( middle, -margin );
    fdlTrueTo.top = new FormAttachment( wTransformName, margin );
    wlTrueTo.setLayoutData(fdlTrueTo);
    wTrueTo = new CCombo( shell, SWT.BORDER );
    props.setLook( wTrueTo );

    TransformMeta transforminfo = pipelineMeta.findTransform( transformName );
    if ( transforminfo != null ) {
      List<TransformMeta> nextTransforms = pipelineMeta.findNextTransforms( transforminfo );
      for (TransformMeta transformMeta : nextTransforms) {
        wTrueTo.add(transformMeta.getName());
      }
    }

    wTrueTo.addModifyListener( lsMod );
    FormData fdTrueTo = new FormData();
    fdTrueTo.left = new FormAttachment( middle, 0 );
    fdTrueTo.top = new FormAttachment( wTransformName, margin );
    fdTrueTo.right = new FormAttachment( 100, 0 );
    wTrueTo.setLayoutData(fdTrueTo);

    // Send 'False' data to...
    Label wlFalseTo = new Label(shell, SWT.RIGHT);
    wlFalseTo.setText( BaseMessages.getString( PKG, "FilterRowsDialog.SendFalseTo.Label" ) );
    props.setLook(wlFalseTo);
    FormData fdlFalseTo = new FormData();
    fdlFalseTo.left = new FormAttachment( 0, 0 );
    fdlFalseTo.right = new FormAttachment( middle, -margin );
    fdlFalseTo.top = new FormAttachment( wTrueTo, margin );
    wlFalseTo.setLayoutData(fdlFalseTo);
    wFalseTo = new CCombo( shell, SWT.BORDER );
    props.setLook( wFalseTo );

    transforminfo = pipelineMeta.findTransform( transformName );
    if ( transforminfo != null ) {
      List<TransformMeta> nextTransforms = pipelineMeta.findNextTransforms( transforminfo );
      for (TransformMeta transformMeta : nextTransforms) {
        wFalseTo.add(transformMeta.getName());
      }
    }

    wFalseTo.addModifyListener( lsMod );
    FormData fdFalseFrom = new FormData();
    fdFalseFrom.left = new FormAttachment( middle, 0 );
    fdFalseFrom.top = new FormAttachment( wTrueTo, margin );
    fdFalseFrom.right = new FormAttachment( 100, 0 );
    wFalseTo.setLayoutData(fdFalseFrom);

    Label wlCondition = new Label(shell, SWT.NONE);
    wlCondition.setText( BaseMessages.getString( PKG, "FilterRowsDialog.Condition.Label" ) );
    props.setLook(wlCondition);
    FormData fdlCondition = new FormData();
    fdlCondition.left = new FormAttachment( 0, 0 );
    fdlCondition.top = new FormAttachment( wFalseTo, margin );
    wlCondition.setLayoutData(fdlCondition);

    IRowMeta inputfields = null;
    try {
      inputfields = pipelineMeta.getPrevTransformFields( variables, transformName );
    } catch ( HopException ke ) {
      inputfields = new RowMeta();
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "FilterRowsDialog.FailedToGetFields.DialogTitle" ), BaseMessages
        .getString( PKG, "FilterRowsDialog.FailedToGetFields.DialogMessage" ), ke );
    }

    // Some buttons
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOk, wCancel }, margin, null );

    wCondition = new ConditionEditor( shell, SWT.BORDER, condition, inputfields );

    FormData fdCondition = new FormData();
    fdCondition.left = new FormAttachment( 0, 0 );
    fdCondition.top = new FormAttachment(wlCondition, margin );
    fdCondition.right = new FormAttachment( 100, 0 );
    fdCondition.bottom = new FormAttachment( wOk, -2 * margin );
    wCondition.setLayoutData(fdCondition);
    wCondition.addModifyListener( lsMod );

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
    List<IStream> targetStreams = input.getTransformIOMeta().getTargetStreams();

    wTrueTo.setText( Const.NVL( targetStreams.get( 0 ).getTransformName(), "" ) );
    wFalseTo.setText( Const.NVL( targetStreams.get( 1 ).getTransformName(), "" ) );

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    input.setChanged( backupChanged );
    // Also change the condition back to what it was...
    input.setCondition( backupCondition );
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( wTransformName.getText() ) ) {
      return;
    }

    if ( wCondition.getLevel() > 0 ) {
      wCondition.goUp();
    } else {
      String trueTransformName = wTrueTo.getText();
      if ( trueTransformName.length() == 0 ) {
        trueTransformName = null;
      }
      String falseTransformName = wFalseTo.getText();
      if ( falseTransformName.length() == 0 ) {
        falseTransformName = null;
      }

      List<IStream> targetStreams = input.getTransformIOMeta().getTargetStreams();

      targetStreams.get( 0 ).setTransformMeta( pipelineMeta.findTransform( trueTransformName ) );
      targetStreams.get( 1 ).setTransformMeta( pipelineMeta.findTransform( falseTransformName ) );

      input.setTrueTransformName( trueTransformName );
      input.setFalseTransformName( falseTransformName );

      transformName = wTransformName.getText(); // return value
      input.setCondition( condition );

      dispose();
    }
  }
}
