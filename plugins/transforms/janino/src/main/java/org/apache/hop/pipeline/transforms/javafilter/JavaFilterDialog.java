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

package org.apache.hop.pipeline.transforms.javafilter;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.errorhandling.IStream;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.StyledTextComp;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

import java.util.List;
import java.util.*;

public class JavaFilterDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = JavaFilterMeta.class; // For Translator

  private Text wTransformName;
  private CCombo wTrueTo;
  private CCombo wFalseTo;
  private StyledTextComp wCondition;

  private final JavaFilterMeta input;

  private Map<String, Integer> inputFields;
  private ColumnInfo[] colinf;

  public JavaFilterDialog( Shell parent, IVariables variables, Object in, PipelineMeta tr, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, tr, sname );

    // The order here is important... currentMeta is looked at for changes
    input = (JavaFilterMeta) in;
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX );
    props.setLook( shell );
    setShellImage( shell, input );

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "JavaFilterDialog.DialogTitle" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // TransformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "System.Label.TransformName" ) );
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

    // Some buttons
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOk, wCancel }, margin, null );

    // /////////////////////////////////
    // START OF Settings GROUP
    // /////////////////////////////////

    Group wSettingsGroup = new Group(shell, SWT.SHADOW_NONE);
    props.setLook(wSettingsGroup);
    wSettingsGroup.setText( BaseMessages.getString( PKG, "JavaFIlterDialog.Settings.Label" ) );
    FormLayout settingsLayout = new FormLayout();
    settingsLayout.marginWidth = 10;
    settingsLayout.marginHeight = 10;
    wSettingsGroup.setLayout( settingsLayout );

    // Send 'True' data to...
    Label wlTrueTo = new Label(wSettingsGroup, SWT.RIGHT );
    wlTrueTo.setText( BaseMessages.getString( PKG, "JavaFilterDialog.SendTrueTo.Label" ) );
    props.setLook( wlTrueTo );
    FormData fdlTrueTo = new FormData();
    fdlTrueTo.left = new FormAttachment( 0, 0 );
    fdlTrueTo.right = new FormAttachment( middle, -margin );
    fdlTrueTo.top = new FormAttachment( wTransformName, margin );
    wlTrueTo.setLayoutData( fdlTrueTo );
    wTrueTo = new CCombo(wSettingsGroup, SWT.BORDER );
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
    wTrueTo.setLayoutData( fdTrueTo );

    // Send 'False' data to...
    Label wlFalseTo = new Label(wSettingsGroup, SWT.RIGHT );
    wlFalseTo.setText( BaseMessages.getString( PKG, "JavaFilterDialog.SendFalseTo.Label" ) );
    props.setLook( wlFalseTo );
    FormData fdlFalseTo = new FormData();
    fdlFalseTo.left = new FormAttachment( 0, 0 );
    fdlFalseTo.right = new FormAttachment( middle, -margin );
    fdlFalseTo.top = new FormAttachment( wTrueTo, margin );
    wlFalseTo.setLayoutData( fdlFalseTo );
    wFalseTo = new CCombo(wSettingsGroup, SWT.BORDER );
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
    wFalseTo.setLayoutData( fdFalseFrom );

    // bufferSize
    //
    Label wlCondition = new Label(wSettingsGroup, SWT.RIGHT );
    wlCondition.setText( BaseMessages.getString( PKG, "JavaFIlterDialog.Condition.Label" ) );
    props.setLook( wlCondition );
    FormData fdlCondition = new FormData();
    fdlCondition.top = new FormAttachment( wFalseTo, margin );
    fdlCondition.left = new FormAttachment( 0, 0 );
    fdlCondition.right = new FormAttachment( middle, -margin );
    wlCondition.setLayoutData( fdlCondition );
    wCondition =
      new StyledTextComp( variables, wSettingsGroup, SWT.MULTI
        | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL );
    props.setLook( wCondition );
    wCondition.addModifyListener( lsMod );
    FormData fdCondition = new FormData();
    fdCondition.top = new FormAttachment( wFalseTo, margin );
    fdCondition.left = new FormAttachment( middle, 0 );
    fdCondition.right = new FormAttachment( 100, 0 );
    fdCondition.bottom = new FormAttachment( 100, -margin );
    wCondition.setLayoutData( fdCondition );

    FormData fdSettingsGroup = new FormData();
    fdSettingsGroup.left = new FormAttachment( 0, margin );
    fdSettingsGroup.top = new FormAttachment( wTransformName, margin );
    fdSettingsGroup.right = new FormAttachment( 100, -margin );
    fdSettingsGroup.bottom = new FormAttachment( wOk, -margin );
    wSettingsGroup.setLayoutData(fdSettingsGroup);

    // ///////////////////////////////////////////////////////////
    // / END OF Settings GROUP
    // ///////////////////////////////////////////////////////////

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
    input.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformName;
  }

  protected void setComboBoxes() {
    // Something was changed in the row.
    //
    final Map<String, Integer> fields = new HashMap<>();

    // Add the currentMeta fields...
    fields.putAll( inputFields );

    shell.getDisplay().syncExec( () -> {
      // Add the newly create fields.
      //
      /*
       * int nrNonEmptyFields = wFields.nrNonEmpty(); for (int i=0;i<nrNonEmptyFields;i++) { TableItem item =
       * wFields.getNonEmpty(i); fields.put(item.getText(1), new Integer(1000000+i)); // The number is just to debug
       * the origin of the fieldname }
       */

      Set<String> keySet = fields.keySet();
      List<String> entries = new ArrayList<>( keySet );

      String[] fieldNames = entries.toArray( new String[ entries.size() ] );

      Const.sortStrings( fieldNames );

      colinf[ 5 ].setComboValues( fieldNames );
    } );

  }

  /**
   * Copy information from the meta-data currentMeta to the dialog fields.
   */
  public void getData() {
    List<IStream> targetStreams = input.getTransformIOMeta().getTargetStreams();

    wTrueTo.setText( Const.NVL( targetStreams.get( 0 ).getTransformName(), "" ) );
    wFalseTo.setText( Const.NVL( targetStreams.get( 1 ).getTransformName(), "" ) );
    wCondition.setText( Const.NVL( input.getCondition(), "" ) );

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

    transformName = wTransformName.getText(); // return value

    String trueTransformName = Const.NVL( wTrueTo.getText(), null );
    String falseTransformName = Const.NVL( wFalseTo.getText(), null );

    List<IStream> targetStreams = input.getTransformIOMeta().getTargetStreams();

    targetStreams.get( 0 ).setTransformMeta( pipelineMeta.findTransform( trueTransformName ) );
    targetStreams.get( 1 ).setTransformMeta( pipelineMeta.findTransform( falseTransformName ) );

    input.setCondition( wCondition.getText() );

    dispose();
  }
}
