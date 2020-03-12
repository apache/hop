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

package org.apache.hop.ui.trans.steps.javafilter;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.BaseStepMeta;
import org.apache.hop.trans.step.StepDialogInterface;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.trans.step.errorhandling.StreamInterface;
import org.apache.hop.trans.steps.javafilter.JavaFilterMeta;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.StyledTextComp;
import org.apache.hop.ui.trans.step.BaseStepDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
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
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JavaFilterDialog extends BaseStepDialog implements StepDialogInterface {
  private static Class<?> PKG = JavaFilterMeta.class; // for i18n purposes, needed by Translator2!!

  private Text wStepname;
  private CCombo wTrueTo;
  private CCombo wFalseTo;
  private StyledTextComp wCondition;

  private JavaFilterMeta input;

  private Map<String, Integer> inputFields;
  private ColumnInfo[] colinf;

  private Group wSettingsGroup;
  private FormData fdSettingsGroup;

  public JavaFilterDialog( Shell parent, Object in, TransMeta tr, String sname ) {
    super( parent, (BaseStepMeta) in, tr, sname );

    // The order here is important... currentMeta is looked at for changes
    input = (JavaFilterMeta) in;
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
    shell.setText( BaseMessages.getString( PKG, "JavaFilterDialog.DialogTitle" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // Stepname line
    wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname.setText( BaseMessages.getString( PKG, "System.Label.StepName" ) );
    props.setLook( wlStepname );
    fdlStepname = new FormData();
    fdlStepname.left = new FormAttachment( 0, 0 );
    fdlStepname.right = new FormAttachment( middle, -margin );
    fdlStepname.top = new FormAttachment( 0, margin );
    wlStepname.setLayoutData( fdlStepname );
    wStepname = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wStepname.setText( stepname );
    props.setLook( wStepname );
    wStepname.addModifyListener( lsMod );
    fdStepname = new FormData();
    fdStepname.left = new FormAttachment( middle, 0 );
    fdStepname.top = new FormAttachment( 0, margin );
    fdStepname.right = new FormAttachment( 100, 0 );
    wStepname.setLayoutData( fdStepname );

    // Some buttons
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOK, wCancel }, margin, null );

    // /////////////////////////////////
    // START OF Settings GROUP
    // /////////////////////////////////

    wSettingsGroup = new Group( shell, SWT.SHADOW_NONE );
    props.setLook( wSettingsGroup );
    wSettingsGroup.setText( BaseMessages.getString( PKG, "JavaFIlterDialog.Settings.Label" ) );
    FormLayout settingsLayout = new FormLayout();
    settingsLayout.marginWidth = 10;
    settingsLayout.marginHeight = 10;
    wSettingsGroup.setLayout( settingsLayout );

    // Send 'True' data to...
    Label wlTrueTo = new Label( wSettingsGroup, SWT.RIGHT );
    wlTrueTo.setText( BaseMessages.getString( PKG, "JavaFilterDialog.SendTrueTo.Label" ) );
    props.setLook( wlTrueTo );
    FormData fdlTrueTo = new FormData();
    fdlTrueTo.left = new FormAttachment( 0, 0 );
    fdlTrueTo.right = new FormAttachment( middle, -margin );
    fdlTrueTo.top = new FormAttachment( wStepname, margin );
    wlTrueTo.setLayoutData( fdlTrueTo );
    wTrueTo = new CCombo( wSettingsGroup, SWT.BORDER );
    props.setLook( wTrueTo );

    StepMeta stepinfo = transMeta.findStep( stepname );
    if ( stepinfo != null ) {
      List<StepMeta> nextSteps = transMeta.findNextSteps( stepinfo );
      for ( int i = 0; i < nextSteps.size(); i++ ) {
        StepMeta stepMeta = nextSteps.get( i );
        wTrueTo.add( stepMeta.getName() );
      }
    }

    wTrueTo.addModifyListener( lsMod );
    FormData fdTrueTo = new FormData();
    fdTrueTo.left = new FormAttachment( middle, 0 );
    fdTrueTo.top = new FormAttachment( wStepname, margin );
    fdTrueTo.right = new FormAttachment( 100, 0 );
    wTrueTo.setLayoutData( fdTrueTo );

    // Send 'False' data to...
    Label wlFalseTo = new Label( wSettingsGroup, SWT.RIGHT );
    wlFalseTo.setText( BaseMessages.getString( PKG, "JavaFilterDialog.SendFalseTo.Label" ) );
    props.setLook( wlFalseTo );
    FormData fdlFalseTo = new FormData();
    fdlFalseTo.left = new FormAttachment( 0, 0 );
    fdlFalseTo.right = new FormAttachment( middle, -margin );
    fdlFalseTo.top = new FormAttachment( wTrueTo, margin );
    wlFalseTo.setLayoutData( fdlFalseTo );
    wFalseTo = new CCombo( wSettingsGroup, SWT.BORDER );
    props.setLook( wFalseTo );

    stepinfo = transMeta.findStep( stepname );
    if ( stepinfo != null ) {
      List<StepMeta> nextSteps = transMeta.findNextSteps( stepinfo );
      for ( int i = 0; i < nextSteps.size(); i++ ) {
        StepMeta stepMeta = nextSteps.get( i );
        wFalseTo.add( stepMeta.getName() );
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
    Label wlCondition = new Label( wSettingsGroup, SWT.RIGHT );
    wlCondition.setText( BaseMessages.getString( PKG, "JavaFIlterDialog.Condition.Label" ) );
    props.setLook( wlCondition );
    FormData fdlCondition = new FormData();
    fdlCondition.top = new FormAttachment( wFalseTo, margin );
    fdlCondition.left = new FormAttachment( 0, 0 );
    fdlCondition.right = new FormAttachment( middle, -margin );
    wlCondition.setLayoutData( fdlCondition );
    wCondition =
      new StyledTextComp( transMeta, wSettingsGroup, SWT.MULTI
        | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL, "" );
    props.setLook( wCondition );
    wCondition.addModifyListener( lsMod );
    FormData fdCondition = new FormData();
    fdCondition.top = new FormAttachment( wFalseTo, margin );
    fdCondition.left = new FormAttachment( middle, 0 );
    fdCondition.right = new FormAttachment( 100, 0 );
    fdCondition.bottom = new FormAttachment( 100, -margin );
    wCondition.setLayoutData( fdCondition );

    fdSettingsGroup = new FormData();
    fdSettingsGroup.left = new FormAttachment( 0, margin );
    fdSettingsGroup.top = new FormAttachment( wStepname, margin );
    fdSettingsGroup.right = new FormAttachment( 100, -margin );
    fdSettingsGroup.bottom = new FormAttachment( wOK, -margin );
    wSettingsGroup.setLayoutData( fdSettingsGroup );

    // ///////////////////////////////////////////////////////////
    // / END OF Settings GROUP
    // ///////////////////////////////////////////////////////////

    // Add listeners
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };
    lsOK = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };

    wCancel.addListener( SWT.Selection, lsCancel );
    wOK.addListener( SWT.Selection, lsOK );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wStepname.addSelectionListener( lsDef );

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
    return stepname;
  }

  protected void setComboBoxes() {
    // Something was changed in the row.
    //
    final Map<String, Integer> fields = new HashMap<String, Integer>();

    // Add the currentMeta fields...
    fields.putAll( inputFields );

    shell.getDisplay().syncExec( new Runnable() {
      public void run() {
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
      }
    } );

  }

  /**
   * Copy information from the meta-data currentMeta to the dialog fields.
   */
  public void getData() {
    List<StreamInterface> targetStreams = input.getStepIOMeta().getTargetStreams();

    wTrueTo.setText( Const.NVL( targetStreams.get( 0 ).getStepname(), "" ) );
    wFalseTo.setText( Const.NVL( targetStreams.get( 1 ).getStepname(), "" ) );
    wCondition.setText( Const.NVL( input.getCondition(), "" ) );

    wStepname.selectAll();
    wStepname.setFocus();
  }

  private void cancel() {
    stepname = null;
    input.setChanged( changed );
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( wStepname.getText() ) ) {
      return;
    }

    stepname = wStepname.getText(); // return value

    String trueStepname = Const.NVL( wTrueTo.getText(), null );
    String falseStepname = Const.NVL( wFalseTo.getText(), null );

    List<StreamInterface> targetStreams = input.getStepIOMeta().getTargetStreams();

    targetStreams.get( 0 ).setStepMeta( transMeta.findStep( trueStepname ) );
    targetStreams.get( 1 ).setStepMeta( transMeta.findStep( falseStepname ) );

    input.setCondition( wCondition.getText() );

    dispose();
  }
}
