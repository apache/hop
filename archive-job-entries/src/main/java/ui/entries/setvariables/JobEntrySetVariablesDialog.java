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

package org.apache.hop.ui.job.entries.setvariables;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.job.JobMeta;
import org.apache.hop.job.entries.setvariables.JobEntrySetVariables;
import org.apache.hop.job.entry.JobEntryDialogInterface;
import org.apache.hop.job.entry.JobEntryInterface;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.job.dialog.JobDialog;
import org.apache.hop.ui.job.entry.JobEntryDialog;
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
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

/**
 * This dialog allows you to edit the Set variables job entry settings.
 *
 * @author Samatar Hassan
 * @since 06-05-2007
 */

public class JobEntrySetVariablesDialog extends JobEntryDialog implements JobEntryDialogInterface {
  private static Class<?> PKG = JobEntrySetVariables.class; // for i18n purposes, needed by Translator2!!

  private Label wlName;
  private Text wName;
  private FormData fdlName, fdName;

  private Label wlVarSubs;
  private Button wVarSubs;
  private FormData fdlVarSubs, fdVarSubs;

  private Button wOK, wCancel;
  private Listener lsOK, lsCancel;

  private JobEntrySetVariables jobEntry;
  private Shell shell;

  private Label wlFields;
  private TableView wFields;
  private FormData fdlFields, fdFields;

  private Label wlFilename;
  private TextVar wFilename;
  private FormData fdlFilename, fdFilename;

  private Label wlFileVariableType;
  private CCombo wFileVariableType;
  private FormData fdlFileVariableType, fdFileVariableType;

  private SelectionAdapter lsDef;

  private boolean changed;

  private Group gSettings, gFilename;
  private FormData fdgSettings, fdgFilename;

  public JobEntrySetVariablesDialog( Shell parent, JobEntryInterface jobEntryInt, JobMeta jobMeta ) {
    super( parent, jobEntryInt, jobMeta );
    jobEntry = (JobEntrySetVariables) jobEntryInt;

    if ( this.jobEntry.getName() == null ) {
      this.jobEntry.setName( BaseMessages.getString( PKG, "JobEntrySetVariables.Name.Default" ) );
    }
  }

  public JobEntryInterface open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, props.getJobsDialogStyle() );
    props.setLook( shell );
    JobDialog.setShellImage( shell, jobEntry );

    ModifyListener lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        jobEntry.setChanged();
      }
    };
    changed = jobEntry.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "JobEntrySetVariables.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // Name line
    wlName = new Label( shell, SWT.RIGHT );
    wlName.setText( BaseMessages.getString( PKG, "JobEntrySetVariables.Name.Label" ) );
    props.setLook( wlName );
    fdlName = new FormData();
    fdlName.left = new FormAttachment( 0, 0 );
    fdlName.right = new FormAttachment( middle, -margin );
    fdlName.top = new FormAttachment( 0, margin );
    wlName.setLayoutData( fdlName );
    wName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wName );
    wName.addModifyListener( lsMod );
    fdName = new FormData();
    fdName.left = new FormAttachment( middle, 0 );
    fdName.top = new FormAttachment( 0, margin );
    fdName.right = new FormAttachment( 100, 0 );
    wName.setLayoutData( fdName );

    gFilename = new Group( shell, SWT.SHADOW_NONE );
    props.setLook( gFilename );
    gFilename.setText( BaseMessages.getString( PKG, "JobEntrySetVariables.FilenameGroup.Label" ) );

    FormLayout groupFilenameLayout = new FormLayout();
    groupFilenameLayout.marginWidth = 10;
    groupFilenameLayout.marginHeight = 10;
    gFilename.setLayout( groupFilenameLayout );

    // Name line
    wlFilename = new Label( gFilename, SWT.RIGHT );
    wlFilename.setText( BaseMessages.getString( PKG, "JobEntrySetVariables.Filename.Label" ) );
    props.setLook( wlFilename );
    fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment( 0, 0 );
    fdlFilename.right = new FormAttachment( middle, -margin );
    fdlFilename.top = new FormAttachment( 0, margin );
    wlFilename.setLayoutData( fdlFilename );
    wFilename = new TextVar( jobMeta, gFilename, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFilename );
    wFilename.addModifyListener( lsMod );
    fdFilename = new FormData();
    fdFilename.left = new FormAttachment( middle, 0 );
    fdFilename.top = new FormAttachment( 0, margin );
    fdFilename.right = new FormAttachment( 100, 0 );
    wFilename.setLayoutData( fdFilename );

    // file variable type line
    wlFileVariableType = new Label( gFilename, SWT.RIGHT );
    wlFileVariableType.setText( BaseMessages.getString( PKG, "JobEntrySetVariables.FileVariableType.Label" ) );
    props.setLook( wlFileVariableType );
    fdlFileVariableType = new FormData();
    fdlFileVariableType.left = new FormAttachment( 0, 0 );
    fdlFileVariableType.right = new FormAttachment( middle, -margin );
    fdlFileVariableType.top = new FormAttachment( wFilename, margin );
    wlFileVariableType.setLayoutData( fdlFileVariableType );
    wFileVariableType = new CCombo( gFilename, SWT.SINGLE | SWT.LEFT | SWT.BORDER | SWT.READ_ONLY );
    props.setLook( wFileVariableType );
    wFileVariableType.addModifyListener( lsMod );
    fdFileVariableType = new FormData();
    fdFileVariableType.left = new FormAttachment( middle, 0 );
    fdFileVariableType.top = new FormAttachment( wFilename, margin );
    fdFileVariableType.right = new FormAttachment( 100, 0 );
    wFileVariableType.setLayoutData( fdFileVariableType );
    wFileVariableType.setItems( JobEntrySetVariables.getVariableTypeDescriptions() );

    fdgFilename = new FormData();
    fdgFilename.left = new FormAttachment( 0, margin );
    fdgFilename.top = new FormAttachment( wName, margin );
    fdgFilename.right = new FormAttachment( 100, -margin );
    gFilename.setLayoutData( fdgFilename );

    //
    // START OF SETTINGS GROUP
    //
    gSettings = new Group( shell, SWT.SHADOW_NONE );
    props.setLook( gSettings );
    gSettings.setText( BaseMessages.getString( PKG, "JobEntrySetVariables.Settings.Label" ) );

    FormLayout groupLayout = new FormLayout();
    groupLayout.marginWidth = 10;
    groupLayout.marginHeight = 10;
    gSettings.setLayout( groupLayout );

    wlVarSubs = new Label( gSettings, SWT.RIGHT );
    wlVarSubs.setText( BaseMessages.getString( PKG, "JobEntrySetVariables.VarsReplace.Label" ) );
    props.setLook( wlVarSubs );
    fdlVarSubs = new FormData();
    fdlVarSubs.left = new FormAttachment( 0, 0 );
    fdlVarSubs.top = new FormAttachment( wName, margin );
    fdlVarSubs.right = new FormAttachment( middle, -margin );
    wlVarSubs.setLayoutData( fdlVarSubs );
    wVarSubs = new Button( gSettings, SWT.CHECK );
    props.setLook( wVarSubs );
    wVarSubs.setToolTipText( BaseMessages.getString( PKG, "JobEntrySetVariables.VarsReplace.Tooltip" ) );
    fdVarSubs = new FormData();
    fdVarSubs.left = new FormAttachment( middle, 0 );
    fdVarSubs.top = new FormAttachment( wName, margin );
    fdVarSubs.right = new FormAttachment( 100, 0 );
    wVarSubs.setLayoutData( fdVarSubs );
    wVarSubs.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        jobEntry.setChanged();
      }
    } );

    fdgSettings = new FormData();
    fdgSettings.left = new FormAttachment( 0, margin );
    fdgSettings.top = new FormAttachment( gFilename, margin );
    fdgSettings.right = new FormAttachment( 100, -margin );
    gSettings.setLayoutData( fdgSettings );

    // ///////////////////////////////////////////////////////////
    // / END OF SETTINGS GROUP
    // ///////////////////////////////////////////////////////////

    wlFields = new Label( shell, SWT.NONE );
    wlFields.setText( BaseMessages.getString( PKG, "SetVariableDialog.Variables.Label" ) );
    props.setLook( wlFields );
    fdlFields = new FormData();
    fdlFields.left = new FormAttachment( 0, 0 );
    fdlFields.top = new FormAttachment( gSettings, margin );
    wlFields.setLayoutData( fdlFields );

    int rows =
      jobEntry.variableName == null
        ? 1 : ( jobEntry.variableName.length == 0 ? 0 : jobEntry.variableName.length );
    final int FieldsRows = rows;

    ColumnInfo[] colinf =
      {
        new ColumnInfo(
          BaseMessages.getString( PKG, "SetVariableDialog.Fields.Column.VariableName" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "SetVariableDialog.Fields.Column.Value" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "SetVariableDialog.Fields.Column.VariableType" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, JobEntrySetVariables.getVariableTypeDescriptions(), false ), };
    colinf[ 0 ].setUsingVariables( true );
    colinf[ 1 ].setUsingVariables( true );

    wFields =
      new TableView(
        jobMeta, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, FieldsRows, lsMod, props );

    fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment( wlFields, margin );
    fdFields.right = new FormAttachment( 100, 0 );
    fdFields.bottom = new FormAttachment( 100, -50 );
    wFields.setLayoutData( fdFields );

    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    BaseStepDialog.positionBottomButtons( shell, new Button[] { wOK, wCancel }, margin, wFields );

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

    wName.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();

    BaseStepDialog.setSize( shell );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return jobEntry;
  }

  public void dispose() {
    WindowProperty winprop = new WindowProperty( shell );
    props.setScreen( winprop );
    shell.dispose();
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    wName.setText( Const.nullToEmpty( jobEntry.getName() ) );

    wFilename.setText( Const.NVL( jobEntry.getFilename(), "" ) );
    wFileVariableType.setText( JobEntrySetVariables.getVariableTypeDescription( jobEntry.getFileVariableType() ) );

    wVarSubs.setSelection( jobEntry.isReplaceVars() );

    if ( jobEntry.variableName != null ) {
      for ( int i = 0; i < jobEntry.variableName.length; i++ ) {
        TableItem ti = wFields.table.getItem( i );
        if ( jobEntry.variableName[ i ] != null ) {
          ti.setText( 1, jobEntry.variableName[ i ] );
        }
        if ( jobEntry.getVariableValue()[ i ] != null ) {
          ti.setText( 2, jobEntry.getVariableValue()[ i ] );
        }

        ti.setText( 3, JobEntrySetVariables.getVariableTypeDescription( jobEntry.getVariableType()[ i ] ) );

      }
      wFields.setRowNums();
      wFields.optWidth( true );
    }

    wName.selectAll();
    wName.setFocus();
  }

  private void cancel() {
    jobEntry.setChanged( changed );
    jobEntry = null;
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( wName.getText() ) ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setText( BaseMessages.getString( PKG, "System.StepJobEntryNameMissing.Title" ) );
      mb.setMessage( BaseMessages.getString( PKG, "System.JobEntryNameMissing.Msg" ) );
      mb.open();
      return;
    }
    jobEntry.setName( wName.getText() );

    jobEntry.setFilename( wFilename.getText() );
    jobEntry.setFileVariableType( JobEntrySetVariables.getVariableType( wFileVariableType.getText() ) );
    jobEntry.setReplaceVars( wVarSubs.getSelection() );

    int nritems = wFields.nrNonEmpty();
    int nr = 0;
    for ( int i = 0; i < nritems; i++ ) {
      String arg = wFields.getNonEmpty( i ).getText( 1 );
      if ( arg != null && arg.length() != 0 ) {
        nr++;
      }
    }
    jobEntry.variableName = new String[ nr ];
    jobEntry.variableValue = new String[ nr ];
    jobEntry.variableType = new int[ nr ];

    nr = 0;
    for ( int i = 0; i < nritems; i++ ) {
      String varname = wFields.getNonEmpty( i ).getText( 1 );
      String varvalue = wFields.getNonEmpty( i ).getText( 2 );
      String vartype = wFields.getNonEmpty( i ).getText( 3 );

      if ( varname != null && varname.length() != 0 ) {
        jobEntry.variableName[ nr ] = varname;
        jobEntry.variableValue[ nr ] = varvalue;
        jobEntry.variableType[ nr ] = JobEntrySetVariables.getVariableType( vartype );
        nr++;
      }
    }

    dispose();
  }

  public boolean evaluates() {
    return true;
  }

  public boolean isUnconditional() {
    return false;
  }
}
