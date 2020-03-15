/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.job.entries.checkdbconnection;

import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.PluginDialog;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.job.JobMeta;
import org.apache.hop.job.entries.checkdbconnection.JobEntryCheckDbConnections;
import org.apache.hop.job.entry.JobEntryDialogInterface;
import org.apache.hop.job.entry.JobEntryInterface;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.job.dialog.JobDialog;
import org.apache.hop.ui.job.entry.JobEntryDialog;
import org.apache.hop.ui.trans.step.BaseStepDialog;
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
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import java.util.List;

/**
 * This dialog allows you to edit the check database connection job entry settings.
 *
 * @author Samatar
 * @since 12-10-2007
 */
@PluginDialog(
  id = "CHECK_DB_CONNECTIONS", 
  image = "CheckDbConnection.svg", 
  pluginType = PluginDialog.PluginType.JOBENTRY,
  documentationUrl = "https://www.project-hop.org/manual/latest/plugins/actions/" 
)
public class JobEntryCheckDbConnectionsDialog extends JobEntryDialog implements JobEntryDialogInterface {
  private static Class<?> PKG = JobEntryCheckDbConnectionsDialog.class; // for i18n purposes, needed by Translator2!!

  private Label wlName;

  private Text wName;

  private FormData fdlName, fdName;

  private Button wOK, wCancel;

  private Listener lsOK, lsCancel;

  private JobEntryCheckDbConnections jobEntry;

  private Shell shell;

  private SelectionAdapter lsDef;

  private boolean changed;
  private Label wlFields;
  private TableView wFields;
  private FormData fdlFields, fdFields;

  private Button wbdSourceFileFolder; // Delete
  private FormData fdbdSourceFileFolder;

  private Button wbgetConnections; // Get connections
  private FormData fdbgetConnections;

  private String[] connections;

  public JobEntryCheckDbConnectionsDialog( Shell parent, JobEntryInterface jobEntryInt, JobMeta jobMeta ) {
    super( parent, jobEntryInt, jobMeta );
    jobEntry = (JobEntryCheckDbConnections) jobEntryInt;
    if ( this.jobEntry.getName() == null ) {
      this.jobEntry.setName( BaseMessages.getString( PKG, "JobCheckDbConnections.Name.Default" ) );
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
    shell.setText( BaseMessages.getString( PKG, "JobCheckDbConnections.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Filename line
    wlName = new Label( shell, SWT.RIGHT );
    wlName.setText( BaseMessages.getString( PKG, "JobCheckDbConnections.Name.Label" ) );
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

    wlFields = new Label( shell, SWT.NONE );
    wlFields.setText( BaseMessages.getString( PKG, "JobCheckDbConnections.Fields.Label" ) );
    props.setLook( wlFields );
    fdlFields = new FormData();
    fdlFields.left = new FormAttachment( 0, 0 );
    // fdlFields.right= new FormAttachment(middle, -margin);
    fdlFields.top = new FormAttachment( wName, 2 * margin );
    wlFields.setLayoutData( fdlFields );

    // Buttons to the right of the screen...
    wbdSourceFileFolder = new Button( shell, SWT.PUSH | SWT.CENTER );
    props.setLook( wbdSourceFileFolder );
    wbdSourceFileFolder.setText( BaseMessages.getString( PKG, "JobCheckDbConnections.DeleteEntry" ) );
    wbdSourceFileFolder.setToolTipText( BaseMessages.getString(
      PKG, "JobCheckDbConnections.DeleteSourceFileButton.Label" ) );
    fdbdSourceFileFolder = new FormData();
    fdbdSourceFileFolder.right = new FormAttachment( 100, -margin );
    fdbdSourceFileFolder.top = new FormAttachment( wlFields, 50 );
    wbdSourceFileFolder.setLayoutData( fdbdSourceFileFolder );

    // Buttons to the right of the screen...
    wbgetConnections = new Button( shell, SWT.PUSH | SWT.CENTER );
    props.setLook( wbgetConnections );
    wbgetConnections.setText( BaseMessages.getString( PKG, "JobCheckDbConnections.GetConnections" ) );
    wbgetConnections
      .setToolTipText( BaseMessages.getString( PKG, "JobCheckDbConnections.GetConnections.Tooltip" ) );
    fdbgetConnections = new FormData();
    fdbgetConnections.right = new FormAttachment( 100, -margin );
    fdbgetConnections.top = new FormAttachment( wlFields, 20 );
    wbgetConnections.setLayoutData( fdbgetConnections );

    addDatabases();

    int rows = jobEntry.getConnections() == null ? 1
      : ( jobEntry.getConnections().length == 0 ? 0 : jobEntry.getConnections().length );

    final int FieldsRows = rows;

    ColumnInfo[] colinf =
      new ColumnInfo[] {
        new ColumnInfo(
          BaseMessages.getString( PKG, "JobCheckDbConnections.Fields.Argument.Label" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, connections, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "JobCheckDbConnections.Fields.WaitFor.Label" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "JobCheckDbConnections.Fields.WaitForTime.Label" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, JobEntryCheckDbConnections.unitTimeDesc, false ), };

    colinf[ 0 ].setToolTip( BaseMessages.getString( PKG, "JobCheckDbConnections.Fields.Column" ) );
    colinf[ 1 ].setUsingVariables( true );
    colinf[ 1 ].setToolTip( BaseMessages.getString( PKG, "JobCheckDbConnections.WaitFor.ToolTip" ) );

    wFields =
      new TableView(
        jobMeta, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, FieldsRows, lsMod, props );

    fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment( wlFields, margin );
    fdFields.right = new FormAttachment( wbgetConnections, -margin );
    fdFields.bottom = new FormAttachment( 100, -50 );
    wFields.setLayoutData( fdFields );

    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    FormData fd = new FormData();
    fd.right = new FormAttachment( 50, -10 );
    fd.bottom = new FormAttachment( 100, 0 );
    fd.width = 100;
    wOK.setLayoutData( fd );

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    fd = new FormData();
    fd.left = new FormAttachment( 50, 10 );
    fd.bottom = new FormAttachment( 100, 0 );
    fd.width = 100;
    wCancel.setLayoutData( fd );

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
    // Delete files from the list of files...
    wbdSourceFileFolder.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        int[] idx = wFields.getSelectionIndices();
        wFields.remove( idx );
        wFields.removeEmptyRows();
        wFields.setRowNums();
      }
    } );

    // get connections...
    wbgetConnections.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        getDatabases();
      }
    } );

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
    props.setDialogSize( shell, "JobCheckDbConnectionsDialogSize" );
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return jobEntry;
  }

  public void addDatabases() {
    connections = jobMeta.getDatabaseNames();
  }

  public void getDatabases() {
    wFields.removeAll();
    List<DatabaseMeta> databases = jobMeta.getDatabases();
    for ( int i = 0; i < databases.size(); i++ ) {
      DatabaseMeta ci = databases.get( i );
      if ( ci != null ) {
        wFields.add( new String[] { ci.getName(), "0", JobEntryCheckDbConnections.unitTimeDesc[ 0 ] } );
      }
    }
    wFields.removeEmptyRows();
    wFields.setRowNums();
    wFields.optWidth( true );
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
    if ( jobEntry.getName() != null ) {
      wName.setText( jobEntry.getName() );
    }

    if ( jobEntry.getConnections() != null ) {
      for ( int i = 0; i < jobEntry.getConnections().length; i++ ) {
        TableItem ti = wFields.table.getItem( i );
        if ( jobEntry.getConnections()[ i ] != null ) {
          ti.setText( 1, jobEntry.getConnections()[ i ].getName() );
          ti.setText( 2, "" + Const.toInt( jobEntry.getWaitfors()[ i ], 0 ) );
          ti.setText( 3, JobEntryCheckDbConnections.getWaitTimeDesc( jobEntry.getWaittimes()[ i ] ) );
        }
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

    int nritems = wFields.nrNonEmpty();

    DatabaseMeta[] connections = new DatabaseMeta[ nritems ];
    String[] waitfors = new String[ nritems ];
    int[] waittimes = new int[ nritems ];

    for ( int i = 0; i < nritems; i++ ) {
      String arg = wFields.getNonEmpty( i ).getText( 1 );
      DatabaseMeta dbMeta = jobMeta.findDatabase( arg );
      if ( dbMeta != null ) {
        connections[ i ] = dbMeta;
        waitfors[ i ] = "" + Const.toInt( wFields.getNonEmpty( i ).getText( 2 ), 0 );
        waittimes[ i ] =
          JobEntryCheckDbConnections.getWaitTimeByDesc( wFields.getNonEmpty( i ).getText( 3 ) );
      }
    }
    jobEntry.setConnections( connections );
    jobEntry.setWaitfors( waitfors );
    jobEntry.setWaittimes( waittimes );

    dispose();
  }

  public boolean evaluates() {
    return true;
  }

  public boolean isUnconditional() {
    return false;
  }

}
