/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
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

package org.apache.hop.workflow.actions.checkdbconnection;

import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.PluginDialog;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.IActionDialog;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
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
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import java.util.List;

/**
 * This dialog allows you to edit the check database connection action settings.
 *
 * @author Samatar
 * @since 12-10-2007
 */
@PluginDialog(
  id = "CHECK_DB_CONNECTIONS", 
  image = "CheckDbConnection.svg", 
  pluginType = PluginDialog.PluginType.ACTION,
  documentationUrl = "https://www.project-hop.org/manual/latest/plugins/actions/" 
)
public class ActionCheckDbConnectionsDialog extends ActionDialog implements IActionDialog {
  private static final Class<?> PKG = ActionCheckDbConnectionsDialog.class; // for i18n purposes, needed by Translator!!


  private Text wName;

  private ActionICheckDbConnections jobEntry;

  private SelectionAdapter lsDef;

  private boolean changed;
  
  private TableView wFields;

  private FormData fdbdSourceFileFolder;

  private FormData fdbgetConnections;

  public ActionCheckDbConnectionsDialog( Shell parent, IAction jobEntryInt, WorkflowMeta workflowMeta ) {
    super( parent, jobEntryInt, workflowMeta );
    jobEntry = (ActionICheckDbConnections) jobEntryInt;
    if ( this.jobEntry.getName() == null ) {
      this.jobEntry.setName( BaseMessages.getString( PKG, "JobCheckDbConnections.Name.Default" ) );
    }
  }

  @Override
  public IAction open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    Shell shell = new Shell( parent, props.getWorkflowsDialogStyle() );
    props.setLook( shell );
    WorkflowDialog.setShellImage( shell, jobEntry );

    ModifyListener lsMod = ( ModifyEvent e ) -> jobEntry.setChanged();    
	changed = jobEntry.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "JobCheckDbConnections.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Filename line
    Label wlName = new Label( shell, SWT.RIGHT );
    wlName.setText( BaseMessages.getString( PKG, "JobCheckDbConnections.Name.Label" ) );
    props.setLook( wlName );
    FormData fdlName = new FormData();
    fdlName.left = new FormAttachment( 0, 0 );
    fdlName.right = new FormAttachment( middle, -margin );
    fdlName.top = new FormAttachment( 0, margin );
    wlName.setLayoutData( fdlName );
    wName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wName );
    wName.addModifyListener( lsMod );
    FormData fdName = new FormData();
    fdName.left = new FormAttachment( middle, 0 );
    fdName.top = new FormAttachment( 0, margin );
    fdName.right = new FormAttachment( 100, 0 );
    wName.setLayoutData( fdName );

    Label wlFields = new Label( shell, SWT.NONE );
    wlFields.setText( BaseMessages.getString( PKG, "JobCheckDbConnections.Fields.Label" ) );
    props.setLook( wlFields );
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment( 0, 0 );
    // fdlFields.right= new FormAttachment(middle, -margin);
    fdlFields.top = new FormAttachment( wName, 2 * margin );
    wlFields.setLayoutData( fdlFields );

    // Buttons to the right of the screen...
    Button wbdSourceFileFolder = new Button( shell, SWT.PUSH | SWT.CENTER );
    props.setLook( wbdSourceFileFolder );
    wbdSourceFileFolder.setText( BaseMessages.getString( PKG, "JobCheckDbConnections.DeleteEntry" ) );
    wbdSourceFileFolder.setToolTipText( BaseMessages.getString(
      PKG, "JobCheckDbConnections.DeleteSourceFileButton.Label" ) );
    fdbdSourceFileFolder = new FormData();
    fdbdSourceFileFolder.right = new FormAttachment( 100, -margin );
    fdbdSourceFileFolder.top = new FormAttachment( wlFields, 50 );
    wbdSourceFileFolder.setLayoutData( fdbdSourceFileFolder );

    // Buttons to the right of the screen...
    Button wbgetConnections = new Button( shell, SWT.PUSH | SWT.CENTER );
    props.setLook( wbgetConnections );
    wbgetConnections.setText( BaseMessages.getString( PKG, "JobCheckDbConnections.GetConnections" ) );
    wbgetConnections
      .setToolTipText( BaseMessages.getString( PKG, "JobCheckDbConnections.GetConnections.Tooltip" ) );
    fdbgetConnections = new FormData();
    fdbgetConnections.right = new FormAttachment( 100, -margin );
    fdbgetConnections.top = new FormAttachment( wlFields, 20 );
    wbgetConnections.setLayoutData( fdbgetConnections );

    int rows = jobEntry.getConnections() == null ? 1
      : ( jobEntry.getConnections().length == 0 ? 0 : jobEntry.getConnections().length );

    final int FieldsRows = rows;

    ColumnInfo[] colinf =
      new ColumnInfo[] {
        new ColumnInfo(
          BaseMessages.getString( PKG, "JobCheckDbConnections.Fields.Argument.Label" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, workflowMeta.getDatabaseNames(), false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "JobCheckDbConnections.Fields.WaitFor.Label" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "JobCheckDbConnections.Fields.WaitForTime.Label" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, ActionICheckDbConnections.unitTimeDesc, false ), };

    colinf[ 0 ].setToolTip( BaseMessages.getString( PKG, "JobCheckDbConnections.Fields.Column" ) );
    colinf[ 1 ].setUsingVariables( true );
    colinf[ 1 ].setToolTip( BaseMessages.getString( PKG, "JobCheckDbConnections.WaitFor.ToolTip" ) );

    wFields =
      new TableView(
        workflowMeta, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, FieldsRows, lsMod, props );

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment( wlFields, margin );
    fdFields.right = new FormAttachment( wbgetConnections, -margin );
    fdFields.bottom = new FormAttachment( 100, -50 );
    wFields.setLayoutData( fdFields );

    Button wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    FormData fd = new FormData();
    fd.right = new FormAttachment( 50, -10 );
    fd.bottom = new FormAttachment( 100, 0 );
    fd.width = 100;
    wOk.setLayoutData( fd );
    wOk.addListener( SWT.Selection, (Event e) -> { ok();  } );

    Button wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    fd = new FormData();
    fd.left = new FormAttachment( 50, 10 );
    fd.bottom = new FormAttachment( 100, 0 );
    fd.width = 100;
    wCancel.setLayoutData( fd );
    wCancel.addListener( SWT.Selection, (Event e) -> { cancel(); } );

    BaseTransformDialog.positionBottomButtons( shell, new Button[] { wOk, wCancel }, margin, wFields );

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

    BaseTransformDialog.setSize( shell );

    shell.open();
    props.setDialogSize( shell, "JobCheckDbConnectionsDialogSize" );
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return jobEntry;
  }

//  public void addDatabases() {
//    connections = workflowMeta.getDatabaseNames();
//  }

  public void getDatabases() {
    wFields.removeAll();
    List<DatabaseMeta> databases = workflowMeta.getDatabases();
    for ( int i = 0; i < databases.size(); i++ ) {
      DatabaseMeta ci = databases.get( i );
      if ( ci != null ) {
        wFields.add( new String[] { ci.getName(), "0", ActionICheckDbConnections.unitTimeDesc[ 0 ] } );
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
          ti.setText( 3, ActionICheckDbConnections.getWaitTimeDesc( jobEntry.getWaittimes()[ i ] ) );
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
      mb.setText( BaseMessages.getString( PKG, "System.TransformActionNameMissing.Title" ) );
      mb.setMessage( BaseMessages.getString( PKG, "System.ActionNameMissing.Msg" ) );
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
      DatabaseMeta dbMeta = workflowMeta.findDatabase( arg );
      if ( dbMeta != null ) {
        connections[ i ] = dbMeta;
        waitfors[ i ] = "" + Const.toInt( wFields.getNonEmpty( i ).getText( 2 ), 0 );
        waittimes[ i ] =
          ActionICheckDbConnections.getWaitTimeByDesc( wFields.getNonEmpty( i ).getText( 3 ) );
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
