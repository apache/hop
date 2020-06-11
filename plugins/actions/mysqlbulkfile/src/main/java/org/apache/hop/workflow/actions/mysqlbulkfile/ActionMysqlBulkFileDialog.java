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

package org.apache.hop.workflow.actions.mysqlbulkfile;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.PluginDialog;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.IActionDialog;
import org.apache.hop.ui.core.database.dialog.DatabaseExplorerDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
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
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

/**
 * This dialog allows you to edit the MYSQL Bulk Load To a file entry settings. (select the connection and the table to
 * be checked) This entry type evaluates!
 *
 * @author Samatar
 * @since 06-03-2006
 */
@PluginDialog( 
		  id = "MYSQL_BULK_FILE", 
		  image = "MysqlBulkFile.svg", 
		  pluginType = PluginDialog.PluginType.ACTION,
		  documentationUrl = "https://www.project-hop.org/manual/latest/plugins/actions/mysqlbulkfile.html"
)
public class ActionMysqlBulkFileDialog extends ActionDialog implements IActionDialog {
  private static Class<?> PKG = ActionMysqlBulkFile.class; // for i18n purposes, needed by Translator!!

  private static final String[] FILETYPES = new String[] {
    BaseMessages.getString( PKG, "JobMysqlBulkFile.Filetype.Text" ),
    BaseMessages.getString( PKG, "JobMysqlBulkFile.Filetype.All" ) };

  private Label wlName;

  private Text wName;

  private FormData fdlName, fdName;

  private MetaSelectionLine<DatabaseMeta> wConnection;

  private Label wlTablename;

  private TextVar wTablename;

  private FormData fdlTablename, fdTablename;

  // Schema name
  private Label wlSchemaname;
  private TextVar wSchemaname;
  private FormData fdlSchemaname, fdSchemaname;

  private Button wOk, wCancel;

  private Listener lsOk, lsCancel;

  private ActionMysqlBulkFile action;

  private Shell shell;

  private SelectionAdapter lsDef;

  private boolean changed;

  // Fichier
  private Label wlFilename;

  private Button wbFilename;

  private TextVar wFilename;

  private FormData fdlFilename, fdbFilename, fdFilename;

  // HighPriority
  private Label wlHighPriority;
  private Button wHighPriority;
  private FormData fdlHighPriority, fdHighPriority;

  // Separator
  private Label wlSeparator;
  private TextVar wSeparator;
  private FormData fdlSeparator, fdSeparator;

  // Enclosed
  private Label wlEnclosed;
  private TextVar wEnclosed;
  private FormData fdlEnclosed, fdEnclosed;

  // OptionEnclosed
  private Label wlOptionEnclosed;
  private Button wOptionEnclosed;
  private FormData fdlOptionEnclosed, fdOptionEnclosed;

  // Line terminated
  private Label wlLineterminated;
  private TextVar wLineterminated;
  private FormData fdlLineterminated, fdLineterminated;

  // List Columns

  private Label wlListColumn;

  private TextVar wListColumn;

  private FormData fdlListColumn, fdListColumn;

  // Limit First lines
  private Label wlLimitlines;
  private TextVar wLimitlines;
  private FormData fdlLimitlines, fdLimitlines;

  // If Output File exists
  private Label wlIfFileExists;
  private CCombo wIfFileExists;
  private FormData fdlIfFileExists, fdIfFileExists;

  // Out/ DUMP
  private Label wlOutDumpValue;
  private CCombo wOutDumpValue;
  private FormData fdlOutDumpValue, fdOutDumpValue;

  private Button wbTable;
  private Button wbListColumns;

  // Add File to result

  private Group wFileResult;
  private FormData fdFileResult;

  private Label wlAddFileToResult;
  private Button wAddFileToResult;
  private FormData fdlAddFileToResult, fdAddFileToResult;

  public ActionMysqlBulkFileDialog( Shell parent, IAction action, WorkflowMeta workflowMeta ) {
    super( parent, action, workflowMeta );
    this.action = (ActionMysqlBulkFile) action;
    if ( this.action.getName() == null ) {
      this.action.setName( BaseMessages.getString( PKG, "JobMysqlBulkFile.Name.Default" ) );
    }
  }

  public IAction open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE );
    props.setLook( shell );
    WorkflowDialog.setShellImage( shell, action );

    ModifyListener lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        action.setChanged();
      }
    };
    changed = action.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "JobMysqlBulkFile.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Filename line
    wlName = new Label( shell, SWT.RIGHT );
    wlName.setText( BaseMessages.getString( PKG, "JobMysqlBulkFile.Name.Label" ) );
    props.setLook( wlName );
    fdlName = new FormData();
    fdlName.left = new FormAttachment( 0, 0 );
    fdlName.right = new FormAttachment( middle, 0 );
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

    // Connection line
    wConnection = addConnectionLine( shell, wName, action.getDatabase(), lsMod );

    // Schema name line
    wlSchemaname = new Label( shell, SWT.RIGHT );
    wlSchemaname.setText( BaseMessages.getString( PKG, "JobMysqlBulkFile.Schemaname.Label" ) );
    props.setLook( wlSchemaname );
    fdlSchemaname = new FormData();
    fdlSchemaname.left = new FormAttachment( 0, 0 );
    fdlSchemaname.right = new FormAttachment( middle, 0 );
    fdlSchemaname.top = new FormAttachment( wConnection, margin );
    wlSchemaname.setLayoutData( fdlSchemaname );

    wSchemaname = new TextVar( workflowMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSchemaname );
    wSchemaname.setToolTipText( BaseMessages.getString( PKG, "JobMysqlBulkFile.Schemaname.Tooltip" ) );
    wSchemaname.addModifyListener( lsMod );
    fdSchemaname = new FormData();
    fdSchemaname.left = new FormAttachment( middle, 0 );
    fdSchemaname.top = new FormAttachment( wConnection, margin );
    fdSchemaname.right = new FormAttachment( 100, 0 );
    wSchemaname.setLayoutData( fdSchemaname );

    // Table name line
    wlTablename = new Label( shell, SWT.RIGHT );
    wlTablename.setText( BaseMessages.getString( PKG, "JobMysqlBulkFile.Tablename.Label" ) );
    props.setLook( wlTablename );
    fdlTablename = new FormData();
    fdlTablename.left = new FormAttachment( 0, 0 );
    fdlTablename.right = new FormAttachment( middle, 0 );
    fdlTablename.top = new FormAttachment( wSchemaname, margin );
    wlTablename.setLayoutData( fdlTablename );

    wbTable = new Button( shell, SWT.PUSH | SWT.CENTER );
    props.setLook( wbTable );
    wbTable.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    FormData fdbTable = new FormData();
    fdbTable.right = new FormAttachment( 100, 0 );
    fdbTable.top = new FormAttachment( wSchemaname, margin / 2 );
    wbTable.setLayoutData( fdbTable );
    wbTable.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        getTableName();
      }
    } );

    wTablename = new TextVar( workflowMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTablename );
    wTablename.setToolTipText( BaseMessages.getString( PKG, "JobMysqlBulkFile.Tablename.Tooltip" ) );
    wTablename.addModifyListener( lsMod );
    fdTablename = new FormData();
    fdTablename.left = new FormAttachment( middle, 0 );
    fdTablename.top = new FormAttachment( wSchemaname, margin );
    fdTablename.right = new FormAttachment( wbTable, -margin );
    wTablename.setLayoutData( fdTablename );

    // Filename line
    wlFilename = new Label( shell, SWT.RIGHT );
    wlFilename.setText( BaseMessages.getString( PKG, "JobMysqlBulkFile.Filename.Label" ) );
    props.setLook( wlFilename );
    fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment( 0, 0 );
    fdlFilename.top = new FormAttachment( wTablename, margin );
    fdlFilename.right = new FormAttachment( middle, -margin );
    wlFilename.setLayoutData( fdlFilename );

    wbFilename = new Button( shell, SWT.PUSH | SWT.CENTER );
    props.setLook( wbFilename );
    wbFilename.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    fdbFilename = new FormData();
    fdbFilename.right = new FormAttachment( 100, 0 );
    fdbFilename.top = new FormAttachment( wTablename, 0 );
    // fdbFilename.height = 22;
    wbFilename.setLayoutData( fdbFilename );

    wFilename = new TextVar( workflowMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFilename );
    wFilename.addModifyListener( lsMod );
    fdFilename = new FormData();
    fdFilename.left = new FormAttachment( middle, 0 );
    fdFilename.top = new FormAttachment( wTablename, margin );
    fdFilename.right = new FormAttachment( wbFilename, -margin );
    wFilename.setLayoutData( fdFilename );

    // Whenever something changes, set the tooltip to the expanded version:
    wFilename.addModifyListener( e -> wFilename.setToolTipText( workflowMeta.environmentSubstitute( wFilename.getText() ) ) );

    wbFilename.addListener( SWT.Selection, e-> BaseDialog.presentFileDialog( shell, wFilename, workflowMeta,
      new String[] { "*.txt", "*.csv", "*"  }, FILETYPES, true )
    );

    // High Priority ?
    wlHighPriority = new Label( shell, SWT.RIGHT );
    wlHighPriority.setText( BaseMessages.getString( PKG, "JobMysqlBulkFile.HighPriority.Label" ) );
    props.setLook( wlHighPriority );
    fdlHighPriority = new FormData();
    fdlHighPriority.left = new FormAttachment( 0, 0 );
    fdlHighPriority.top = new FormAttachment( wFilename, margin );
    fdlHighPriority.right = new FormAttachment( middle, -margin );
    wlHighPriority.setLayoutData( fdlHighPriority );
    wHighPriority = new Button( shell, SWT.CHECK );
    props.setLook( wHighPriority );
    wHighPriority.setToolTipText( BaseMessages.getString( PKG, "JobMysqlBulkFile.HighPriority.Tooltip" ) );
    fdHighPriority = new FormData();
    fdHighPriority.left = new FormAttachment( middle, 0 );
    fdHighPriority.top = new FormAttachment( wFilename, margin );
    fdHighPriority.right = new FormAttachment( 100, 0 );
    wHighPriority.setLayoutData( fdHighPriority );
    wHighPriority.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
      }
    } );

    // Out Dump
    wlOutDumpValue = new Label( shell, SWT.RIGHT );
    wlOutDumpValue.setText( BaseMessages.getString( PKG, "JobMysqlBulkFile.OutDumpValue.Label" ) );
    props.setLook( wlOutDumpValue );
    fdlOutDumpValue = new FormData();
    fdlOutDumpValue.left = new FormAttachment( 0, 0 );
    fdlOutDumpValue.right = new FormAttachment( middle, 0 );
    fdlOutDumpValue.top = new FormAttachment( wHighPriority, margin );
    wlOutDumpValue.setLayoutData( fdlOutDumpValue );
    wOutDumpValue = new CCombo( shell, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wOutDumpValue.add( BaseMessages.getString( PKG, "JobMysqlBulkFile.OutFileValue.Label" ) );
    wOutDumpValue.add( BaseMessages.getString( PKG, "JobMysqlBulkFile.DumpFileValue.Label" ) );
    wOutDumpValue.select( 0 ); // +1: starts at -1

    props.setLook( wOutDumpValue );
    fdOutDumpValue = new FormData();
    fdOutDumpValue.left = new FormAttachment( middle, 0 );
    fdOutDumpValue.top = new FormAttachment( wHighPriority, margin );
    fdOutDumpValue.right = new FormAttachment( 100, 0 );
    wOutDumpValue.setLayoutData( fdOutDumpValue );

    fdOutDumpValue = new FormData();
    fdOutDumpValue.left = new FormAttachment( middle, 0 );
    fdOutDumpValue.top = new FormAttachment( wHighPriority, margin );
    fdOutDumpValue.right = new FormAttachment( 100, 0 );
    wOutDumpValue.setLayoutData( fdOutDumpValue );

    wOutDumpValue.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        DumpFile();
      }
    } );

    // Separator
    wlSeparator = new Label( shell, SWT.RIGHT );
    wlSeparator.setText( BaseMessages.getString( PKG, "JobMysqlBulkFile.Separator.Label" ) );
    props.setLook( wlSeparator );
    fdlSeparator = new FormData();
    fdlSeparator.left = new FormAttachment( 0, 0 );
    fdlSeparator.right = new FormAttachment( middle, 0 );
    fdlSeparator.top = new FormAttachment( wOutDumpValue, margin );
    wlSeparator.setLayoutData( fdlSeparator );

    wSeparator = new TextVar( workflowMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSeparator );
    wSeparator.addModifyListener( lsMod );
    fdSeparator = new FormData();
    fdSeparator.left = new FormAttachment( middle, 0 );
    fdSeparator.top = new FormAttachment( wOutDumpValue, margin );
    fdSeparator.right = new FormAttachment( 100, 0 );
    wSeparator.setLayoutData( fdSeparator );

    // enclosed
    wlEnclosed = new Label( shell, SWT.RIGHT );
    wlEnclosed.setText( BaseMessages.getString( PKG, "JobMysqlBulkFile.Enclosed.Label" ) );
    props.setLook( wlEnclosed );
    fdlEnclosed = new FormData();
    fdlEnclosed.left = new FormAttachment( 0, 0 );
    fdlEnclosed.right = new FormAttachment( middle, 0 );
    fdlEnclosed.top = new FormAttachment( wSeparator, margin );
    wlEnclosed.setLayoutData( fdlEnclosed );

    wEnclosed = new TextVar( workflowMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wEnclosed );
    wEnclosed.addModifyListener( lsMod );
    fdEnclosed = new FormData();
    fdEnclosed.left = new FormAttachment( middle, 0 );
    fdEnclosed.top = new FormAttachment( wSeparator, margin );
    fdEnclosed.right = new FormAttachment( 100, 0 );
    wEnclosed.setLayoutData( fdEnclosed );

    // Optionnally enclosed ?
    wlOptionEnclosed = new Label( shell, SWT.RIGHT );
    wlOptionEnclosed.setText( BaseMessages.getString( PKG, "JobMysqlBulkFile.OptionEnclosed.Label" ) );
    props.setLook( wlOptionEnclosed );
    fdlOptionEnclosed = new FormData();
    fdlOptionEnclosed.left = new FormAttachment( 0, 0 );
    fdlOptionEnclosed.top = new FormAttachment( wEnclosed, margin );
    fdlOptionEnclosed.right = new FormAttachment( middle, -margin );
    wlOptionEnclosed.setLayoutData( fdlOptionEnclosed );
    wOptionEnclosed = new Button( shell, SWT.CHECK );
    props.setLook( wOptionEnclosed );
    wOptionEnclosed.setToolTipText( BaseMessages.getString( PKG, "JobMysqlBulkFile.OptionEnclosed.Tooltip" ) );
    fdOptionEnclosed = new FormData();
    fdOptionEnclosed.left = new FormAttachment( middle, 0 );
    fdOptionEnclosed.top = new FormAttachment( wEnclosed, margin );
    fdOptionEnclosed.right = new FormAttachment( 100, 0 );
    wOptionEnclosed.setLayoutData( fdOptionEnclosed );
    wOptionEnclosed.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
      }
    } );

    // Line terminated
    wlLineterminated = new Label( shell, SWT.RIGHT );
    wlLineterminated.setText( BaseMessages.getString( PKG, "JobMysqlBulkFile.Lineterminated.Label" ) );
    props.setLook( wlLineterminated );
    fdlLineterminated = new FormData();
    fdlLineterminated.left = new FormAttachment( 0, 0 );
    fdlLineterminated.right = new FormAttachment( middle, 0 );
    fdlLineterminated.top = new FormAttachment( wOptionEnclosed, margin );
    wlLineterminated.setLayoutData( fdlLineterminated );

    wLineterminated = new TextVar( workflowMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wLineterminated );
    wLineterminated.addModifyListener( lsMod );
    fdLineterminated = new FormData();
    fdLineterminated.left = new FormAttachment( middle, 0 );
    fdLineterminated.top = new FormAttachment( wOptionEnclosed, margin );
    fdLineterminated.right = new FormAttachment( 100, 0 );
    wLineterminated.setLayoutData( fdLineterminated );

    // List of columns to set for
    wlListColumn = new Label( shell, SWT.RIGHT );
    wlListColumn.setText( BaseMessages.getString( PKG, "JobMysqlBulkFile.ListColumn.Label" ) );
    props.setLook( wlListColumn );
    fdlListColumn = new FormData();
    fdlListColumn.left = new FormAttachment( 0, 0 );
    fdlListColumn.right = new FormAttachment( middle, 0 );
    fdlListColumn.top = new FormAttachment( wLineterminated, margin );
    wlListColumn.setLayoutData( fdlListColumn );

    wbListColumns = new Button( shell, SWT.PUSH | SWT.CENTER );
    props.setLook( wbListColumns );
    wbListColumns.setText( BaseMessages.getString( PKG, "System.Button.Edit" ) );
    FormData fdbListColumns = new FormData();
    fdbListColumns.right = new FormAttachment( 100, 0 );
    fdbListColumns.top = new FormAttachment( wLineterminated, margin );
    wbListColumns.setLayoutData( fdbListColumns );
    wbListColumns.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        getListColumns();
      }
    } );

    wListColumn = new TextVar( workflowMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wListColumn );
    wListColumn.setToolTipText( BaseMessages.getString( PKG, "JobMysqlBulkFile.ListColumn.Tooltip" ) );
    wListColumn.addModifyListener( lsMod );
    fdListColumn = new FormData();
    fdListColumn.left = new FormAttachment( middle, 0 );
    fdListColumn.top = new FormAttachment( wLineterminated, margin );
    fdListColumn.right = new FormAttachment( wbListColumns, -margin );
    wListColumn.setLayoutData( fdListColumn );

    // Nbr of lines to Limit
    wlLimitlines = new Label( shell, SWT.RIGHT );
    wlLimitlines.setText( BaseMessages.getString( PKG, "JobMysqlBulkFile.Limitlines.Label" ) );
    props.setLook( wlLimitlines );
    fdlLimitlines = new FormData();
    fdlLimitlines.left = new FormAttachment( 0, 0 );
    fdlLimitlines.right = new FormAttachment( middle, 0 );
    fdlLimitlines.top = new FormAttachment( wListColumn, margin );
    wlLimitlines.setLayoutData( fdlLimitlines );

    wLimitlines = new TextVar( workflowMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wLimitlines );
    wLimitlines.setToolTipText( BaseMessages.getString( PKG, "JobMysqlBulkFile.Limitlines.Tooltip" ) );
    wLimitlines.addModifyListener( lsMod );
    fdLimitlines = new FormData();
    fdLimitlines.left = new FormAttachment( middle, 0 );
    fdLimitlines.top = new FormAttachment( wListColumn, margin );
    fdLimitlines.right = new FormAttachment( 100, 0 );
    wLimitlines.setLayoutData( fdLimitlines );

    // IF File Exists
    wlIfFileExists = new Label( shell, SWT.RIGHT );
    wlIfFileExists.setText( BaseMessages.getString( PKG, "JobMysqlBulkFile.IfFileExists.Label" ) );
    props.setLook( wlIfFileExists );
    fdlIfFileExists = new FormData();
    fdlIfFileExists.left = new FormAttachment( 0, 0 );
    fdlIfFileExists.right = new FormAttachment( middle, 0 );
    fdlIfFileExists.top = new FormAttachment( wLimitlines, margin );
    wlIfFileExists.setLayoutData( fdlIfFileExists );
    wIfFileExists = new CCombo( shell, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wIfFileExists.add( BaseMessages.getString( PKG, "JobMysqlBulkFile.Create_NewFile_IfFileExists.Label" ) );
    wIfFileExists.add( BaseMessages.getString( PKG, "JobMysqlBulkFile.Do_Nothing_IfFileExists.Label" ) );
    wIfFileExists.add( BaseMessages.getString( PKG, "JobMysqlBulkFile.Fail_IfFileExists.Label" ) );
    wIfFileExists.select( 2 ); // +1: starts at -1

    props.setLook( wIfFileExists );
    fdIfFileExists = new FormData();
    fdIfFileExists.left = new FormAttachment( middle, 0 );
    fdIfFileExists.top = new FormAttachment( wLimitlines, margin );
    fdIfFileExists.right = new FormAttachment( 100, 0 );
    wIfFileExists.setLayoutData( fdIfFileExists );

    fdIfFileExists = new FormData();
    fdIfFileExists.left = new FormAttachment( middle, 0 );
    fdIfFileExists.top = new FormAttachment( wLimitlines, margin );
    fdIfFileExists.right = new FormAttachment( 100, 0 );
    wIfFileExists.setLayoutData( fdIfFileExists );

    // fileresult grouping?
    // ////////////////////////
    // START OF LOGGING GROUP///
    // /
    wFileResult = new Group( shell, SWT.SHADOW_NONE );
    props.setLook( wFileResult );
    wFileResult.setText( BaseMessages.getString( PKG, "JobMysqlBulkFile.FileResult.Group.Label" ) );

    FormLayout groupLayout = new FormLayout();
    groupLayout.marginWidth = 10;
    groupLayout.marginHeight = 10;

    wFileResult.setLayout( groupLayout );

    // Add file to result
    wlAddFileToResult = new Label( wFileResult, SWT.RIGHT );
    wlAddFileToResult.setText( BaseMessages.getString( PKG, "JobMysqlBulkFile.AddFileToResult.Label" ) );
    props.setLook( wlAddFileToResult );
    fdlAddFileToResult = new FormData();
    fdlAddFileToResult.left = new FormAttachment( 0, 0 );
    fdlAddFileToResult.top = new FormAttachment( wIfFileExists, margin );
    fdlAddFileToResult.right = new FormAttachment( middle, -margin );
    wlAddFileToResult.setLayoutData( fdlAddFileToResult );
    wAddFileToResult = new Button( wFileResult, SWT.CHECK );
    props.setLook( wAddFileToResult );
    wAddFileToResult.setToolTipText( BaseMessages.getString( PKG, "JobMysqlBulkFile.AddFileToResult.Tooltip" ) );
    fdAddFileToResult = new FormData();
    fdAddFileToResult.left = new FormAttachment( middle, 0 );
    fdAddFileToResult.top = new FormAttachment( wIfFileExists, margin );
    fdAddFileToResult.right = new FormAttachment( 100, 0 );
    wAddFileToResult.setLayoutData( fdAddFileToResult );
    wAddFileToResult.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
      }
    } );

    fdFileResult = new FormData();
    fdFileResult.left = new FormAttachment( 0, margin );
    fdFileResult.top = new FormAttachment( wIfFileExists, margin );
    fdFileResult.right = new FormAttachment( 100, -margin );
    wFileResult.setLayoutData( fdFileResult );
    // ///////////////////////////////////////////////////////////
    // / END OF LOGGING GROUP
    // ///////////////////////////////////////////////////////////

    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    FormData fd = new FormData();
    fd.right = new FormAttachment( 50, -10 );
    fd.bottom = new FormAttachment( 100, 0 );
    fd.width = 100;
    wOk.setLayoutData( fd );

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    fd = new FormData();
    fd.left = new FormAttachment( 50, 10 );
    fd.bottom = new FormAttachment( 100, 0 );
    fd.width = 100;
    wCancel.setLayoutData( fd );

    // Add listeners
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };
    lsOk = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };

    wCancel.addListener( SWT.Selection, lsCancel );
    wOk.addListener( SWT.Selection, lsOk );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wName.addSelectionListener( lsDef );
    wTablename.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();

    BaseTransformDialog.setSize( shell );

    shell.open();
    props.setDialogSize( shell, "JobMysqlBulkFileDialogSize" );
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return action;
  }

  public void DumpFile() {

    action.setChanged();
    if ( wOutDumpValue.getSelectionIndex() == 0 ) {
      wSeparator.setEnabled( true );
      wEnclosed.setEnabled( true );
      wLineterminated.setEnabled( true );

    } else {
      wSeparator.setEnabled( false );
      wEnclosed.setEnabled( false );
      wLineterminated.setEnabled( false );

    }

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
    wName.setText( Const.NVL( action.getName(), "" ) );
    if ( action.getSchemaname() != null ) {
      wTablename.setText( action.getSchemaname() );
    }
    if ( action.getTablename() != null ) {
      wTablename.setText( action.getTablename() );
    }
    if ( action.getFilename() != null ) {
      wFilename.setText( action.getFilename() );
    }
    if ( action.getSeparator() != null ) {
      wSeparator.setText( action.getSeparator() );
    }

    if ( action.getEnclosed() != null ) {
      wEnclosed.setText( action.getEnclosed() );
    }
    wOptionEnclosed.setSelection( action.isOptionEnclosed() );

    if ( action.getLineterminated() != null ) {
      wLineterminated.setText( action.getLineterminated() );
    }

    wHighPriority.setSelection( action.isHighPriority() );
    wOptionEnclosed.setSelection( action.isOptionEnclosed() );

    if ( action.getLimitlines() != null ) {
      wLimitlines.setText( action.getLimitlines() );
    } else {
      wLimitlines.setText( "0" );
    }

    if ( action.getListColumn() != null ) {
      wListColumn.setText( action.getListColumn() );
    }

    if ( action.outdumpvalue >= 0 ) {
      wOutDumpValue.select( action.outdumpvalue );
    } else {
      wOutDumpValue.select( 0 ); // NORMAL priority
    }

    if ( action.iffileexists >= 0 ) {
      wIfFileExists.select( action.iffileexists );
    } else {
      wIfFileExists.select( 2 ); // FAIL
    }

    if ( action.getDatabase() != null ) {
      wConnection.setText( action.getDatabase().getName() );
    }

    wAddFileToResult.setSelection( action.isAddFileToResult() );

    wName.selectAll();
    wName.setFocus();
  }

  private void cancel() {
    action.setChanged( changed );
    action = null;
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
    action.setName( wName.getText() );
    action.setDatabase( workflowMeta.findDatabase( wConnection.getText() ) );
    action.setSchemaname( wSchemaname.getText() );
    action.setTablename( wTablename.getText() );
    action.setFilename( wFilename.getText() );
    action.setSeparator( wSeparator.getText() );
    action.setEnclosed( wEnclosed.getText() );
    action.setOptionEnclosed( wOptionEnclosed.getSelection() );
    action.setLineterminated( wLineterminated.getText() );

    action.setLimitlines( wLimitlines.getText() );
    action.setListColumn( wListColumn.getText() );

    action.outdumpvalue = wOutDumpValue.getSelectionIndex();

    action.setHighPriority( wHighPriority.getSelection() );
    action.iffileexists = wIfFileExists.getSelectionIndex();

    action.setAddFileToResult( wAddFileToResult.getSelection() );

    dispose();
  }

  private void getTableName() {
    String databaseName = wConnection.getText();
    if ( StringUtils.isNotEmpty( databaseName ) ) {
      DatabaseMeta databaseMeta = workflowMeta.findDatabase( databaseName );
      if ( databaseMeta != null ) {
        DatabaseExplorerDialog std = new DatabaseExplorerDialog( shell, SWT.NONE, databaseMeta, workflowMeta.getDatabases() );
        std.setSelectedSchemaAndTable( wSchemaname.getText(), wTablename.getText() );
        if ( std.open() ) {
          // wSchemaname.setText(Const.NVL(std.getSchemaName(), ""));
          wTablename.setText( Const.NVL( std.getTableName(), "" ) );
        }
      } else {
        MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
        mb.setMessage( BaseMessages.getString( PKG, "JobMysqlBulkFile.ConnectionError2.DialogMessage" ) );
        mb.setText( BaseMessages.getString( PKG, "System.Dialog.Error.Title" ) );
        mb.open();
      }
    }
  }

  /**
   * Get a list of columns, comma separated, allow the user to select from it.
   */
  private void getListColumns() {
    if ( !Utils.isEmpty( wTablename.getText() ) ) {
      DatabaseMeta databaseMeta = workflowMeta.findDatabase( wConnection.getText() );
      if ( databaseMeta != null ) {
        Database database = new Database( loggingObject, databaseMeta );
        database.shareVariablesWith( workflowMeta );
        try {
          database.connect();
          IRowMeta row =
            database.getTableFieldsMeta( wSchemaname.getText(), wTablename.getText() );
          String[] available = row.getFieldNames();

          String[] source = wListColumn.getText().split( "," );
          for ( int i = 0; i < source.length; i++ ) {
            source[ i ] = Const.trim( source[ i ] );
          }
          int[] idxSource = Const.indexsOfStrings( source, available );
          EnterSelectionDialog dialog = new EnterSelectionDialog( shell, available,
            BaseMessages.getString( PKG, "JobMysqlBulkFile.SelectColumns.Title" ),
            BaseMessages.getString( PKG, "JobMysqlBulkFile.SelectColumns.Message" ) );
          dialog.setMulti( true );
          dialog.setAvoidQuickSearch();
          dialog.setSelectedNrs( idxSource );
          if ( dialog.open() != null ) {
            String columns = "";
            int[] idx = dialog.getSelectionIndeces();
            for ( int i = 0; i < idx.length; i++ ) {
              if ( i > 0 ) {
                columns += ", ";
              }
              columns += available[ idx[ i ] ];
            }
            wListColumn.setText( columns );
          }
        } catch ( HopDatabaseException e ) {
          new ErrorDialog( shell, BaseMessages.getString( PKG, "System.Dialog.Error.Title" ), BaseMessages
            .getString( PKG, "JobMysqlBulkFile.ConnectionError2.DialogMessage" ), e );
        } finally {
          database.disconnect();
        }
      }
    }
  }
}
