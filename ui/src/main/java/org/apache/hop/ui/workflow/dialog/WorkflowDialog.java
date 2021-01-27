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

package org.apache.hop.ui.workflow.dialog;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.parameters.DuplicateParamException;
import org.apache.hop.core.parameters.UnknownParamException;
import org.apache.hop.core.plugins.ActionPluginType;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.util.HelpUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import java.util.ArrayList;

/**
 * Allows you to edit the Workflow settings. Just feed it a WorkflowMeta object.
 *
 * @author Matt Casters
 * @since 02-jul-2003
 */
public class WorkflowDialog extends Dialog {
  private static final Class<?> PKG = WorkflowDialog.class; // For Translator

  private static Class<?> PKGBASE = WorkflowMeta.class;

  private CTabFolder wTabFolder;

  private final PropsUi props;

  private Text wWorkflowName;
  private Button wNameFilenameSync;
  private Text wFilename;

  protected Button wOk, wCancel;

  private final IVariables variables;
  private WorkflowMeta workflowMeta;

  private Shell shell;

  private SelectionAdapter lsDef;

  private ModifyListener lsMod;

  private boolean changed;

  // param tab
  private TableView wParamFields;

  // Workflow description
  private Text wDescription;

  // Extended description
  private Label wlExtendedDescription;

  private Text wExtendedDescription;

  private FormData fdlExtendedDescription, fdExtendedDescription;

  // Workflow Status
  private Label wlWorkflowStatus;

  private CCombo wWorkflowStatus;

  private FormData fdlWorkflowStatus, fdWorkflowStatus;

  // Workflow version
  private Text wVersion;

  private int middle;

  private int margin;

  // Workflow creation
  private Text wCreateUser;

  private Text wCreateDate;

  // Workflow modification
  private Text wModUser;

  private Text wModDate;


  private int previousLogTableIndex = 0;

  private ArrayList<IWorkflowDialogPlugin> extraTabs;

  public WorkflowDialog( Shell parent, int style, IVariables variables, WorkflowMeta workflowMeta ) {
    super( parent, style );
    this.variables = variables;
    this.workflowMeta = workflowMeta;
    this.props = PropsUi.getInstance();
  }

  public WorkflowMeta open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN | SWT.APPLICATION_MODAL );
    props.setLook( shell );
    shell.setImage( GuiResource.getInstance().getImageWorkflow() );

    lsMod = e -> changed = true;

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "WorkflowDialog.WorkflowProperties.ShellText" ) );

    middle = props.getMiddlePct();
    margin = props.getMargin();

    wTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );

    addWorkflowTab();
    addParamTab();
    addSettingsTab();

    // See if there are any other tabs to be added...
    extraTabs = new ArrayList<>();
    java.util.List<IPlugin> jobDialogPlugins = PluginRegistry.getInstance().getPlugins( WorkflowDialogPluginType.class );
    for ( IPlugin jobDialogPlugin : jobDialogPlugins ) {
      try {
        IWorkflowDialogPlugin extraTab = (IWorkflowDialogPlugin) PluginRegistry.getInstance().loadClass( jobDialogPlugin );
        extraTab.addTab( workflowMeta, parent, wTabFolder );
        extraTabs.add( extraTab );
      } catch ( Exception e ) {
        new ErrorDialog(
          shell, "Error", "Error loading workflow dialog plugin with id " + jobDialogPlugin.getIds()[ 0 ], e );
      }
    }

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( 0, 0 );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( 100, -50 );
    wTabFolder.setLayoutData( fdTabFolder );

    // THE BUTTONS
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOk.addListener( SWT.Selection, e->ok() );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, e->cancel() );

    BaseTransformDialog.positionBottomButtons( shell, new Button[] { wOk, wCancel }, props.getMargin(), null );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wWorkflowName.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    wTabFolder.setSelection( 0 );
    getData();
    BaseTransformDialog.setSize( shell );

    changed = false;

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return workflowMeta;
  }

  public String[] listParameterNames() {
    int count = wParamFields.nrNonEmpty();
    java.util.List<String> list = new ArrayList<>();
    for ( int i = 0; i < count; i++ ) {
      TableItem item = wParamFields.getNonEmpty( i );
      String parameterName = item.getText( 1 );
      if ( !Utils.isEmpty( parameterName ) ) {
        if ( !list.contains( parameterName ) ) {
          list.add( parameterName );
        }
      }
    }
    return list.toArray( new String[ list.size() ] );
  }

  private void addWorkflowTab() {
    // ////////////////////////
    // START OF JOB TAB///
    // /
    CTabItem wWorkflowTab = new CTabItem( wTabFolder, SWT.NONE );
    wWorkflowTab.setText( BaseMessages.getString( PKG, "WorkflowDialog.WorkflowTab.Label" ) );

    Composite wWorkflowComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wWorkflowComp );

    FormLayout workflowLayout = new FormLayout();
    workflowLayout.marginWidth = props.getMargin();
    workflowLayout.marginHeight = props.getMargin();
    wWorkflowComp.setLayout( workflowLayout );

    // Workflow name:
    Label wlWorkflowName = new Label( wWorkflowComp, SWT.RIGHT );
    wlWorkflowName.setText( BaseMessages.getString( PKG, "WorkflowDialog.WorkflowName.Label" ) );
    props.setLook( wlWorkflowName );
    FormData fdlWorkflowName = new FormData();
    fdlWorkflowName.left = new FormAttachment( 0, 0 );
    fdlWorkflowName.right = new FormAttachment( middle, -margin );
    fdlWorkflowName.top = new FormAttachment( 0, margin );
    wlWorkflowName.setLayoutData( fdlWorkflowName );
    wWorkflowName = new Text( wWorkflowComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wWorkflowName );
    wWorkflowName.addModifyListener( lsMod );
    FormData fdWorkflowName = new FormData();
    fdWorkflowName.left = new FormAttachment( middle, 0 );
    fdWorkflowName.top = new FormAttachment( 0, margin );
    fdWorkflowName.right = new FormAttachment( 100, 0 );
    wWorkflowName.setLayoutData( fdWorkflowName );
    Control lastControl = wWorkflowName;

    // Synchronize name with filename?
    //
    Label wlNameFilenameSync = new Label( wWorkflowComp, SWT.RIGHT );
    wlNameFilenameSync.setText( BaseMessages.getString( PKG, "WorkflowDialog.NameFilenameSync.Label" ) );
    props.setLook( wlNameFilenameSync );
    FormData fdlNameFilenameSync = new FormData();
    fdlNameFilenameSync.left = new FormAttachment( 0, 0 );
    fdlNameFilenameSync.right = new FormAttachment( middle, -margin );
    fdlNameFilenameSync.top = new FormAttachment( lastControl, margin );
    wlNameFilenameSync.setLayoutData( fdlNameFilenameSync );
    wNameFilenameSync = new Button( wWorkflowComp, SWT.CHECK );
    props.setLook( wNameFilenameSync );
    FormData fdNameFilenameSync = new FormData();
    fdNameFilenameSync.left = new FormAttachment( middle, 0 );
    fdNameFilenameSync.top = new FormAttachment( wlNameFilenameSync, 0, SWT.CENTER );
    fdNameFilenameSync.right = new FormAttachment( 100, 0 );
    wNameFilenameSync.setLayoutData( fdNameFilenameSync );
    wNameFilenameSync.addListener( SWT.Selection, this::updateNameFilenameSync);
    lastControl = wNameFilenameSync;

    // JobFilename:
    Label wlFilename = new Label( wWorkflowComp, SWT.RIGHT );
    wlFilename.setText( BaseMessages.getString( PKG, "WorkflowDialog.Filename.Label" ) );
    props.setLook( wlFilename );
    FormData fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment( 0, 0 );
    fdlFilename.right = new FormAttachment( middle, -margin );
    fdlFilename.top = new FormAttachment( lastControl, margin );
    wlFilename.setLayoutData( fdlFilename );
    wFilename = new Text( wWorkflowComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFilename );
    wFilename.addModifyListener( lsMod );
    FormData fdFilename = new FormData();
    fdFilename.left = new FormAttachment( middle, 0 );
    fdFilename.top = new FormAttachment( wlFilename, 0, SWT.CENTER );
    fdFilename.right = new FormAttachment( 100, 0 );
    wFilename.setLayoutData( fdFilename );
    wFilename.setEditable( false );
    wFilename.setBackground( GuiResource.getInstance().getColorLightGray() );

    // Workflow description:
    Label wlDescription = new Label( wWorkflowComp, SWT.RIGHT );
    wlDescription.setText( BaseMessages.getString( PKG, "WorkflowDialog.Description.Label" ) );
    props.setLook( wlDescription );
    FormData fdlDescription = new FormData();
    fdlDescription.left = new FormAttachment( 0, 0 );
    fdlDescription.right = new FormAttachment( middle, -margin );
    fdlDescription.top = new FormAttachment( wFilename, margin );
    wlDescription.setLayoutData( fdlDescription );
    wDescription = new Text( wWorkflowComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wDescription );
    wDescription.addModifyListener( lsMod );
    FormData fdDescription = new FormData();
    fdDescription.left = new FormAttachment( middle, 0 );
    fdDescription.top = new FormAttachment( wFilename, margin );
    fdDescription.right = new FormAttachment( 100, 0 );
    wDescription.setLayoutData( fdDescription );

    // Pipeline Extended description
    wlExtendedDescription = new Label( wWorkflowComp, SWT.RIGHT );
    wlExtendedDescription.setText( BaseMessages.getString( PKG, "WorkflowDialog.Extendeddescription.Label" ) );
    props.setLook( wlExtendedDescription );
    fdlExtendedDescription = new FormData();
    fdlExtendedDescription.left = new FormAttachment( 0, 0 );
    fdlExtendedDescription.top = new FormAttachment( wDescription, margin );
    fdlExtendedDescription.right = new FormAttachment( middle, -margin );
    wlExtendedDescription.setLayoutData( fdlExtendedDescription );

    wExtendedDescription = new Text( wWorkflowComp, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL );
    props.setLook( wExtendedDescription, Props.WIDGET_STYLE_FIXED );
    wExtendedDescription.addModifyListener( lsMod );
    fdExtendedDescription = new FormData();
    fdExtendedDescription.left = new FormAttachment( middle, 0 );
    fdExtendedDescription.top = new FormAttachment( wDescription, margin );
    fdExtendedDescription.right = new FormAttachment( 100, 0 );
    fdExtendedDescription.bottom = new FormAttachment( 50, -margin );
    wExtendedDescription.setLayoutData( fdExtendedDescription );

    // Pipeline Status
    wlWorkflowStatus = new Label( wWorkflowComp, SWT.RIGHT );
    wlWorkflowStatus.setText( BaseMessages.getString( PKG, "WorkflowDialog.WorkflowStatus.Label" ) );
    props.setLook( wlWorkflowStatus );
    fdlWorkflowStatus = new FormData();
    fdlWorkflowStatus.left = new FormAttachment( 0, 0 );
    fdlWorkflowStatus.right = new FormAttachment( middle, 0 );
    fdlWorkflowStatus.top = new FormAttachment( wExtendedDescription, margin * 2 );
    wlWorkflowStatus.setLayoutData( fdlWorkflowStatus );
    wWorkflowStatus = new CCombo( wWorkflowComp, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wWorkflowStatus.add( BaseMessages.getString( PKG, "WorkflowDialog.Draft_WorkflowStatus.Label" ) );
    wWorkflowStatus.add( BaseMessages.getString( PKG, "WorkflowDialog.Production_WorkflowStatus.Label" ) );
    wWorkflowStatus.add( "" );
    wWorkflowStatus.select( -1 ); // +1: starts at -1

    props.setLook( wWorkflowStatus );
    fdWorkflowStatus = new FormData();
    fdWorkflowStatus.left = new FormAttachment( middle, 0 );
    fdWorkflowStatus.top = new FormAttachment( wExtendedDescription, margin * 2 );
    fdWorkflowStatus.right = new FormAttachment( 100, 0 );
    wWorkflowStatus.setLayoutData( fdWorkflowStatus );

    // Workflow version:
    Label wlVersion = new Label( wWorkflowComp, SWT.RIGHT );
    wlVersion.setText( BaseMessages.getString( PKG, "WorkflowDialog.Version.Label" ) );
    props.setLook( wlVersion );
    FormData fdlVersion = new FormData();
    fdlVersion.left = new FormAttachment( 0, 0 );
    fdlVersion.right = new FormAttachment( middle, -margin );
    fdlVersion.top = new FormAttachment( wWorkflowStatus, margin );
    wlVersion.setLayoutData( fdlVersion );
    wVersion = new Text( wWorkflowComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wVersion );
    wVersion.addModifyListener( lsMod );
    FormData fdVersion = new FormData();
    fdVersion.left = new FormAttachment( middle, 0 );
    fdVersion.top = new FormAttachment( wWorkflowStatus, margin );
    fdVersion.right = new FormAttachment( 100, 0 );
    wVersion.setLayoutData( fdVersion );


    // Create User:
    Label wlCreateUser = new Label( wWorkflowComp, SWT.RIGHT );
    wlCreateUser.setText( BaseMessages.getString( PKG, "WorkflowDialog.CreateUser.Label" ) );
    props.setLook( wlCreateUser );
    FormData fdlCreateUser = new FormData();
    fdlCreateUser.left = new FormAttachment( 0, 0 );
    fdlCreateUser.right = new FormAttachment( middle, -margin );
    fdlCreateUser.top = new FormAttachment( wVersion, margin );
    wlCreateUser.setLayoutData( fdlCreateUser );
    wCreateUser = new Text( wWorkflowComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wCreateUser );
    wCreateUser.setEditable( false );
    wCreateUser.addModifyListener( lsMod );
    FormData fdCreateUser = new FormData();
    fdCreateUser.left = new FormAttachment( middle, 0 );
    fdCreateUser.top = new FormAttachment( wVersion, margin );
    fdCreateUser.right = new FormAttachment( 100, 0 );
    wCreateUser.setLayoutData( fdCreateUser );

    // Created Date:
    Label wlCreateDate = new Label( wWorkflowComp, SWT.RIGHT );
    wlCreateDate.setText( BaseMessages.getString( PKG, "WorkflowDialog.CreateDate.Label" ) );
    props.setLook( wlCreateDate );
    FormData fdlCreateDate = new FormData();
    fdlCreateDate.left = new FormAttachment( 0, 0 );
    fdlCreateDate.right = new FormAttachment( middle, -margin );
    fdlCreateDate.top = new FormAttachment( wCreateUser, margin );
    wlCreateDate.setLayoutData( fdlCreateDate );
    wCreateDate = new Text( wWorkflowComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wCreateDate );
    wCreateDate.setEditable( false );
    wCreateDate.addModifyListener( lsMod );
    FormData fdCreateDate = new FormData();
    fdCreateDate.left = new FormAttachment( middle, 0 );
    fdCreateDate.top = new FormAttachment( wCreateUser, margin );
    fdCreateDate.right = new FormAttachment( 100, 0 );
    wCreateDate.setLayoutData( fdCreateDate );

    // Modified User:
    Label wlModUser = new Label( wWorkflowComp, SWT.RIGHT );
    wlModUser.setText( BaseMessages.getString( PKG, "WorkflowDialog.LastModifiedUser.Label" ) );
    props.setLook( wlModUser );
    FormData fdlModUser = new FormData();
    fdlModUser.left = new FormAttachment( 0, 0 );
    fdlModUser.right = new FormAttachment( middle, -margin );
    fdlModUser.top = new FormAttachment( wCreateDate, margin );
    wlModUser.setLayoutData( fdlModUser );
    wModUser = new Text( wWorkflowComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wModUser );
    wModUser.setEditable( false );
    wModUser.addModifyListener( lsMod );
    FormData fdModUser = new FormData();
    fdModUser.left = new FormAttachment( middle, 0 );
    fdModUser.top = new FormAttachment( wCreateDate, margin );
    fdModUser.right = new FormAttachment( 100, 0 );
    wModUser.setLayoutData( fdModUser );

    // Modified Date:
    Label wlModDate = new Label( wWorkflowComp, SWT.RIGHT );
    wlModDate.setText( BaseMessages.getString( PKG, "WorkflowDialog.LastModifiedDate.Label" ) );
    props.setLook( wlModDate );
    FormData fdlModDate = new FormData();
    fdlModDate.left = new FormAttachment( 0, 0 );
    fdlModDate.right = new FormAttachment( middle, -margin );
    fdlModDate.top = new FormAttachment( wModUser, margin );
    wlModDate.setLayoutData( fdlModDate );
    wModDate = new Text( wWorkflowComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wModDate );
    wModDate.setEditable( false );
    wModDate.addModifyListener( lsMod );
    FormData fdModDate = new FormData();
    fdModDate.left = new FormAttachment( middle, 0 );
    fdModDate.top = new FormAttachment( wModUser, margin );
    fdModDate.right = new FormAttachment( 100, 0 );
    wModDate.setLayoutData( fdModDate );

    FormData fdWorkflowComp = new FormData();
    fdWorkflowComp.left = new FormAttachment( 0, 0 );
    fdWorkflowComp.top = new FormAttachment( 0, 0 );
    fdWorkflowComp.right = new FormAttachment( 100, 0 );
    fdWorkflowComp.bottom = new FormAttachment( 100, 0 );

    wWorkflowComp.setLayoutData( fdWorkflowComp );
    wWorkflowTab.setControl( wWorkflowComp );

    // ///////////////////////////////////////////////////////////
    // / END OF JOB TAB
    // ///////////////////////////////////////////////////////////
  }

  private void updateNameFilenameSync( Event event ) {
    String name = wWorkflowName.getText();
    String filename = wFilename.getText();
    boolean sync = wNameFilenameSync.getSelection();

    String actualName = WorkflowMeta.extractNameFromFilename( sync, name, filename, WorkflowMeta.WORKFLOW_EXTENSION );
    wWorkflowName.setEnabled( !sync );
    wWorkflowName.setEditable( !sync );

    wWorkflowName.setText( Const.NVL(actualName, "") );
  }

  private void addParamTab() {
    // ////////////////////////
    // START OF PARAM TAB
    // /
    CTabItem wParamTab = new CTabItem( wTabFolder, SWT.NONE );
    wParamTab.setText( BaseMessages.getString( PKG, "WorkflowDialog.ParamTab.Label" ) );

    FormLayout paramLayout = new FormLayout();
    paramLayout.marginWidth = props.getMargin();
    paramLayout.marginHeight = props.getMargin();

    Composite wParamComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wParamComp );
    wParamComp.setLayout( paramLayout );

    Label wlFields = new Label( wParamComp, SWT.RIGHT );
    wlFields.setText( BaseMessages.getString( PKG, "WorkflowDialog.Parameters.Label" ) );
    props.setLook( wlFields );
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment( 0, 0 );
    fdlFields.top = new FormAttachment( 0, 0 );
    wlFields.setLayoutData( fdlFields );

    final int FieldsCols = 3;
    final int FieldsRows = workflowMeta.listParameters().length;

    ColumnInfo[] colinf = new ColumnInfo[ FieldsCols ];
    colinf[ 0 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "WorkflowDialog.ColumnInfo.Parameter.Label" ), ColumnInfo.COLUMN_TYPE_TEXT,
        false );
    colinf[ 1 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "WorkflowDialog.ColumnInfo.Default.Label" ), ColumnInfo.COLUMN_TYPE_TEXT,
        false );
    colinf[ 2 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "WorkflowDialog.ColumnInfo.Description.Label" ), ColumnInfo.COLUMN_TYPE_TEXT,
        false );

    wParamFields =
      new TableView(
        variables, wParamComp, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, FieldsRows, lsMod, props );

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment( wlFields, margin );
    fdFields.right = new FormAttachment( 100, 0 );
    fdFields.bottom = new FormAttachment( 100, 0 );
    wParamFields.setLayoutData( fdFields );

    FormData fdDepComp = new FormData();
    fdDepComp.left = new FormAttachment( 0, 0 );
    fdDepComp.top = new FormAttachment( 0, 0 );
    fdDepComp.right = new FormAttachment( 100, 0 );
    fdDepComp.bottom = new FormAttachment( 100, 0 );
    wParamComp.setLayoutData( fdDepComp );

    wParamComp.layout();
    wParamTab.setControl( wParamComp );

    // ///////////////////////////////////////////////////////////
    // / END OF PARAM TAB
    // ///////////////////////////////////////////////////////////
  }


  private void addSettingsTab() {
    // ////////////////////////
    // START OF SETTINGS TAB///
    // /
    CTabItem wSettingsTab = new CTabItem( wTabFolder, SWT.NONE );
    wSettingsTab.setText( BaseMessages.getString( PKG, "WorkflowDialog.SettingsTab.Label" ) );

    FormLayout LogLayout = new FormLayout();
    LogLayout.marginWidth = props.getMargin();
    LogLayout.marginHeight = props.getMargin();

    Composite wSettingsComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wSettingsComp );
    wSettingsComp.setLayout( LogLayout );


    FormData fdLogComp = new FormData();
    fdLogComp.left = new FormAttachment( 0, 0 );
    fdLogComp.top = new FormAttachment( 0, 0 );
    fdLogComp.right = new FormAttachment( 100, 0 );
    fdLogComp.bottom = new FormAttachment( 100, 0 );
    wSettingsComp.setLayoutData( fdLogComp );

    wSettingsComp.layout();
    wSettingsTab.setControl( wSettingsComp );

    // ///////////////////////////////////////////////////////////
    // / END OF LOG TAB
    // ///////////////////////////////////////////////////////////
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
    wWorkflowName.setText( Const.NVL( workflowMeta.getName(), "" ) );
    wNameFilenameSync.setSelection( workflowMeta.isNameSynchronizedWithFilename() );
    wFilename.setText( Const.NVL( workflowMeta.getFilename(), "" ) );
    updateNameFilenameSync( null );
    wDescription.setText( Const.NVL( workflowMeta.getDescription(), "" ) );
    wExtendedDescription.setText( Const.NVL( workflowMeta.getExtendedDescription(), "" ) );
    wVersion.setText( Const.NVL( workflowMeta.getWorkflowVersion(), "" ) );
    wWorkflowStatus.select( workflowMeta.getWorkflowStatus() - 1 );

    if ( workflowMeta.getCreatedUser() != null ) {
      wCreateUser.setText( workflowMeta.getCreatedUser() );
    }
    if ( workflowMeta.getCreatedDate() != null && workflowMeta.getCreatedDate() != null ) {
      wCreateDate.setText( workflowMeta.getCreatedDate().toString() );
    }

    if ( workflowMeta.getModifiedUser() != null ) {
      wModUser.setText( workflowMeta.getModifiedUser() );
    }
    if ( workflowMeta.getModifiedDate() != null && workflowMeta.getModifiedDate() != null ) {
      wModDate.setText( workflowMeta.getModifiedDate().toString() );
    }

    // The named parameters
    String[] parameters = workflowMeta.listParameters();
    for ( int idx = 0; idx < parameters.length; idx++ ) {
      TableItem item = wParamFields.table.getItem( idx );

      String description;
      try {
        description = workflowMeta.getParameterDescription( parameters[ idx ] );
      } catch ( UnknownParamException e ) {
        description = "";
      }
      String defValue;
      try {
        defValue = workflowMeta.getParameterDefault( parameters[ idx ] );
      } catch ( UnknownParamException e ) {
        defValue = "";
      }

      item.setText( 1, parameters[ idx ] );
      item.setText( 2, Const.NVL( defValue, "" ) );
      item.setText( 3, Const.NVL( description, "" ) );
    }
    wParamFields.setRowNums();
    wParamFields.optWidth( true );

    for ( IWorkflowDialogPlugin extraTab : extraTabs ) {
      extraTab.getData( workflowMeta );
    }
  }

  private void cancel() {
    props.setScreen( new WindowProperty( shell ) );
    workflowMeta = null;
    dispose();
  }

  private void ok() {

    workflowMeta.setName( wWorkflowName.getText() );
    workflowMeta.setDescription( wDescription.getText() );
    workflowMeta.setExtendedDescription( wExtendedDescription.getText() );
    workflowMeta.setWorkflowVersion( wVersion.getText() );
    if ( wWorkflowStatus.getSelectionIndex() != 2 ) {
      // Saving the index as meta data is in fact pretty bad, but since
      // it was already in ...
      workflowMeta.setWorkflowStatus( wWorkflowStatus.getSelectionIndex() + 1 );
    } else {
      workflowMeta.setWorkflowStatus( -1 );
    }

    // Clear and add parameters
    workflowMeta.removeAllParameters();
    int nrNonEmptyFields = wParamFields.nrNonEmpty();
    for ( int i = 0; i < nrNonEmptyFields; i++ ) {
      TableItem item = wParamFields.getNonEmpty( i );

      try {
        workflowMeta.addParameterDefinition( item.getText( 1 ), item.getText( 2 ), item.getText( 3 ) );
      } catch ( DuplicateParamException e ) {
        // Ignore the duplicate parameter.
      }
    }

    for ( IWorkflowDialogPlugin extraTab : extraTabs ) {
      extraTab.ok( workflowMeta );
    }

    workflowMeta.setChanged( changed || workflowMeta.hasChanged() );

    dispose();
  }

  public static final Button setShellImage( Shell shell, IAction action ) {
    Button helpButton = null;
    try {
      final IPlugin plugin = getPlugin( action );

      // Check if action is deprecated by annotation
      Deprecated deprecated = action.getClass().getDeclaredAnnotation(Deprecated.class);
      if ( deprecated!=null ) {
        addDeprecation( shell );
      }
      
      helpButton = HelpUtils.createHelpButton( shell, HelpUtils.getHelpDialogTitle( plugin ), plugin );

      shell.setImage( getImage( shell, plugin ) );

    } catch ( Throwable e ) {
      // Ignore unexpected errors, not worth it
    }
    return helpButton;
  }

  private static void addDeprecation( Shell shell ) {

    if ( shell == null ) {

      return;
    }
    shell.addShellListener( new ShellAdapter() {

      private boolean deprecation = false;

      @Override public void shellActivated( ShellEvent shellEvent ) {
        super.shellActivated( shellEvent );
        if ( deprecation ) {
          return;
        }
        String deprecated = BaseMessages.getString( PKGBASE, "System.Deprecated" ).toLowerCase();
        shell.setText( shell.getText() + " (" + deprecated + ")" );
        deprecation = true;
      }
    } );
  }

  public static IPlugin getPlugin( IAction action ) {
    return PluginRegistry.getInstance().getPlugin( ActionPluginType.class, action );
  }

  public static Image getImage( Shell shell, IPlugin plugin ) {
    String id = plugin.getIds()[ 0 ];
    if ( id != null ) {
      return GuiResource.getInstance().getImagesActions().get( id ).getAsBitmapForSize(
        shell.getDisplay(), ConstUi.ICON_SIZE, ConstUi.ICON_SIZE );
    }
    return null;
  }

}
