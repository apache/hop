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

package org.apache.hop.pipeline.transforms.salesforceinsert;

import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.FieldType;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.SourceToTargetMapping;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaNone;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.salesforce.SalesforceConnection;
import org.apache.hop.pipeline.transforms.salesforce.SalesforceConnectionUtils;
import org.apache.hop.pipeline.transforms.salesforce.SalesforceTransformDialog;
import org.apache.hop.pipeline.transforms.salesforce.SalesforceTransformMeta;
import org.apache.hop.ui.core.dialog.EnterMappingDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.LabelTextVar;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transform.ComponentSelectionListener;
import org.apache.hop.ui.pipeline.transform.ITableItemInsertListener;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SalesforceInsertDialog extends SalesforceTransformDialog {

  private static Class<?> PKG = SalesforceInsertMeta.class; // For Translator

  private CTabFolder wTabFolder;
  private FormData fdTabFolder;

  private CTabItem wGeneralTab;

  private Composite wGeneralComp;

  private FormData fdGeneralComp;

  private FormData fdlModule, fdModule;

  private FormData fdlBatchSize, fdBatchSize;

  private FormData fdUserName, fdURL, fdPassword;

  private Label wlModule, wlBatchSize;

  private Map<String, Integer> inputFields;

  private ColumnInfo[] ciReturn;

  private Button wDoMapping;
  private FormData fdDoMapping;

  private Label wlReturn;
  private TableView wReturn;
  private FormData fdlReturn, fdReturn;

  private Button wGetLU;
  private FormData fdGetLU;
  private Listener lsGetLU;

  private SalesforceInsertMeta input;

  private LabelTextVar wUserName, wURL, wPassword;

  private TextVar wBatchSize;

  private ComboVar wModule;

  private Button wTest;

  private FormData fdTest;
  private Listener lsTest;

  private Group wConnectionGroup;
  private FormData fdConnectionGroup;

  private Group wSettingsGroup, wOutFieldsGroup;
  private FormData fdSettingsGroup, fdOutFieldsGroup;

  private Label wlSalesforceIDFieldName;
  private FormData fdlSalesforceIDFieldName;

  private TextVar wSalesforceIDFieldName;
  private FormData fdSalesforceIDFieldName;

  private Label wlUseCompression;
  private FormData fdlUseCompression;
  private Button wUseCompression;
  private FormData fdUseCompression;

  private Label wlTimeOut;
  private FormData fdlTimeOut;
  private TextVar wTimeOut;
  private FormData fdTimeOut;

  /**
   * List of ColumnInfo that should have the field names of the selected database table
   */
  private List<ColumnInfo> tableFieldColumns = new ArrayList<>();
  private boolean gotModule = false;
  private boolean gotFields = false;

  private Label wlRollbackAllChangesOnError;
  private FormData fdlRollbackAllChangesOnError;
  private Button wRollbackAllChangesOnError;
  private FormData fdRollbackAllChangesOnError;

  private boolean getModulesListError = false; /* True if error getting modules list */

  public SalesforceInsertDialog( Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, variables, in, pipelineMeta, sname );
    input = (SalesforceInsertMeta) in;
    inputFields = new HashMap<>();
  }

  @Override
  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    ModifyListener lsMod = new ModifyListener() {
      @Override
      public void modifyText( ModifyEvent e ) {
        input.setChanged();
      }
    };
    ModifyListener lsTableMod = new ModifyListener() {
      @Override
      public void modifyText( ModifyEvent arg0 ) {
        input.setChanged();
        setModuleFieldCombo();
      }
    };
    SelectionAdapter lsSelection = new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
        setModuleFieldCombo();
      }
    };
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "SalesforceInsertDialog.DialogTitle" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // TransformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "System.Label.TransformName" ) );
    props.setLook( wlTransformName );
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment( 0, 0 );
    fdlTransformName.top = new FormAttachment( 0, margin );
    fdlTransformName.right = new FormAttachment( middle, -margin );
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

    wTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );

    // ////////////////////////
    // START OF FILE TAB ///
    // ////////////////////////
    wGeneralTab = new CTabItem( wTabFolder, SWT.NONE );
    wGeneralTab.setText( BaseMessages.getString( PKG, "SalesforceInsertDialog.General.Tab" ) );

    wGeneralComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wGeneralComp );

    FormLayout generalLayout = new FormLayout();
    generalLayout.marginWidth = 3;
    generalLayout.marginHeight = 3;
    wGeneralComp.setLayout( generalLayout );

    // ///////////////////////////////
    // START OF Connection GROUP //
    // ///////////////////////////////

    wConnectionGroup = new Group( wGeneralComp, SWT.SHADOW_NONE );
    props.setLook( wConnectionGroup );
    wConnectionGroup.setText( BaseMessages.getString( PKG, "SalesforceInsertDialog.ConnectionGroup.Label" ) );

    FormLayout connectionGroupLayout = new FormLayout();
    connectionGroupLayout.marginWidth = 10;
    connectionGroupLayout.marginHeight = 10;
    wConnectionGroup.setLayout( connectionGroupLayout );

    // Webservice URL
    wURL = new LabelTextVar( variables, wConnectionGroup,
      BaseMessages.getString( PKG, "SalesforceInsertDialog.URL.Label" ),
      BaseMessages.getString( PKG, "SalesforceInsertDialog.URL.Tooltip" ) );
    props.setLook( wURL );
    wURL.addModifyListener( lsMod );
    fdURL = new FormData();
    fdURL.left = new FormAttachment( 0, 0 );
    fdURL.top = new FormAttachment( wTransformName, margin );
    fdURL.right = new FormAttachment( 100, 0 );
    wURL.setLayoutData( fdURL );

    // UserName line
    wUserName = new LabelTextVar( variables, wConnectionGroup,
      BaseMessages.getString( PKG, "SalesforceInsertDialog.User.Label" ),
      BaseMessages.getString( PKG, "SalesforceInsertDialog.User.Tooltip" ) );
    props.setLook( wUserName );
    wUserName.addModifyListener( lsMod );
    fdUserName = new FormData();
    fdUserName.left = new FormAttachment( 0, 0 );
    fdUserName.top = new FormAttachment( wURL, margin );
    fdUserName.right = new FormAttachment( 100, 0 );
    wUserName.setLayoutData( fdUserName );

    // Password line
    wPassword = new LabelTextVar( variables, wConnectionGroup,
      BaseMessages.getString( PKG, "SalesforceInsertDialog.Password.Label" ),
      BaseMessages.getString( PKG, "SalesforceInsertDialog.Password.Tooltip" ), true );
    props.setLook( wPassword );
    wPassword.addModifyListener( lsMod );
    fdPassword = new FormData();
    fdPassword.left = new FormAttachment( 0, 0 );
    fdPassword.top = new FormAttachment( wUserName, margin );
    fdPassword.right = new FormAttachment( 100, 0 );
    wPassword.setLayoutData( fdPassword );

    // Test Salesforce connection button
    wTest = new Button( wConnectionGroup, SWT.PUSH );
    wTest.setText( BaseMessages.getString( PKG, "SalesforceInsertDialog.TestConnection.Label" ) );
    props.setLook( wTest );
    fdTest = new FormData();
    wTest.setToolTipText( BaseMessages.getString( PKG, "SalesforceInsertDialog.TestConnection.Tooltip" ) );
    // fdTest.left = new FormAttachment(middle, 0);
    fdTest.top = new FormAttachment( wPassword, margin );
    fdTest.right = new FormAttachment( 100, 0 );
    wTest.setLayoutData( fdTest );

    fdConnectionGroup = new FormData();
    fdConnectionGroup.left = new FormAttachment( 0, margin );
    fdConnectionGroup.top = new FormAttachment( wTransformName, margin );
    fdConnectionGroup.right = new FormAttachment( 100, -margin );
    wConnectionGroup.setLayoutData( fdConnectionGroup );

    // ///////////////////////////////
    // END OF Connection GROUP //
    // ///////////////////////////////

    // ///////////////////////////////
    // START OF Settings GROUP //
    // ///////////////////////////////

    wSettingsGroup = new Group( wGeneralComp, SWT.SHADOW_NONE );
    props.setLook( wSettingsGroup );
    wSettingsGroup.setText( BaseMessages.getString( PKG, "SalesforceInsertDialog.SettingsGroup.Label" ) );

    FormLayout settingGroupLayout = new FormLayout();
    settingGroupLayout.marginWidth = 10;
    settingGroupLayout.marginHeight = 10;
    wSettingsGroup.setLayout( settingGroupLayout );

    // Timeout
    wlTimeOut = new Label( wSettingsGroup, SWT.RIGHT );
    wlTimeOut.setText( BaseMessages.getString( PKG, "SalesforceInsertDialog.TimeOut.Label" ) );
    props.setLook( wlTimeOut );
    fdlTimeOut = new FormData();
    fdlTimeOut.left = new FormAttachment( 0, 0 );
    fdlTimeOut.top = new FormAttachment( wSettingsGroup, margin );
    fdlTimeOut.right = new FormAttachment( middle, -margin );
    wlTimeOut.setLayoutData( fdlTimeOut );
    wTimeOut = new TextVar( variables, wSettingsGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTimeOut );
    wTimeOut.addModifyListener( lsMod );
    fdTimeOut = new FormData();
    fdTimeOut.left = new FormAttachment( middle, 0 );
    fdTimeOut.top = new FormAttachment( wSettingsGroup, margin );
    fdTimeOut.right = new FormAttachment( 100, 0 );
    wTimeOut.setLayoutData( fdTimeOut );

    // Use compression?
    wlUseCompression = new Label( wSettingsGroup, SWT.RIGHT );
    wlUseCompression.setText(
      BaseMessages.getString( PKG, "SalesforceInsertDialog.UseCompression.Label" ) );
    props.setLook( wlUseCompression );
    fdlUseCompression = new FormData();
    fdlUseCompression.left = new FormAttachment( 0, 0 );
    fdlUseCompression.top = new FormAttachment( wTimeOut, margin );
    fdlUseCompression.right = new FormAttachment( middle, -margin );
    wlUseCompression.setLayoutData( fdlUseCompression );
    wUseCompression = new Button( wSettingsGroup, SWT.CHECK );
    props.setLook( wUseCompression );
    wUseCompression.setToolTipText(
      BaseMessages.getString( PKG, "SalesforceInsertDialog.UseCompression.Tooltip" ) );
    fdUseCompression = new FormData();
    fdUseCompression.left = new FormAttachment( middle, 0 );
    fdUseCompression.top = new FormAttachment( wlUseCompression, 0, SWT.CENTER );
    wUseCompression.setLayoutData( fdUseCompression );
    wUseCompression.addSelectionListener( new ComponentSelectionListener( input ) );

    // Rollback all changes on error?
    wlRollbackAllChangesOnError = new Label( wSettingsGroup, SWT.RIGHT );
    wlRollbackAllChangesOnError.setText(
      BaseMessages.getString( PKG, "SalesforceInsertDialog.RollbackAllChangesOnError.Label" ) );
    props.setLook( wlRollbackAllChangesOnError );
    fdlRollbackAllChangesOnError = new FormData();
    fdlRollbackAllChangesOnError.left = new FormAttachment( 0, 0 );
    fdlRollbackAllChangesOnError.top = new FormAttachment( wUseCompression, margin );
    fdlRollbackAllChangesOnError.right = new FormAttachment( middle, -margin );
    wlRollbackAllChangesOnError.setLayoutData( fdlRollbackAllChangesOnError );
    wRollbackAllChangesOnError = new Button( wSettingsGroup, SWT.CHECK );
    wRollbackAllChangesOnError.addSelectionListener( new ComponentSelectionListener( input ) );
    props.setLook( wRollbackAllChangesOnError );
    wRollbackAllChangesOnError.setToolTipText(
      BaseMessages.getString( PKG, "SalesforceInsertDialog.RollbackAllChangesOnError.Tooltip" ) );
    fdRollbackAllChangesOnError = new FormData();
    fdRollbackAllChangesOnError.left = new FormAttachment( middle, 0 );
    fdRollbackAllChangesOnError.top = new FormAttachment( wlRollbackAllChangesOnError, 0, SWT.CENTER );
    wRollbackAllChangesOnError.setLayoutData( fdRollbackAllChangesOnError );

    // BatchSize value
    wlBatchSize = new Label( wSettingsGroup, SWT.RIGHT );
    wlBatchSize.setText( BaseMessages.getString( PKG, "SalesforceInsertDialog.Limit.Label" ) );
    props.setLook( wlBatchSize );
    fdlBatchSize = new FormData();
    fdlBatchSize.left = new FormAttachment( 0, 0 );
    fdlBatchSize.top = new FormAttachment( wRollbackAllChangesOnError, margin );
    fdlBatchSize.right = new FormAttachment( middle, -margin );
    wlBatchSize.setLayoutData( fdlBatchSize );
    wBatchSize = new TextVar( variables, wSettingsGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wBatchSize );
    wBatchSize.addModifyListener( lsMod );
    fdBatchSize = new FormData();
    fdBatchSize.left = new FormAttachment( middle, 0 );
    fdBatchSize.top = new FormAttachment( wRollbackAllChangesOnError, margin );
    fdBatchSize.right = new FormAttachment( 100, 0 );
    wBatchSize.setLayoutData( fdBatchSize );

    // Module
    wlModule = new Label( wSettingsGroup, SWT.RIGHT );
    wlModule.setText( BaseMessages.getString( PKG, "SalesforceInsertDialog.Module.Label" ) );
    props.setLook( wlModule );
    fdlModule = new FormData();
    fdlModule.left = new FormAttachment( 0, 0 );
    fdlModule.top = new FormAttachment( wBatchSize, margin );
    fdlModule.right = new FormAttachment( middle, -margin );
    wlModule.setLayoutData( fdlModule );
    wModule = new ComboVar( variables, wSettingsGroup, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wModule.setEditable( true );
    props.setLook( wModule );
    wModule.addModifyListener( lsTableMod );
    wModule.addSelectionListener( lsSelection );
    fdModule = new FormData();
    fdModule.left = new FormAttachment( middle, 0 );
    fdModule.top = new FormAttachment( wBatchSize, margin );
    fdModule.right = new FormAttachment( 100, -margin );
    wModule.setLayoutData( fdModule );
    wModule.addFocusListener( new FocusListener() {
      @Override
      public void focusLost( org.eclipse.swt.events.FocusEvent e ) {
        getModulesListError = false;
      }

      @Override
      public void focusGained( org.eclipse.swt.events.FocusEvent e ) {
        // check if the URL and login credentials passed and not just had error
        if ( Utils.isEmpty( wURL.getText() )
          || Utils.isEmpty( wUserName.getText() ) || Utils.isEmpty( wPassword.getText() )
          || ( getModulesListError ) ) {
          return;
        }

        Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
        shell.setCursor( busy );
        getModulesList();
        shell.setCursor( null );
        busy.dispose();
      }
    } );

    fdSettingsGroup = new FormData();
    fdSettingsGroup.left = new FormAttachment( 0, margin );
    fdSettingsGroup.top = new FormAttachment( wConnectionGroup, margin );
    fdSettingsGroup.right = new FormAttachment( 100, -margin );
    wSettingsGroup.setLayoutData( fdSettingsGroup );

    // ///////////////////////////////
    // END OF Settings GROUP //
    // ///////////////////////////////

    // ///////////////////////////////
    // START OF OutFields GROUP //
    // ///////////////////////////////

    wOutFieldsGroup = new Group( wGeneralComp, SWT.SHADOW_NONE );
    props.setLook( wOutFieldsGroup );
    wOutFieldsGroup.setText( BaseMessages.getString( PKG, "SalesforceInsertDialog.OutFieldsGroup.Label" ) );

    FormLayout OutFieldsGroupLayout = new FormLayout();
    OutFieldsGroupLayout.marginWidth = 10;
    OutFieldsGroupLayout.marginHeight = 10;
    wOutFieldsGroup.setLayout( OutFieldsGroupLayout );

    // SalesforceIDFieldName
    wlSalesforceIDFieldName = new Label( wOutFieldsGroup, SWT.RIGHT );
    wlSalesforceIDFieldName.setText( BaseMessages.getString(
      PKG, "SalesforceInsertDialog.SalesforceIDFieldName.Label" ) );
    props.setLook( wlSalesforceIDFieldName );
    fdlSalesforceIDFieldName = new FormData();
    fdlSalesforceIDFieldName.left = new FormAttachment( 0, 0 );
    fdlSalesforceIDFieldName.top = new FormAttachment( wSettingsGroup, margin );
    fdlSalesforceIDFieldName.right = new FormAttachment( middle, -margin );
    wlSalesforceIDFieldName.setLayoutData( fdlSalesforceIDFieldName );
    wSalesforceIDFieldName = new TextVar( variables, wOutFieldsGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSalesforceIDFieldName );
    wSalesforceIDFieldName.setToolTipText( BaseMessages.getString(
      PKG, "SalesforceInsertDialog.SalesforceIDFieldName.Tooltip" ) );
    wSalesforceIDFieldName.addModifyListener( lsMod );
    fdSalesforceIDFieldName = new FormData();
    fdSalesforceIDFieldName.left = new FormAttachment( middle, 0 );
    fdSalesforceIDFieldName.top = new FormAttachment( wSettingsGroup, margin );
    fdSalesforceIDFieldName.right = new FormAttachment( 100, 0 );
    wSalesforceIDFieldName.setLayoutData( fdSalesforceIDFieldName );

    fdOutFieldsGroup = new FormData();
    fdOutFieldsGroup.left = new FormAttachment( 0, margin );
    fdOutFieldsGroup.top = new FormAttachment( wSettingsGroup, margin );
    fdOutFieldsGroup.right = new FormAttachment( 100, -margin );
    wOutFieldsGroup.setLayoutData( fdOutFieldsGroup );

    // ///////////////////////////////
    // END OF OutFields GROUP //
    // ///////////////////////////////

    // THE UPDATE/INSERT TABLE
    wlReturn = new Label( wGeneralComp, SWT.NONE );
    wlReturn.setText( BaseMessages.getString( PKG, "SalesforceInsertDialog.UpdateFields.Label" ) );
    props.setLook( wlReturn );
    fdlReturn = new FormData();
    fdlReturn.left = new FormAttachment( 0, 0 );
    fdlReturn.top = new FormAttachment( wOutFieldsGroup, margin );
    wlReturn.setLayoutData( fdlReturn );

    int UpInsCols = 3;
    int UpInsRows = ( input.getUpdateLookup() != null ? input.getUpdateLookup().length : 1 );

    ciReturn = new ColumnInfo[UpInsCols];
    ciReturn[0] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "SalesforceInsertDialog.ColumnInfo.TableField" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false );
    ciReturn[1] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "SalesforceInsertDialog.ColumnInfo.StreamField" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false );
    ciReturn[2] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "SalesforceInsertDialog.ColumnInfo.UseExternalId" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "Y", "N" } );
    ciReturn[2].setToolTip( BaseMessages
      .getString( PKG, "SalesforceInsertDialog.ColumnInfo.UseExternalId.Tooltip" ) );
    tableFieldColumns.add( ciReturn[0] );
    wReturn = new TableView( variables, wGeneralComp, SWT.BORDER
        | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL, ciReturn, UpInsRows, lsMod, props );

    wDoMapping = new Button( wGeneralComp, SWT.PUSH );
    wDoMapping.setText( BaseMessages.getString( PKG, "SalesforceInsertDialog.EditMapping.Label" ) );
    fdDoMapping = new FormData();
    fdDoMapping.top = new FormAttachment( wlReturn, margin );
    fdDoMapping.right = new FormAttachment( 100, 0 );
    wDoMapping.setLayoutData( fdDoMapping );
    wDoMapping.addListener( SWT.Selection, arg0 -> generateMappings() );

    fdReturn = new FormData();
    fdReturn.left = new FormAttachment( 0, 0 );
    fdReturn.top = new FormAttachment( wlReturn, margin );
    fdReturn.right = new FormAttachment( wDoMapping, -margin );
    fdReturn.bottom = new FormAttachment( 100, -2 * margin );
    wReturn.setLayoutData( fdReturn );

    wGetLU = new Button( wGeneralComp, SWT.PUSH );
    wGetLU.setText( BaseMessages.getString( PKG, "SalesforceInsertDialog.GetAndUpdateFields.Label" ) );
    fdGetLU = new FormData();
    fdGetLU.top = new FormAttachment( wDoMapping, margin );
    fdGetLU.left = new FormAttachment( wReturn, margin );
    fdGetLU.right = new FormAttachment( 100, 0 );
    wGetLU.setLayoutData( fdGetLU );

    //
    // Search the fields in the background
    //

    final Runnable runnable = new Runnable() {
      @Override
      public void run() {
        TransformMeta transformMeta = pipelineMeta.findTransform( transformName );
        if ( transformMeta != null ) {
          try {
            IRowMeta row = pipelineMeta.getPrevTransformFields( variables, transformMeta );

            // Remember these fields...
            for ( int i = 0; i < row.size(); i++ ) {
              inputFields.put( row.getValueMeta( i ).getName(), Integer.valueOf( i ) );
            }

            setComboBoxes();
            // Dislay in red missing field names
            Display.getDefault().asyncExec( new Runnable() {
              @Override
              public void run() {
                if ( !wReturn.isDisposed() ) {
                  for ( int i = 0; i < wReturn.table.getItemCount(); i++ ) {
                    TableItem it = wReturn.table.getItem( i );
                    if ( !Utils.isEmpty( it.getText( 2 ) ) ) {
                      if ( !inputFields.containsKey( it.getText( 2 ) ) ) {
                        it.setBackground( GuiResource.getInstance().getColorRed() );
                      }
                    }
                  }
                }
              }
            } );
          } catch ( HopException e ) {
            logError( BaseMessages.getString( PKG, "System.Dialog.GetFieldsFailed.Message" ) );
          }
        }
      }
    };
    new Thread( runnable ).start();

    fdGeneralComp = new FormData();
    fdGeneralComp.left = new FormAttachment( 0, 0 );
    fdGeneralComp.top = new FormAttachment( wTransformName, margin );
    fdGeneralComp.right = new FormAttachment( 100, 0 );
    fdGeneralComp.bottom = new FormAttachment( 100, 0 );
    wGeneralComp.setLayoutData( fdGeneralComp );

    wGeneralComp.layout();
    wGeneralTab.setControl( wGeneralComp );

    // THE BUTTONS
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOk, wCancel }, margin, null );

    fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( wTransformName, margin );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( wOk, -margin );
    wTabFolder.setLayoutData( fdTabFolder );

    // Add listeners
    lsOk = new Listener() {
      @Override
      public void handleEvent( Event e ) {
        ok();
      }
    };
    lsTest = new Listener() {
      @Override
      public void handleEvent( Event e ) {
        test();
      }
    };

    lsGetLU = new Listener() {
      @Override
      public void handleEvent( Event e ) {
        getUpdate();
      }
    };
    lsCancel = new Listener() {
      @Override
      public void handleEvent( Event e ) {
        cancel();
      }
    };

    wOk.addListener( SWT.Selection, lsOk );
    wGetLU.addListener( SWT.Selection, lsGetLU );
    wTest.addListener( SWT.Selection, lsTest );
    wCancel.addListener( SWT.Selection, lsCancel );

    lsDef = new SelectionAdapter() {
      @Override
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wTransformName.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      @Override
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    wTabFolder.setSelection( 0 );

    // Set the shell size, based upon previous time...
    setSize();
    getData( input );
    input.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformName;
  }

  private void getUpdate() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields( variables, transformName );
      if ( r != null ) {
        ITableItemInsertListener listener = ( tableItem, v ) -> {
          tableItem.setText( 3, "Y" );
          return true;
        };
        BaseTransformDialog.getFieldsFromPrevious( r, wReturn, 1, new int[] { 1, 2 }, new int[] {}, -1, -1, listener );
      }
    } catch ( HopException ke ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "SalesforceInsertDialog.FailedToGetFields.DialogTitle" ),
        BaseMessages.getString( PKG, "SalesforceInsertDialog.FailedToGetFields.DialogMessage" ), ke );
    }
  }

  /**
   * Read the data from the TextFileInputMeta object and show it in this dialog.
   *
   * @param in
   *          The SalesforceInsertMeta object to obtain the data from.
   */
  public void getData( SalesforceInsertMeta in ) {
    wURL.setText( Const.NVL( in.getTargetUrl(), "" ) );
    wUserName.setText( Const.NVL( in.getUsername(), "" ) );
    wPassword.setText( Const.NVL( in.getPassword(), "" ) );
    wBatchSize.setText( in.getBatchSize() );
    wModule.setText( Const.NVL( in.getModule(), "Account" ) );
    wBatchSize.setText( "" + in.getBatchSize() );
    wSalesforceIDFieldName.setText( Const.NVL( in.getSalesforceIDFieldName(), "" ) );
    if ( isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "SalesforceInsertDialog.Log.GettingFieldsInfo" ) );
    }

    if ( input.getUpdateLookup() != null ) {
      for ( int i = 0; i < input.getUpdateLookup().length; i++ ) {
        TableItem item = wReturn.table.getItem( i );
        if ( input.getUpdateLookup()[i] != null ) {
          item.setText( 1, input.getUpdateLookup()[i] );
        }
        if ( input.getUpdateStream()[i] != null ) {
          item.setText( 2, input.getUpdateStream()[i] );
        }
        if ( input.getUseExternalId()[i] == null || input.getUseExternalId()[i].booleanValue() ) {
          item.setText( 3, "Y" );
        } else {
          item.setText( 3, "N" );
        }
      }
    }

    wReturn.removeEmptyRows();
    wReturn.setRowNums();
    wReturn.optWidth( true );

    wTimeOut.setText( Const.NVL( in.getTimeout(), SalesforceConnectionUtils.DEFAULT_TIMEOUT ) );
    wUseCompression.setSelection( in.isCompression() );
    wRollbackAllChangesOnError.setSelection( in.isRollbackAllChangesOnError() );

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    input.setChanged( changed );
    dispose();
  }

  private void ok() {
    try {
      getInfo( input );
    } catch ( HopException e ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "SalesforceInsertDialog.ErrorValidateData.DialogTitle" ),
        BaseMessages.getString( PKG, "SalesforceInsertDialog.ErrorValidateData.DialogMessage" ), e );
    }

    dispose();
  }

  @Override
  protected void getInfo( SalesforceTransformMeta in ) throws HopException {
    SalesforceInsertMeta meta = (SalesforceInsertMeta) in;
    transformName = wTransformName.getText(); // return value

    // copy info to SalesforceInsertMeta class (input)
    meta.setTargetUrl( Const.NVL( wURL.getText(), SalesforceConnectionUtils.TARGET_DEFAULT_URL ) );
    meta.setUsername( wUserName.getText() );
    meta.setPassword( wPassword.getText() );
    meta.setModule( Const.NVL( wModule.getText(), "Account" ) );
    meta.setSalesforceIDFieldName( wSalesforceIDFieldName.getText() );
    meta.setBatchSize( wBatchSize.getText() );

    int nrFields = wReturn.nrNonEmpty();

    meta.allocate( nrFields );

    //CHECKSTYLE:Indentation:OFF
    for ( int i = 0; i < nrFields; i++ ) {
      TableItem item = wReturn.getNonEmpty( i );
      meta.getUpdateLookup()[i] = item.getText( 1 );
      meta.getUpdateStream()[i] = item.getText( 2 );
      meta.getUseExternalId()[i] = Boolean.valueOf( "Y".equals( item.getText( 3 ) ) );
    }
    meta.setCompression( wUseCompression.getSelection() );
    meta.setTimeout( Const.NVL( wTimeOut.getText(), "0" ) );
    meta.setRollbackAllChangesOnError( wRollbackAllChangesOnError.getSelection() );
  }

  // check if module, username is given
  private boolean checkInput() {
    if ( Utils.isEmpty( wModule.getText() ) ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "SalesforceInsertDialog.ModuleMissing.DialogMessage" ) );
      mb.setText( BaseMessages.getString( PKG, "System.Dialog.Error.Title" ) );
      mb.open();
      return false;
    }
    return checkUser();
  }

  // check if module, username is given
  private boolean checkUser() {

    if ( Utils.isEmpty( wUserName.getText() ) ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "SalesforceInsertDialog.UsernameMissing.DialogMessage" ) );
      mb.setText( BaseMessages.getString( PKG, "System.Dialog.Error.Title" ) );
      mb.open();
      return false;
    }

    return true;
  }

  private SalesforceConnection getConnection() throws HopException {
    String url = variables.resolve( wURL.getText() );
    // Define a new Salesforce connection
    SalesforceConnection connection =
      new SalesforceConnection( log, url, variables.resolve( wUserName.getText() ),
        Utils.resolvePassword( variables, wPassword.getText() ) );
    int realTimeOut = Const.toInt( variables.resolve( wTimeOut.getText() ), 0 );
    connection.setTimeOut( realTimeOut );
    // connect to Salesforce
    connection.connect();

    return connection;
  }

  private String[] getFieldNames() throws HopException {
    SalesforceConnection connection = null;
    String selectedModule = variables.resolve( wModule.getText() );
    try {
      // Define a new Salesforce connection
      connection = getConnection();
      // return fieldsname for the module
      return connection.getFields( selectedModule );
    } catch ( Exception e ) {
      throw new HopException( "Error getting fields from module [" + selectedModule + "]!", e );
    } finally {
      if ( connection != null ) {
        try {
          connection.close();
        } catch ( Exception e ) {
          // Ignore close errors
        }
      }
    }
  }

  /**
   * Reads in the fields from the previous transforms and from the ONE next transform and opens an EnterMappingDialog with this
   * information. After the user did the mapping, those information is put into the Select/Rename table.
   */
  private void generateMappings() {

    if ( !checkInput() ) {
      return;
    }

    // Determine the source and target fields...
    //
    IRowMeta sourceFields;
    IRowMeta targetFields = new RowMeta();

    try {
      sourceFields = pipelineMeta.getPrevTransformFields( variables, transformMeta );
    } catch ( HopException e ) {
      new ErrorDialog( shell,
        BaseMessages.getString( PKG, "SalesforceInsertDialog.DoMapping.UnableToFindSourceFields.Title" ),
        BaseMessages.getString( PKG, "SalesforceInsertDialog.DoMapping.UnableToFindSourceFields.Message" ), e );
      return;
    }

    try {
      SalesforceConnection connection = getConnection();
      Field[] fields = connection.getObjectFields( variables.resolve( wModule.getText() ) );
      String[] fieldNames = connection.getFields( fields );

      FieldType dateType = FieldType.date;
      for ( int i = 0; i < fields.length; i++ ) {
        if ( dateType.equals( fields[ i ].getType() ) ) {
          // Mark date columns as TYPE_DATE to strip time part later
          targetFields.addValueMeta( ValueMetaFactory.createValueMeta( fieldNames[i], IValueMeta.TYPE_DATE ) );
        } else {
          targetFields.addValueMeta( new ValueMetaNone( fieldNames[i] ) );
        }
      }
    } catch ( Exception e ) {
      new ErrorDialog( shell,
        BaseMessages.getString( PKG, "SalesforceInsertDialog.DoMapping.UnableToFindTargetFields.Title" ),
        BaseMessages.getString( PKG, "SalesforceInsertDialog.DoMapping.UnableToFindTargetFields.Message" ), e );
      return;
    }

    String[] inputNames = new String[sourceFields.size()];
    for ( int i = 0; i < sourceFields.size(); i++ ) {
      IValueMeta value = sourceFields.getValueMeta( i );
      inputNames[i] = value.getName() + EnterMappingDialog.STRING_ORIGIN_SEPARATOR + value.getOrigin() + ")";
    }

    // Create the existing mapping list...
    //
    List<SourceToTargetMapping> mappings = new ArrayList<>();
    StringBuffer missingSourceFields = new StringBuffer();
    StringBuffer missingTargetFields = new StringBuffer();

    int nrFields = wReturn.nrNonEmpty();
    for ( int i = 0; i < nrFields; i++ ) {
      TableItem item = wReturn.getNonEmpty( i );
      String source = item.getText( 2 );
      String target = item.getText( 1 );

      int sourceIndex = sourceFields.indexOfValue( source );
      if ( sourceIndex < 0 ) {
        missingSourceFields.append( Const.CR + "   " + source + " --> " + target );
      }
      int targetIndex = targetFields.indexOfValue( target );
      if ( targetIndex < 0 ) {
        missingTargetFields.append( Const.CR + "   " + source + " --> " + target );
      }
      if ( sourceIndex < 0 || targetIndex < 0 ) {
        continue;
      }

      SourceToTargetMapping mapping = new SourceToTargetMapping( sourceIndex, targetIndex );
      mappings.add( mapping );
    }

    // show a confirm dialog if some missing field was found
    //
    if ( missingSourceFields.length() > 0 || missingTargetFields.length() > 0 ) {

      String message = "";
      if ( missingSourceFields.length() > 0 ) {
        message +=
          BaseMessages.getString(
            PKG, "SalesforceInsertDialog.DoMapping.SomeSourceFieldsNotFound", missingSourceFields.toString() )
            + Const.CR;
      }
      if ( missingTargetFields.length() > 0 ) {
        message +=
          BaseMessages.getString(
            PKG, "SalesforceInsertDialog.DoMapping.SomeTargetFieldsNotFound", missingSourceFields.toString() )
            + Const.CR;
      }
      message += Const.CR;
      message +=
        BaseMessages.getString( PKG, "SalesforceInsertDialog.DoMapping.SomeFieldsNotFoundContinue" ) + Const.CR;
      MessageDialog.setDefaultImage( GuiResource.getInstance().getImageHop() );
      boolean goOn =
        MessageDialog.openConfirm( shell, BaseMessages.getString(
          PKG, "SalesforceInsertDialog.DoMapping.SomeFieldsNotFoundTitle" ), message );
      if ( !goOn ) {
        return;
      }
    }
    EnterMappingDialog d =
      new EnterMappingDialog( SalesforceInsertDialog.this.shell, sourceFields.getFieldNames(), targetFields
        .getFieldNames(), mappings );
    mappings = d.open();

    // mappings == null if the user pressed cancel
    //
    if ( mappings != null ) {
      // Clear and re-populate!
      //
      wReturn.table.removeAll();
      wReturn.table.setItemCount( mappings.size() );
      for ( int i = 0; i < mappings.size(); i++ ) {
        SourceToTargetMapping mapping = mappings.get( i );
        TableItem item = wReturn.table.getItem( i );
        item.setText( 2, sourceFields.getValueMeta( mapping.getSourcePosition() ).getName() );
        item.setText( 1, targetFields.getValueMeta( mapping.getTargetPosition() ).getName() );
      }
      wReturn.setRowNums();
      wReturn.optWidth( true );
    }
  }

  protected void setComboBoxes() {
    // Something was changed in the row.
    //
    final Map<String, Integer> fields = new HashMap<>();

    // Add the currentMeta fields...
    fields.putAll( inputFields );

    Set<String> keySet = fields.keySet();
    List<String> entries = new ArrayList<>( keySet );

    String[] fieldNames = entries.toArray( new String[entries.size()] );
    Const.sortStrings( fieldNames );
    // return fields
    ciReturn[1].setComboValues( fieldNames );
  }

  public void setModuleFieldCombo() {
    if ( gotFields ) {
      return;
    }
    gotFields = true;
    Display display = shell.getDisplay();
    if ( !( display == null || display.isDisposed() ) ) {
      display.asyncExec( new Runnable() {
        @Override
        public void run() {
          // clear
          for ( int i = 0; i < tableFieldColumns.size(); i++ ) {
            ColumnInfo colInfo = tableFieldColumns.get( i );
            colInfo.setComboValues( new String[] {} );
          }
          if ( wModule.isDisposed() ) {
            return;
          }
          String selectedModule = variables.resolve( wModule.getText() );
          if ( !Utils.isEmpty( selectedModule ) ) {
            try {
              // loop through the objects and find build the list of fields
              String[] fieldsName = getFieldNames();

              if ( fieldsName != null ) {
                for ( int i = 0; i < tableFieldColumns.size(); i++ ) {
                  ColumnInfo colInfo = tableFieldColumns.get( i );
                  colInfo.setComboValues( fieldsName );
                }
              }
            } catch ( Exception e ) {
              for ( int i = 0; i < tableFieldColumns.size(); i++ ) {
                ColumnInfo colInfo = tableFieldColumns.get( i );
                colInfo.setComboValues( new String[] {} );
              }
              // ignore any errors here. drop downs will not be
              // filled, but no problem for the user
            }
          }

        }
      } );
    }
  }

  private void getModulesList() {
    if ( !gotModule ) {
      SalesforceConnection connection = null;

      try {
        String selectedField = wModule.getText();
        wModule.removeAll();

        connection = getConnection();
        // return
        wModule.setItems( connection.getAllAvailableObjects( false ) );

        if ( !Utils.isEmpty( selectedField ) ) {
          wModule.setText( selectedField );
        }

        gotModule = true;
        getModulesListError = false;

      } catch ( Exception e ) {
        new ErrorDialog( shell,
          BaseMessages.getString( PKG, "SalesforceInsertDialog.ErrorRetrieveModules.DialogTitle" ),
          BaseMessages.getString( PKG, "SalesforceInsertDialog.ErrorRetrieveData.ErrorRetrieveModules" ), e );
        getModulesListError = true;
      } finally {
        if ( connection != null ) {
          try {
            connection.close();
          } catch ( Exception e ) { /* Ignore */
          }
        }
      }
    }
  }
}
