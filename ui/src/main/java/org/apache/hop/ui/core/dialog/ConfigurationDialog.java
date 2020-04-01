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

package org.apache.hop.ui.core.dialog;

import org.apache.hop.ExecutionConfiguration;
import org.apache.hop.base.AbstractMeta;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.parameters.UnknownParamException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.ui.core.PropsUI;
import org.apache.hop.ui.core.gui.GUIResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.pipeline.step.BaseStepDialog;
import org.apache.hop.ui.util.HelpUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.StackLayout;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public abstract class ConfigurationDialog extends Dialog {

  protected AbstractMeta abstractMeta;
  protected ExecutionConfiguration configuration;
  protected TableView wVariables;
  protected boolean retval;
  protected Shell shell;
  protected PropsUI props;
  protected Label wlLogLevel;
  protected Group gDetails;
  protected CCombo wLogLevel;
  protected Button wClearLog;
  protected Label wlRemoteHost;
  protected CCombo wRemoteHost;
  protected Button wPassExport;
  protected Composite localOptionsComposite;
  protected Composite serverOptionsComposite;
  protected Label environmentSeparator;
  protected StackLayout stackedLayout;
  protected int margin;
  protected Group gLocal;
  protected Composite composite;
  protected MetaSelectionLine<PipelineRunConfiguration> wRunConfiguration;

  private TableView wParams;
  private Display display;
  private Shell parent;
  private Button wOK;
  private Button wCancel;
  protected FormData fdDetails;
  private FormData fd_tabFolder;
  private CTabFolder tabFolder;
  private Button alwaysShowOption;

  protected HopGui hopGui;

  public ConfigurationDialog( Shell parent, ExecutionConfiguration configuration, AbstractMeta meta ) {
    super( parent );
    this.parent = parent;
    this.configuration = configuration;
    this.abstractMeta = meta;

    this.hopGui = HopGui.getInstance();

    // Fill the parameters, maybe do this in another place?
    Map<String, String> params = configuration.getParams();
    params.clear();
    String[] paramNames = meta.listParameters();
    for ( String name : paramNames ) {
      params.put( name, "" );
    }

    props = PropsUI.getInstance();
    margin = props.getMargin();
  }

  protected void getInfoVariables() {
    Map<String, String> map = new HashMap<>();
    int nrNonEmptyVariables = wVariables.nrNonEmpty();
    for ( int i = 0; i < nrNonEmptyVariables; i++ ) {
      TableItem tableItem = wVariables.getNonEmpty( i );
      String varName = tableItem.getText( 1 );
      String varValue = tableItem.getText( 2 );

      if ( !Utils.isEmpty( varName ) ) {
        map.put( varName, varValue );
      }
    }
    configuration.setVariables( map );
  }

  /**
   * Get the parameters from the dialog.
   */
  protected void getInfoParameters() {
    Map<String, String> map = new HashMap<>();
    int nrNonEmptyVariables = wParams.nrNonEmpty();
    for ( int i = 0; i < nrNonEmptyVariables; i++ ) {
      TableItem tableItem = wParams.getNonEmpty( i );
      String paramName = tableItem.getText( 1 );
      String defaultValue = tableItem.getText( 2 );
      String paramValue = tableItem.getText( 3 );

      if ( Utils.isEmpty( paramValue ) ) {
        paramValue = Const.NVL( defaultValue, "" );
      }

      map.put( paramName, paramValue );
    }
    configuration.setParams( map );
  }

  protected void ok() {
    abstractMeta.setAlwaysShowRunOptions( alwaysShowOption.getSelection() );
    abstractMeta.setShowDialog( alwaysShowOption.getSelection() );
    if ( Const.isOSX() ) {
      // OSX bug workaround.
      wVariables.applyOSXChanges();
      wParams.applyOSXChanges();
    }
    getInfo();
    retval = true;
    dispose();
  }

  private void dispose() {
    props.setScreen( new WindowProperty( shell ) );
    shell.dispose();
  }

  protected void cancel() {
    dispose();
  }

  public abstract void getInfo();

  protected void getParamsData() {
    wParams.clearAll( false );
    ArrayList<String> paramNames = new ArrayList<>( configuration.getParams().keySet() );
    Collections.sort( paramNames );

    for ( int i = 0; i < paramNames.size(); i++ ) {
      String paramName = paramNames.get( i );
      String paramValue = configuration.getParams().get( paramName );
      String defaultValue;
      try {
        defaultValue = abstractMeta.getParameterDefault( paramName );
      } catch ( UnknownParamException e ) {
        defaultValue = "";
      }

      String description;
      try {
        description = abstractMeta.getParameterDescription( paramName );
      } catch ( UnknownParamException e ) {
        description = "";
      }

      TableItem tableItem = new TableItem( wParams.table, SWT.NONE );
      tableItem.setText( 1, paramName );
      tableItem.setText( 2, Const.NVL( defaultValue, "" ) );
      tableItem.setText( 3, Const.NVL( paramValue, "" ) );
      tableItem.setText( 4, Const.NVL( description, "" ) );
    }
    wParams.removeEmptyRows();
    wParams.setRowNums();
    wParams.optWidth( true );
  }

  /**
   * @param configuration the configuration to set
   */
  public void setConfiguration( ExecutionConfiguration configuration ) {
    this.configuration = configuration;
  }

  protected void mainLayout( String shellTitle, Image img ) {
    display = parent.getDisplay();
    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX );
    props.setLook( shell );
    shell.setImage( img );
    shell.setLayout( new FormLayout() );
    shell.setText( shellTitle );
  }

  protected void optionsSectionLayout( Class<?> PKG, String prefix ) {
    gDetails = new Group( shell, SWT.SHADOW_ETCHED_IN );
    gDetails.setText( BaseMessages.getString( PKG, prefix + ".DetailsGroup.Label" ) );
    props.setLook( gDetails );

    // The layout
    gDetails.setLayout( new FormLayout() );
    fdDetails = new FormData();
    fdDetails.top = new FormAttachment( wRunConfiguration, 15 );
    fdDetails.right = new FormAttachment( 100, -15 );
    fdDetails.left = new FormAttachment( 0, 15 );
    gDetails.setBackground( shell.getBackground() ); // the default looks ugly
    gDetails.setLayoutData( fdDetails );

    optionsSectionControls();
  }

  protected void parametersSectionLayout( Class<?> PKG, String prefix ) {

    tabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( tabFolder, Props.WIDGET_STYLE_TAB );
    fd_tabFolder = new FormData();
    fd_tabFolder.right = new FormAttachment( 100, -15 );
    fd_tabFolder.left = new FormAttachment( 0, 15 );
    fd_tabFolder.top = new FormAttachment( gDetails, 15 );
    fd_tabFolder.bottom = new FormAttachment( alwaysShowOption, -15 );
    tabFolder.setLayoutData( fd_tabFolder );

    // Parameters
    CTabItem tbtmParameters = new CTabItem( tabFolder, SWT.NONE );
    tbtmParameters.setText( BaseMessages.getString( PKG, prefix + ".Params.Label" ) );
    Composite parametersComposite = new Composite( tabFolder, SWT.NONE );
    props.setLook( parametersComposite );

    parametersComposite.setLayout( new FormLayout() );
    tbtmParameters.setControl( parametersComposite );

    ColumnInfo[] cParams = {
      new ColumnInfo( BaseMessages.getString( PKG, prefix + ".ParamsColumn.Argument" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false, true, 126 ), // Stepname
      new ColumnInfo( BaseMessages.getString( PKG, prefix + ".ParamsColumn.Default" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false, true, 138 ), // Preview size
      new ColumnInfo( BaseMessages.getString( PKG, prefix + ".ParamsColumn.Value" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false, false, 142 ), // Preview size
      new ColumnInfo( BaseMessages.getString( PKG, prefix + ".ParamsColumn.Description" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false, true, 181 ), // Preview size
    };

    String[] namedParams = abstractMeta.listParameters();
    int nrParams = namedParams.length;
    wParams =
      new TableView( abstractMeta, parametersComposite, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, cParams,
        nrParams, false, null, props, false );
    FormData fdParams = new FormData();
    fdParams.top = new FormAttachment( 0, 10 );
    fdParams.right = new FormAttachment( 100, -10 );
    fdParams.bottom = new FormAttachment( 100, -45 );
    fdParams.left = new FormAttachment( 0, 10 );
    wParams.setLayoutData( fdParams );

    tabFolder.setSelection( 0 );

    // Variables
    CTabItem tbtmVariables = new CTabItem( tabFolder, SWT.NONE );
    tbtmVariables.setText( BaseMessages.getString( PKG, prefix + ".Variables.Label" ) );

    Composite variablesComposite = new Composite( tabFolder, SWT.NONE );
    props.setLook( variablesComposite );
    variablesComposite.setLayout( new FormLayout() );
    tbtmVariables.setControl( variablesComposite );

    ColumnInfo[] cVariables = {
      new ColumnInfo( BaseMessages.getString( PKG, prefix + ".VariablesColumn.Argument" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false, false, 287 ), // Stepname
      new ColumnInfo( BaseMessages.getString( PKG, prefix + ".VariablesColumn.Value" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false, false, 300 ), // Preview size
    };

    int nrVariables = configuration.getVariables() != null ? configuration.getVariables().size() : 0;
    wVariables =
      new TableView( abstractMeta, variablesComposite, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, cVariables,
        nrVariables, false, null, props, false );

    FormData fdVariables = new FormData();
    fdVariables.top = new FormAttachment( 0, 10 );
    fdVariables.right = new FormAttachment( 100, -10 );
    fdVariables.bottom = new FormAttachment( 100, -10 );
    fdVariables.left = new FormAttachment( 0, 10 );

    wVariables.setLayoutData( fdVariables );
  }

  protected void buttonsSectionLayout( String alwaysShowOptionLabel, String alwaysShowOptionTooltip, final String docTitle, final String docUrl,
                                       final String docHeader ) {

    // Bottom buttons and separator

    wCancel = new Button( shell, SWT.PUSH );
    FormData fd_wCancel = new FormData();
    fd_wCancel.bottom = new FormAttachment( 100, -15 );
    wCancel.setLayoutData( fd_wCancel );
    wCancel.setText( BaseMessages.getString( "System.Button.Cancel" ) );
    wCancel.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        cancel();
      }
    } );

    wOK = new Button( shell, SWT.PUSH );
    FormData fd_wOK = new FormData();
    fd_wOK.right = new FormAttachment( wCancel, -5 );
    fd_wOK.bottom = new FormAttachment( 100, -15 );
    wOK.setLayoutData( fd_wOK );
    wOK.setText( BaseMessages.getString( "System.Button.Launch" ) );
    wOK.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        ok();
      }
    } );

    Button btnHelp = new Button( shell, SWT.NONE );
    btnHelp.setImage( GUIResource.getInstance().getImageHelpWeb() );
    btnHelp.setText( BaseMessages.getString( "System.Button.Help" ) );
    btnHelp.setToolTipText( BaseMessages.getString( "System.Tooltip.Help" ) );
    FormData fd_btnHelp = new FormData();
    fd_btnHelp.bottom = new FormAttachment( 100, -15 );
    fd_btnHelp.left = new FormAttachment( 0, 15 );
    btnHelp.setLayoutData( fd_btnHelp );
    btnHelp.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent evt ) {
        HelpUtils.openHelpDialog( parent.getShell(), docTitle, docUrl, docHeader );
      }
    } );

    Label separator = new Label( shell, SWT.SEPARATOR | SWT.HORIZONTAL );
    fd_wCancel.right = new FormAttachment( 100, -15 );
    FormData fd_separator = new FormData();
    fd_separator.right = new FormAttachment( 100, -15 );
    fd_separator.left = new FormAttachment( 0, 15 );
    fd_separator.bottom = new FormAttachment( wOK, -15 );
    separator.setLayoutData( fd_separator );

    alwaysShowOption = new Button( shell, SWT.CHECK );
    props.setLook( alwaysShowOption );
    alwaysShowOption.setSelection( abstractMeta.isAlwaysShowRunOptions() );

    alwaysShowOption.setToolTipText( alwaysShowOptionTooltip );

    FormData fd_alwaysShowOption = new FormData();
    fd_alwaysShowOption.left = new FormAttachment( 0, 15 );
    fd_alwaysShowOption.bottom = new FormAttachment( separator, -15 );
    alwaysShowOption.setLayoutData( fd_alwaysShowOption );
    alwaysShowOption.setText( alwaysShowOptionLabel );
  }

  protected void openDialog() {

    BaseStepDialog.setSize( shell );

    // Set the focus on the OK button
    wOK.setFocus();

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
  }

  protected abstract void optionsSectionControls();
}
