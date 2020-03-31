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

package org.apache.hop.ui.trans.dialog;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.persist.MetaStoreFactory;
import org.apache.hop.trans.TransExecutionConfiguration;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.config.PipelineRunConfiguration;
import org.apache.hop.ui.core.dialog.ConfigurationDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GUIResource;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TransExecutionConfigurationDialog extends ConfigurationDialog {
  private static Class<?> PKG = TransExecutionConfigurationDialog.class; // for i18n purposes, needed by Translator!!

  public TransExecutionConfigurationDialog( Shell parent, TransExecutionConfiguration configuration,
                                            TransMeta transMeta ) {
    super( parent, configuration, transMeta );
  }

  protected void serverOptionsComposite( Class<?> PKG, String prefix ) {

  }

  protected void optionsSectionControls() {

    wlLogLevel = new Label( gDetails, SWT.NONE );
    props.setLook( wlLogLevel );
    wlLogLevel.setText( BaseMessages.getString( PKG, "TransExecutionConfigurationDialog.LogLevel.Label" ) );
    wlLogLevel.setToolTipText( BaseMessages.getString( PKG, "TransExecutionConfigurationDialog.LogLevel.Tooltip" ) );
    FormData fdlLogLevel = new FormData();
    fdlLogLevel.top = new FormAttachment( 0, 10 );
    fdlLogLevel.left = new FormAttachment( 0, 10 );
    wlLogLevel.setLayoutData( fdlLogLevel );

    wLogLevel = new CCombo( gDetails, SWT.READ_ONLY | SWT.BORDER );
    wLogLevel.setToolTipText( BaseMessages.getString( PKG, "TransExecutionConfigurationDialog.LogLevel.Tooltip" ) );
    props.setLook( wLogLevel );
    FormData fdLogLevel = new FormData();
    fdLogLevel.top = new FormAttachment( wlLogLevel, -2, SWT.TOP );
    fdLogLevel.width = 180;
    fdLogLevel.left = new FormAttachment( wlLogLevel, 6 );
    wLogLevel.setLayoutData( fdLogLevel );
    wLogLevel.setItems( LogLevel.getLogLevelDescriptions() );

    wClearLog = new Button( gDetails, SWT.CHECK );
    wClearLog.setText( BaseMessages.getString( PKG, "TransExecutionConfigurationDialog.ClearLog.Label" ) );
    wClearLog.setToolTipText( BaseMessages.getString( PKG, "TransExecutionConfigurationDialog.ClearLog.Tooltip" ) );
    props.setLook( wClearLog );
    FormData fdClearLog = new FormData();
    fdClearLog.top = new FormAttachment( wLogLevel, 10 );
    fdClearLog.left = new FormAttachment( 0, 10 );
    wClearLog.setLayoutData( fdClearLog );

    wSafeMode = new Button( gDetails, SWT.CHECK );
    wSafeMode.setText( BaseMessages.getString( PKG, "TransExecutionConfigurationDialog.SafeMode.Label" ) );
    wSafeMode.setToolTipText( BaseMessages.getString( PKG, "TransExecutionConfigurationDialog.SafeMode.Tooltip" ) );
    props.setLook( wSafeMode );
    FormData fdSafeMode = new FormData();
    fdSafeMode.top = new FormAttachment( wClearLog, 7 );
    fdSafeMode.left = new FormAttachment( 0, 10 );
    wSafeMode.setLayoutData( fdSafeMode );

    wGatherMetrics = new Button( gDetails, SWT.CHECK );
    wGatherMetrics.setText( BaseMessages.getString( PKG, "TransExecutionConfigurationDialog.GatherMetrics.Label" ) );
    wGatherMetrics.setToolTipText( BaseMessages.getString( PKG, "TransExecutionConfigurationDialog.GatherMetrics.Tooltip" ) );
    props.setLook( wGatherMetrics );
    FormData fdGatherMetrics = new FormData();
    fdGatherMetrics.top = new FormAttachment( wSafeMode, 7 );
    fdGatherMetrics.left = new FormAttachment( 0, 10 );
    fdGatherMetrics.bottom = new FormAttachment( 100, -10 );
    wGatherMetrics.setLayoutData( fdGatherMetrics );
  }

  public boolean open() {
    String shellTitle = BaseMessages.getString( PKG, "TransExecutionConfigurationDialog.Shell.Title" );
    mainLayout( shellTitle, GUIResource.getInstance().getImageTransGraph() );

    String alwaysShowOptionLabel = BaseMessages.getString( PKG, "TransExecutionConfigurationDialog.AlwaysOption.Value" );
    String alwaysShowOptionTooltip = BaseMessages.getString( PKG, "TransExecutionConfigurationDialog.alwaysShowOption" );
    String docUrl = Const.getDocUrl( BaseMessages.getString( HopGui.class, "HopGui.TransExecutionConfigurationDialog.Help" ) );
    String docTitle = BaseMessages.getString( PKG, "TransExecutionConfigurationDialog.docTitle" );
    String docHeader = BaseMessages.getString( PKG, "TransExecutionConfigurationDialog.docHeader" );
    buttonsSectionLayout( alwaysShowOptionLabel, alwaysShowOptionTooltip, docTitle, docUrl, docHeader );

    addRunConfigurationSectionLayout();

    optionsSectionLayout( PKG, "TransExecutionConfigurationDialog" );
    parametersSectionLayout( PKG, "TransExecutionConfigurationDialog" );


    getData();
    openDialog();
    return retval;
  }

  private void addRunConfigurationSectionLayout() {
    String runConfigLabel = BaseMessages.getString( PKG, "TransExecutionConfigurationDialog.PipelineRunConfiguration.Label" );
    String runConfigTooltip = BaseMessages.getString( PKG, "TransExecutionConfigurationDialog.PipelineRunConfiguration.Tooltip" );

    wRunConfiguration = new MetaSelectionLine<>( hopGui.getVariableSpace(), hopGui.getMetaStore(), PipelineRunConfiguration.class,
      shell, SWT.BORDER, runConfigLabel, runConfigTooltip);
    props.setLook( wRunConfiguration );
    FormData fdRunConfiguration = new FormData();
    fdRunConfiguration.right = new FormAttachment( 100, -15 );
    fdRunConfiguration.top = new FormAttachment( 0, 15 );
    fdRunConfiguration.left = new FormAttachment( 15, 0 );
    wRunConfiguration.setLayoutData( fdRunConfiguration );
  }

  private void getVariablesData() {
    wVariables.clearAll( false );
    List<String> variableNames = new ArrayList<>( configuration.getVariables().keySet() );
    Collections.sort( variableNames );

    for ( int i = 0; i < variableNames.size(); i++ ) {
      String variableName = variableNames.get( i );
      String variableValue = configuration.getVariables().get( variableName );

      if ( Const.indexOfString( variableName, abstractMeta.listParameters() ) < 0 ) {

        TableItem tableItem = new TableItem( wVariables.table, SWT.NONE );
        tableItem.setText( 1, variableName );
        tableItem.setText( 2, Const.NVL( variableValue, "" ) );
      }
    }
    wVariables.removeEmptyRows();
    wVariables.setRowNums();
    wVariables.optWidth( true );
  }

  public void getData() {
    wSafeMode.setSelection( configuration.isSafeModeEnabled() );
    wClearLog.setSelection( configuration.isClearingLog() );
    wGatherMetrics.setSelection( configuration.isGatheringMetrics() );

    try {
      wRunConfiguration.fillItems();
    } catch(Exception e) {
      hopGui.getLog().logError( "Unable to get list of pipeline run configurations from the metastore", e );
    }

    try {
      ExtensionPointHandler.callExtensionPoint( LogChannel.UI, HopExtensionPoint.HopUiRunConfiguration.id, wRunConfiguration );
    } catch ( HopException e ) {
      hopGui.getLog().logError( "Error calling extension point with ID '"+HopExtensionPoint.HopUiRunConfiguration.id+"'", e );
    }

    if (Const.indexOfString( configuration.getRunConfiguration(), wRunConfiguration.getItems())<0) {
      getConfiguration().setRunConfiguration( null );
    }
    if ( StringUtils.isNotEmpty( getConfiguration().getRunConfiguration() ) ) {
      wRunConfiguration.setText( getConfiguration().getRunConfiguration() );
    } else if (wRunConfiguration.getItemCount()==1) {
      wRunConfiguration.select( 0 );
    }

    wLogLevel.select( configuration.getLogLevel().getLevel() );
    getParamsData();
    getVariablesData();
  }

  public void getInfo() {
    try {
      getConfiguration().setRunConfiguration( wRunConfiguration.getText() );

      configuration.setSafeModeEnabled( wSafeMode.getSelection() );
      configuration.setClearingLog( wClearLog.getSelection() );
      configuration.setLogLevel( LogLevel.values()[ wLogLevel.getSelectionIndex() ] );
      configuration.setGatheringMetrics( wGatherMetrics.getSelection() );

      // The lower part of the dialog...
      getInfoParameters();
      getInfoVariables();
    } catch ( Exception e ) {
      new ErrorDialog( shell, "Error in settings", "There is an error in the dialog settings", e );
    }
  }

  /**
   * @return the configuration
   */
  public TransExecutionConfiguration getConfiguration() {
    return (TransExecutionConfiguration) configuration;
  }
}
