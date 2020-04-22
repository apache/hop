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

package org.apache.hop.ui.pipeline.dialog;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineExecutionConfiguration;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.ui.core.dialog.ConfigurationDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.shared.AuditManagerGuiUtil;
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

public class PipelineExecutionConfigurationDialog extends ConfigurationDialog {
  private static Class<?> PKG = PipelineExecutionConfigurationDialog.class; // for i18n purposes, needed by Translator!!

  public static final String AUDIT_LIST_TYPE_LAST_USED_RUN_CONFIGURATIONS = "last-pipeline-run-configurations";
  private MetaSelectionLine<PipelineRunConfiguration> wRunConfiguration;

  public PipelineExecutionConfigurationDialog( Shell parent, PipelineExecutionConfiguration configuration,
                                               PipelineMeta pipelineMeta ) {
    super( parent, configuration, pipelineMeta );
  }

  protected void serverOptionsComposite( Class<?> PKG, String prefix ) {

  }

  protected void optionsSectionControls() {

    wlLogLevel = new Label( gDetails, SWT.NONE );
    props.setLook( wlLogLevel );
    wlLogLevel.setText( BaseMessages.getString( PKG, "PipelineExecutionConfigurationDialog.LogLevel.Label" ) );
    wlLogLevel.setToolTipText( BaseMessages.getString( PKG, "PipelineExecutionConfigurationDialog.LogLevel.Tooltip" ) );
    FormData fdlLogLevel = new FormData();
    fdlLogLevel.top = new FormAttachment( 0, 10 );
    fdlLogLevel.left = new FormAttachment( 0, 10 );
    wlLogLevel.setLayoutData( fdlLogLevel );

    wLogLevel = new CCombo( gDetails, SWT.READ_ONLY | SWT.BORDER );
    wLogLevel.setToolTipText( BaseMessages.getString( PKG, "PipelineExecutionConfigurationDialog.LogLevel.Tooltip" ) );
    props.setLook( wLogLevel );
    FormData fdLogLevel = new FormData();
    fdLogLevel.top = new FormAttachment( wlLogLevel, -2, SWT.TOP );
    fdLogLevel.width = 350;
    fdLogLevel.left = new FormAttachment( wlLogLevel, 6 );
    wLogLevel.setLayoutData( fdLogLevel );
    wLogLevel.setItems( LogLevel.getLogLevelDescriptions() );

    wClearLog = new Button( gDetails, SWT.CHECK );
    wClearLog.setText( BaseMessages.getString( PKG, "PipelineExecutionConfigurationDialog.ClearLog.Label" ) );
    wClearLog.setToolTipText( BaseMessages.getString( PKG, "PipelineExecutionConfigurationDialog.ClearLog.Tooltip" ) );
    props.setLook( wClearLog );
    FormData fdClearLog = new FormData();
    fdClearLog.top = new FormAttachment( wLogLevel, 10 );
    fdClearLog.left = new FormAttachment( 0, 10 );
    wClearLog.setLayoutData( fdClearLog );

  }

  public boolean open() {
    String shellTitle = BaseMessages.getString( PKG, "PipelineExecutionConfigurationDialog.Shell.Title" );
    mainLayout( shellTitle, GuiResource.getInstance().getImagePipelineGraph() );

    String alwaysShowOptionLabel = BaseMessages.getString( PKG, "PipelineExecutionConfigurationDialog.AlwaysOption.Value" );
    String alwaysShowOptionTooltip = BaseMessages.getString( PKG, "PipelineExecutionConfigurationDialog.alwaysShowOption" );
    String docUrl = Const.getDocUrl( BaseMessages.getString( HopGui.class, "HopGui.PipelineExecutionConfigurationDialog.Help" ) );
    String docTitle = BaseMessages.getString( PKG, "PipelineExecutionConfigurationDialog.docTitle" );
    String docHeader = BaseMessages.getString( PKG, "PipelineExecutionConfigurationDialog.docHeader" );
    buttonsSectionLayout( alwaysShowOptionLabel, alwaysShowOptionTooltip, docTitle, docUrl, docHeader );

    addRunConfigurationSectionLayout();

    optionsSectionLayout( PKG, "PipelineExecutionConfigurationDialog" );
    parametersSectionLayout( PKG, "PipelineExecutionConfigurationDialog" );

    getData();
    openDialog();
    return retval;
  }

  private void addRunConfigurationSectionLayout() {
    String runConfigLabel = BaseMessages.getString( PKG, "PipelineExecutionConfigurationDialog.PipelineRunConfiguration.Label" );
    String runConfigTooltip = BaseMessages.getString( PKG, "PipelineExecutionConfigurationDialog.PipelineRunConfiguration.Tooltip" );

    wRunConfiguration = new MetaSelectionLine<>( hopGui.getVariables(), hopGui.getMetaStore(), PipelineRunConfiguration.class,
      shell, SWT.BORDER, runConfigLabel, runConfigTooltip, true );
    wRunConfigurationControl = wRunConfiguration;
    props.setLook( wRunConfiguration );
    FormData fdRunConfiguration = new FormData();
    fdRunConfiguration.right = new FormAttachment( 100, -10 );
    fdRunConfiguration.top = new FormAttachment( 0, props.getMargin() );
    fdRunConfiguration.left = new FormAttachment( 0, 10 );
    wRunConfiguration.setLayoutData( fdRunConfiguration );
  }

  private void getVariablesData() {
    wVariables.clearAll( false );
    List<String> variableNames = new ArrayList<>( configuration.getVariablesMap().keySet() );
    Collections.sort( variableNames );

    for ( int i = 0; i < variableNames.size(); i++ ) {
      String variableName = variableNames.get( i );
      String variableValue = configuration.getVariablesMap().get( variableName );

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
    wClearLog.setSelection( configuration.isClearingLog() );

    try {
      wRunConfiguration.fillItems();
      if ( Const.indexOfString( configuration.getRunConfiguration(), wRunConfiguration.getItems() ) < 0 ) {
        getConfiguration().setRunConfiguration( null );
      }
    } catch ( Exception e ) {
      hopGui.getLog().logError( "Unable to obtain a list of pipeline run configurations", e );
    }

    wRunConfiguration.setText( AuditManagerGuiUtil.getLastUsedValue( AUDIT_LIST_TYPE_LAST_USED_RUN_CONFIGURATIONS ) );

    try {
      ExtensionPointHandler.callExtensionPoint( LogChannel.UI, HopExtensionPoint.HopUiRunConfiguration.id, wRunConfiguration );
    } catch ( HopException e ) {
      hopGui.getLog().logError( "Error calling extension point with ID '" + HopExtensionPoint.HopUiRunConfiguration.id + "'", e );
    }

    // If we don't have a run configuration from history or from a plugin,
    // set it from last execution or if there's only one, just pick that
    //
    if ( StringUtil.isEmpty( wRunConfiguration.getText() ) ) {
      if ( StringUtils.isNotEmpty( getConfiguration().getRunConfiguration() ) ) {
        wRunConfiguration.setText( getConfiguration().getRunConfiguration() );
      } else if ( wRunConfiguration.getItemCount() == 1 ) {
        wRunConfiguration.select( 0 );
      }
    }

    wLogLevel.select( configuration.getLogLevel().getLevel() );

    getParamsData();
    getVariablesData();
  }

  public void getInfo() {
    try {
      String runConfigurationName = wRunConfiguration.getText();
      getConfiguration().setRunConfiguration( runConfigurationName );
      AuditManagerGuiUtil.addLastUsedValue( AUDIT_LIST_TYPE_LAST_USED_RUN_CONFIGURATIONS, runConfigurationName );
      configuration.setClearingLog( wClearLog.getSelection() );
      configuration.setLogLevel( LogLevel.values()[ wLogLevel.getSelectionIndex() ] );

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
  public PipelineExecutionConfiguration getConfiguration() {
    return (PipelineExecutionConfiguration) configuration;
  }
}
