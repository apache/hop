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

package org.apache.hop.ui.workflow.dialog;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.logging.DefaultLogLevel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.workflow.WorkflowExecutionConfiguration;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionCopy;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.ui.core.dialog.ConfigurationDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
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

public class WorkflowExecutionConfigurationDialog extends ConfigurationDialog {
  private static Class<?> PKG = WorkflowExecutionConfigurationDialog.class; // for i18n purposes, needed by Translator!!

  private CCombo wStartCopy;

  public WorkflowExecutionConfigurationDialog( Shell parent, WorkflowExecutionConfiguration configuration, WorkflowMeta workflowMeta ) {
    super( parent, configuration, workflowMeta );
  }

  protected void optionsSectionControls() {

    wlLogLevel = new Label( gDetails, SWT.RIGHT );
    wlLogLevel.setText( BaseMessages.getString( PKG, "WorkflowExecutionConfigurationDialog.LogLevel.Label" ) );
    wlLogLevel.setToolTipText( BaseMessages.getString( PKG, "WorkflowExecutionConfigurationDialog.LogLevel.Tooltip" ) );
    props.setLook( wlLogLevel );
    FormData fdlLogLevel = new FormData();
    fdlLogLevel.top = new FormAttachment( 0, 10 );
    fdlLogLevel.left = new FormAttachment( 0, 10 );
    wlLogLevel.setLayoutData( fdlLogLevel );

    wLogLevel = new CCombo( gDetails, SWT.READ_ONLY | SWT.BORDER );
    wLogLevel.setToolTipText( BaseMessages.getString( PKG, "WorkflowExecutionConfigurationDialog.LogLevel.Tooltip" ) );
    props.setLook( wLogLevel );
    FormData fdLogLevel = new FormData();
    fdLogLevel.top = new FormAttachment( wlLogLevel, -2, SWT.TOP );
    fdLogLevel.width = 180;
    fdLogLevel.left = new FormAttachment( wlLogLevel, 6 );
    wLogLevel.setLayoutData( fdLogLevel );
    wLogLevel.setItems( LogLevel.getLogLevelDescriptions() );

    wClearLog = new Button( gDetails, SWT.CHECK );
    wClearLog.setText( BaseMessages.getString( PKG, "WorkflowExecutionConfigurationDialog.ClearLog.Label" ) );
    wClearLog.setToolTipText( BaseMessages.getString( PKG, "WorkflowExecutionConfigurationDialog.ClearLog.Tooltip" ) );
    props.setLook( wClearLog );
    FormData fdClearLog = new FormData();
    fdClearLog.top = new FormAttachment( wLogLevel, 10 );
    fdClearLog.left = new FormAttachment( 0, 10 );
    wClearLog.setLayoutData( fdClearLog );

    Label lblStartJob = new Label( gDetails, SWT.RIGHT );
    lblStartJob.setText( BaseMessages.getString( PKG, "WorkflowExecutionConfigurationDialog.StartCopy.Label" ) );
    lblStartJob.setToolTipText( BaseMessages.getString( PKG, "WorkflowExecutionConfigurationDialog.StartCopy.Tooltip" ) );
    props.setLook( lblStartJob );
    FormData fd_lblStartJob = new FormData();
    fd_lblStartJob.top = new FormAttachment( wlLogLevel, 18 );
    fd_lblStartJob.right = new FormAttachment( wlLogLevel, 0, SWT.RIGHT );
    lblStartJob.setLayoutData( fd_lblStartJob );

    wStartCopy = new CCombo( gDetails, SWT.READ_ONLY | SWT.BORDER );
    wStartCopy.setToolTipText( BaseMessages.getString( PKG, "WorkflowExecutionConfigurationDialog.StartCopy.Tooltip" ) );
    props.setLook( wStartCopy );
    FormData fd_startJobCombo = new FormData();
    fd_startJobCombo.top = new FormAttachment( lblStartJob, -2, SWT.TOP );
    fd_startJobCombo.width = 180;
    fd_startJobCombo.left = new FormAttachment( lblStartJob, 6 );
    wStartCopy.setLayoutData( fd_startJobCombo );

    WorkflowMeta workflowMeta = (WorkflowMeta) super.abstractMeta;

    String[] names = new String[ workflowMeta.getJobCopies().size() ];
    for ( int i = 0; i < names.length; i++ ) {
      ActionCopy copy = workflowMeta.getJobCopies().get( i );
      names[ i ] = getActionCopyName( copy );
    }
    wStartCopy.setItems( names );
  }

  public boolean open() {

    String shellTitle = BaseMessages.getString( PKG, "WorkflowExecutionConfigurationDialog.Shell.Title" );
    mainLayout( shellTitle, GuiResource.getInstance().getImageWorkflowGraph() );

    addRunConfigurationSectionLayout();

    optionsSectionLayout( PKG, "WorkflowExecutionConfigurationDialog" );
    parametersSectionLayout( PKG, "WorkflowExecutionConfigurationDialog" );

    String alwaysShowOptionLabel = BaseMessages.getString( PKG, "WorkflowExecutionConfigurationDialog.AlwaysOption.Value" );
    String alwaysShowOptionTooltip = BaseMessages.getString( PKG, "WorkflowExecutionConfigurationDialog.alwaysShowOption" );
    String docUrl = Const.getDocUrl( BaseMessages.getString( HopGui.class, "HopGui.WorkflowExecutionConfigurationDialog.Help" ) );
    String docTitle = BaseMessages.getString( PKG, "WorkflowExecutionConfigurationDialog.docTitle" );
    String docHeader = BaseMessages.getString( PKG, "WorkflowExecutionConfigurationDialog.docHeader" );
    buttonsSectionLayout(alwaysShowOptionLabel, alwaysShowOptionTooltip, docTitle, docUrl, docHeader );

    getData();
    openDialog();
    return retval;
  }

  private void addRunConfigurationSectionLayout() {
    String runConfigLabel = BaseMessages.getString( PKG, "ConfigurationDialog.RunConfiguration.Label" );
    String runConfigTooltip = BaseMessages.getString( PKG, "ConfigurationDialog.RunConfiguration.Tooltip" );

    wRunConfiguration = new MetaSelectionLine<>( hopGui.getVariables(), hopGui.getMetaStore(), PipelineRunConfiguration.class,
      shell, SWT.BORDER, runConfigLabel, runConfigTooltip);
    props.setLook( wRunConfiguration );
    FormData fdRunConfiguration = new FormData();
    fdRunConfiguration.right = new FormAttachment( 100, 0 );
    fdRunConfiguration.top = new FormAttachment( 0, props.getMargin() );
    fdRunConfiguration.left = new FormAttachment( 0, 0 );
    wRunConfiguration.setLayoutData( fdRunConfiguration );
  }

  private String getActionCopyName( ActionCopy copy ) {
    return copy.getName() + ( copy.getNr() > 0 ? copy.getNr() : "" );
  }

  private void getVariablesData() {
    wVariables.clearAll( false );
    List<String> variableNames = new ArrayList<>( configuration.getVariablesMap().keySet() );
    Collections.sort( variableNames );

    List<String> paramNames = new ArrayList<>( configuration.getParametersMap().keySet() );

    for ( int i = 0; i < variableNames.size(); i++ ) {
      String variableName = variableNames.get( i );
      String variableValue = configuration.getVariablesMap().get( variableName );

      if ( !paramNames.contains( variableName ) ) {
        //
        // Do not put the parameters among the variables.
        //
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
    wLogLevel.select( DefaultLogLevel.getLogLevel().getLevel() );

    List<String> runConfigurations = new ArrayList<>();
    try {
      ExtensionPointHandler
        .callExtensionPoint( HopGui.getInstance().getLog(), HopExtensionPoint.HopUiRunConfiguration.id,
          new Object[] { runConfigurations, WorkflowMeta.XML_TAG } );
    } catch ( HopException e ) {
      // Ignore errors
    }

    wRunConfiguration.setItems( runConfigurations.toArray( new String[ 0 ] ) );
    if ( !runConfigurations.contains( getConfiguration().getRunConfiguration() ) ) {
      getConfiguration().setRunConfiguration( null );
    }
    if ( Utils.isEmpty( getConfiguration().getRunConfiguration() ) ) {
      wRunConfiguration.select( 0 );
    } else {
      wRunConfiguration.setText( getConfiguration().getRunConfiguration() );
    }

    String startCopy = "";
    if ( !Utils.isEmpty( getConfiguration().getStartCopyName() ) ) {
      ActionCopy copy =
        ( (WorkflowMeta) abstractMeta ).findAction( getConfiguration().getStartCopyName(), getConfiguration().getStartCopyNr() );
      if ( copy != null ) {
        startCopy = getActionCopyName( copy );
      }
    }
    wStartCopy.setText( startCopy );

    getParamsData();
    getVariablesData();
  }

  public void getInfo() {
    try {
      getConfiguration().setRunConfiguration( wRunConfiguration.getText() );

      // various settings
      //
      configuration.setClearingLog( wClearLog.getSelection() );
      configuration.setLogLevel( LogLevel.values()[ wLogLevel.getSelectionIndex() ] );

      String startCopyName = null;
      int startCopyNr = 0;
      if ( !Utils.isEmpty( wStartCopy.getText() ) ) {
        if ( wStartCopy.getSelectionIndex() >= 0 ) {
          ActionCopy copy = ( (WorkflowMeta) abstractMeta ).getJobCopies().get( wStartCopy.getSelectionIndex() );
          startCopyName = copy.getName();
          startCopyNr = copy.getNr();
        }
      }
      getConfiguration().setStartCopyName( startCopyName );
      getConfiguration().setStartCopyNr( startCopyNr );

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
  public WorkflowExecutionConfiguration getConfiguration() {
    return (WorkflowExecutionConfiguration) configuration;
  }

}
