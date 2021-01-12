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

package org.apache.hop.ui.workflow.config;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.workflow.config.IWorkflowEngineRunConfiguration;
import org.apache.hop.workflow.config.WorkflowRunConfiguration;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engine.WorkflowEnginePluginType;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Text;

@GuiPlugin(
  description = "This dialog allows you to configure the various workflow run configurations"
)
/**
 * The dialog for metadata object WorkflowRunConfiguration
 * Don't move this class around as it's sync'ed with the WorkflowRunConfiguration package to find the dialog.
 */
public class WorkflowRunConfigurationEditor extends MetadataEditor<WorkflowRunConfiguration> {

  private static final Class<?> PKG = WorkflowRunConfigurationEditor.class; // For Translator
  
  private WorkflowRunConfiguration runConfiguration;
  private WorkflowRunConfiguration workingConfiguration;

  private Text wName;
  private Text wDescription;
  private ComboVar wPluginType;

  private Composite wPluginSpecificComp;
  private GuiCompositeWidgets guiCompositeWidgets;

  private Map<String, IWorkflowEngineRunConfiguration> metaMap;

  private Listener modifyListener =  e -> setChanged();
  
  /**
   * @param manager The metadata manager
   * @param runConfiguration The object to edit
   */
  public WorkflowRunConfigurationEditor(HopGui hopGui,  MetadataManager<WorkflowRunConfiguration> manager, WorkflowRunConfiguration runConfiguration ) {
	super(hopGui, manager, runConfiguration);
	  
    this.runConfiguration = runConfiguration;
    this.workingConfiguration = new WorkflowRunConfiguration( runConfiguration );
    
    metaMap = populateMetaMap();
    if ( workingConfiguration.getEngineRunConfiguration() != null ) {
      metaMap.put( workingConfiguration.getEngineRunConfiguration().getEnginePluginName(), workingConfiguration.getEngineRunConfiguration() );
    }
  }

  private Map<String, IWorkflowEngineRunConfiguration> populateMetaMap() {
    metaMap = new HashMap<>();
    List<IPlugin> plugins = PluginRegistry.getInstance().getPlugins( WorkflowEnginePluginType.class );
    for ( IPlugin plugin : plugins ) {
      try {
        IWorkflowEngine<?> engine = PluginRegistry.getInstance().loadClass( plugin, IWorkflowEngine.class );

        // Get the default run configuration for the engine.
        //
        IWorkflowEngineRunConfiguration engineRunConfiguration = engine.createDefaultWorkflowEngineRunConfiguration();
        engineRunConfiguration.setEnginePluginId( plugin.getIds()[ 0 ] );
        engineRunConfiguration.setEnginePluginName( plugin.getName() );

        metaMap.put( engineRunConfiguration.getEnginePluginName(), engineRunConfiguration );
      } catch ( Exception e ) {
        HopGui.getInstance().getLog().logError( "Error instantiating workflow run configuration plugin", e );
      }
    }

    return metaMap;
  }

  @Override
  public void createControl(Composite parent) {
	  PropsUi props = PropsUi.getInstance();
    // Create a tabbed interface instead of the confusing left hand side options
    // This will make it more conforming the rest.
    //
   
    int middle = props.getMiddlePct();
    int margin = props.getMargin();


    // The generic widgets: name, description and workflow engine type
    //
    // What's the name
    //
    Label wlName = new Label( parent, SWT.RIGHT );
    props.setLook( wlName );
    wlName.setText( BaseMessages.getString( PKG, "WorkflowRunConfigurationDialog.label.name" ) );
    FormData fdlName = new FormData();
    fdlName.top = new FormAttachment( 0, margin );
    fdlName.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlName.right = new FormAttachment( middle, 0 );
    wlName.setLayoutData( fdlName );
    wName = new Text( parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wName );
    FormData fdName = new FormData();
    fdName.top = new FormAttachment( wlName, 0, SWT.CENTER );
    fdName.left = new FormAttachment( middle, margin ); // To the right of the label
    fdName.right = new FormAttachment( 100, 0 );
    wName.setLayoutData( fdName );
    Control lastControl = wName;

    Label wlDescription = new Label( parent, SWT.RIGHT );
    props.setLook( wlDescription );
    wlDescription.setText( BaseMessages.getString( PKG, "WorkflowRunConfigurationDialog.label.Description" ) );
    FormData fdlDescription = new FormData();
    fdlDescription.top = new FormAttachment( lastControl, margin );
    fdlDescription.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlDescription.right = new FormAttachment( middle, 0 );
    wlDescription.setLayoutData( fdlDescription );
    wDescription = new Text( parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wDescription );
    FormData fdDescription = new FormData();
    fdDescription.top = new FormAttachment( wlDescription, 0, SWT.CENTER );
    fdDescription.left = new FormAttachment( middle, margin ); // To the right of the label
    fdDescription.right = new FormAttachment( 100, 0 );
    wDescription.setLayoutData( fdDescription );
    lastControl = wDescription;

    // What's the type of engine?
    //
    Label wlPluginType = new Label( parent, SWT.RIGHT );
    props.setLook( wlPluginType );
    wlPluginType.setText( BaseMessages.getString( PKG, "WorkflowRunConfigurationDialog.label.EngineType" ) );
    FormData fdlPluginType = new FormData();
    fdlPluginType.top = new FormAttachment( lastControl, margin );
    fdlPluginType.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlPluginType.right = new FormAttachment( middle, 0 );
    wlPluginType.setLayoutData( fdlPluginType );
    wPluginType = new ComboVar( manager.getVariables(), parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wPluginType );
    wPluginType.setItems( getPluginTypes() );
    FormData fdPluginType = new FormData();
    fdPluginType.top = new FormAttachment( wlPluginType, 0, SWT.CENTER );
    fdPluginType.left = new FormAttachment( middle, margin ); // To the right of the label
    fdPluginType.right = new FormAttachment( 100, 0 );
    wPluginType.setLayoutData( fdPluginType );
    lastControl = wPluginType;


    // Add a composite area
    //
    wPluginSpecificComp = new Composite( parent, SWT.BACKGROUND );
    props.setLook( wPluginSpecificComp );
    wPluginSpecificComp.setLayout( new FormLayout() );
    FormData fdPluginSpecificComp = new FormData();
    fdPluginSpecificComp.left = new FormAttachment( 0, 0 );
    fdPluginSpecificComp.right = new FormAttachment( 100, 0 );
    fdPluginSpecificComp.top = new FormAttachment( lastControl, 3 * margin );
    fdPluginSpecificComp.bottom = new FormAttachment( lastControl, (int) ( props.getZoomFactor() * 400 ) );
    wPluginSpecificComp.setLayoutData( fdPluginSpecificComp );

    // Now add the run configuration plugin specific widgets
    //
    guiCompositeWidgets = new GuiCompositeWidgets( manager.getVariables(), 8 ); // max 8 lines

    // Add the plugin specific widgets
    //
    addGuiCompositeWidgets();

    setWidgetsContent();
    
    // Some widget set changed
    resetChanged();
    
    // Add listeners...
    //
    wName.addListener( SWT.Modify, modifyListener );
    wDescription.addListener( SWT.Modify, modifyListener );
    wPluginType.addListener( SWT.Modify, modifyListener );
    wPluginType.addListener( SWT.Modify, e -> changeConnectionType() );
  }



  private void addGuiCompositeWidgets() {

    // Remove existing children
    //
    for ( Control child : wPluginSpecificComp.getChildren() ) {
      child.removeListener( SWT.Modify, modifyListener );
      child.dispose();
    }

    if ( workingConfiguration.getEngineRunConfiguration() != null ) {
      guiCompositeWidgets = new GuiCompositeWidgets( manager.getVariables(), 8 );
      guiCompositeWidgets.createCompositeWidgets( workingConfiguration.getEngineRunConfiguration(), null, wPluginSpecificComp, WorkflowRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID, null );
      for ( Control control : guiCompositeWidgets.getWidgetsMap().values() ) {
        control.addListener( SWT.Modify, modifyListener );
      }
    }
  }

  private AtomicBoolean busyChangingPluginType = new AtomicBoolean( false );

  private void changeConnectionType() {

    if ( busyChangingPluginType.get() ) {
      return;
    }
    busyChangingPluginType.set( true );

    // Capture any information on the widgets
    //
    getWidgetsContent( workingConfiguration );

    // Save the state of this type so we can switch back and forth
    if ( workingConfiguration.getEngineRunConfiguration() != null ) {
      metaMap.put( workingConfiguration.getEngineRunConfiguration().getEnginePluginName(), workingConfiguration.getEngineRunConfiguration() );
    }

    changeWorkingEngineConfiguration( workingConfiguration );

    // Add the plugin widgets
    //
    addGuiCompositeWidgets();

    // Put the data back
    //
    setWidgetsContent();

    //shell.layout( true, true );

    busyChangingPluginType.set( false );
  }


  public void save() throws HopException {
    changeWorkingEngineConfiguration( runConfiguration );
    
    super.save();
  }


  @Override
  public void setWidgetsContent() {

    wName.setText( Const.NVL( workingConfiguration.getName(), "" ) );
    wDescription.setText( Const.NVL( workingConfiguration.getDescription(), "" ) );
    if ( workingConfiguration.getEngineRunConfiguration() != null ) {
      wPluginType.setText( Const.NVL( workingConfiguration.getEngineRunConfiguration().getEnginePluginName(), "" ) );
      guiCompositeWidgets.setWidgetsContents( workingConfiguration.getEngineRunConfiguration(), wPluginSpecificComp,
        WorkflowRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID );
    } else {
      wPluginType.setText( "" );
    }
  }

  @Override
  public void getWidgetsContent( WorkflowRunConfiguration meta ) {

    meta.setName( wName.getText() );
    meta.setDescription( wDescription.getText() );

    // Get the plugin specific information from the widgets on the screen
    //
    if ( meta.getEngineRunConfiguration() != null && guiCompositeWidgets != null && !guiCompositeWidgets.getWidgetsMap().isEmpty() ) {
      guiCompositeWidgets.getWidgetsContents( meta.getEngineRunConfiguration(), WorkflowRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID );
    }
  }

  private void changeWorkingEngineConfiguration( WorkflowRunConfiguration meta ) {
    String pluginName = wPluginType.getText();
    IWorkflowEngineRunConfiguration engineRunConfiguration = metaMap.get( pluginName );
    if ( engineRunConfiguration != null ) {
      // Switch to the right plugin type
      //
      meta.setEngineRunConfiguration( engineRunConfiguration );
    } else {
      meta.setEngineRunConfiguration( null );
    }
  }


  private String[] getPluginTypes() {
    PluginRegistry registry = PluginRegistry.getInstance();
    List<IPlugin> plugins = registry.getPlugins( WorkflowEnginePluginType.class );
    String[] types = new String[ plugins.size() ];
    for ( int i = 0; i < types.length; i++ ) {
      types[ i ] = plugins.get( i ).getName();
    }
    Arrays.sort( types, String.CASE_INSENSITIVE_ORDER );
    return types;
  }

  }
