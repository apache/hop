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

package org.apache.hop.ui.hopgui.file.workflow;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.file.IHasFilename;
import org.apache.hop.core.gui.plugin.action.GuiAction;
import org.apache.hop.core.gui.plugin.action.GuiActionType;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.history.AuditManager;
import org.apache.hop.laf.BasePropertyHandler;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.HopNamespace;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.context.GuiContextHandler;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.file.HopFileTypeBase;
import org.apache.hop.ui.hopgui.file.HopFileTypePlugin;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.perspective.TabItemHandler;
import org.apache.hop.ui.hopgui.perspective.dataorch.HopDataOrchestrationPerspective;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.actions.start.ActionStart;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

@HopFileTypePlugin(
  id = "HopFile-Workflow-Plugin",
  description = "The workflow file information for the Hop GUI",
  image="ui/images/workflow.svg"
)
public class HopWorkflowFileType<T extends WorkflowMeta> extends HopFileTypeBase<T> implements IHopFileType<T> {

  public static final String WORKFLOW_FILE_TYPE_DESCRIPTION = "Workflow";

  public HopWorkflowFileType() {
  }

  @Override public String getName() {
    return WORKFLOW_FILE_TYPE_DESCRIPTION;
  }

  @Override public String getDefaultFileExtension() {
    return ".hwf";
  }

  @Override public String[] getFilterExtensions() {
    return new String[] { "*.hwf" };
  }

  @Override public String[] getFilterNames() {
    return new String[] { "Workflows" };
  }

  public Properties getCapabilities() {
    Properties capabilities = new Properties();
    capabilities.setProperty( IHopFileType.CAPABILITY_NEW, "true" );
    capabilities.setProperty( IHopFileType.CAPABILITY_CLOSE, "true" );
    capabilities.setProperty( IHopFileType.CAPABILITY_START, "true" );
    capabilities.setProperty( IHopFileType.CAPABILITY_STOP, "true" );
    capabilities.setProperty( IHopFileType.CAPABILITY_SAVE, "true" );
    capabilities.setProperty( IHopFileType.CAPABILITY_SAVE_AS, "true" );
    capabilities.setProperty( IHopFileType.CAPABILITY_EXPORT_TO_SVG, "true" );
    capabilities.setProperty( IHopFileType.CAPABILITY_PAUSE, "false" );
    capabilities.setProperty( IHopFileType.CAPABILITY_PREVIEW, "false" );
    capabilities.setProperty( IHopFileType.CAPABILITY_DEBUG, "false" );

    capabilities.setProperty( IHopFileType.CAPABILITY_COPY, "true" );
    capabilities.setProperty( IHopFileType.CAPABILITY_PASTE, "true" );
    capabilities.setProperty( IHopFileType.CAPABILITY_CUT, "true" );
    capabilities.setProperty( IHopFileType.CAPABILITY_DELETE, "true" );

    capabilities.setProperty( IHopFileType.CAPABILITY_FILE_HISTORY, "true" );

    return capabilities;
  }

  @Override public IHopFileTypeHandler openFile( HopGui hopGui, String filename, IVariables parentVariableSpace ) throws HopException {
    try {
      // This file is opened in the data orchestration perspective
      //
      HopDataOrchestrationPerspective perspective = HopGui.getDataOrchestrationPerspective();
      perspective.activate();

      // See if the same workflow isn't already open.
      // Other file types we might allow to open more than once but not workflows for now.
      //
      TabItemHandler tabItemHandlerWithFilename = perspective.findTabItemHandlerWithFilename( filename );
      if ( tabItemHandlerWithFilename != null ) {
        // Same file so we can simply switch to it.
        // This will prevent confusion.
        //
        perspective.switchToTab( tabItemHandlerWithFilename );
        return tabItemHandlerWithFilename.getTypeHandler();
      }

      // Load the workflow from file
      //
      WorkflowMeta workflowMeta = new WorkflowMeta( parentVariableSpace, filename, hopGui.getMetadataProvider() );

      // Pass the MetaStore for reference lookups
      //
      workflowMeta.setMetadataProvider( hopGui.getMetadataProvider() );

      // Keep track of open...
      //
      AuditManager.registerEvent( HopNamespace.getNamespace(), "file", filename, "open" );

      // Inform those that want to know about it that we loaded a pipeline
      //
      ExtensionPointHandler.callExtensionPoint( hopGui.getLog(), parentVariableSpace, "WorkflowAfterOpen", workflowMeta );

      // Show it in the perspective
      //
      return perspective.addWorkflow( hopGui, workflowMeta, this );
    } catch ( Exception e ) {
      throw new HopException( "Error opening workflow file '" + filename + "'", e );
    }
  }

  @Override public IHopFileTypeHandler newFile( HopGui hopGui, IVariables parentVariableSpace ) throws HopException {
    try {
      // This file is created in the data orchestration perspective
      //
      HopDataOrchestrationPerspective perspective = HopGui.getDataOrchestrationPerspective();
      perspective.activate();

      // Create the empty pipeline
      //
      WorkflowMeta workflowMeta = new WorkflowMeta();
      workflowMeta.setName( "New workflow" );

      // Pass the MetaStore for reference lookups
      //
      workflowMeta.setMetadataProvider( hopGui.getMetadataProvider() );

      // Add a Start action by default...
      //
      ActionStart start = new ActionStart("Start");
      ActionMeta startMeta = new ActionMeta(start);
      startMeta.setLocation( 50, 50 );
      workflowMeta.addAction( startMeta );

      // Show it in the perspective
      //
      return perspective.addWorkflow( hopGui, workflowMeta, this );
    } catch ( Exception e ) {
      throw new HopException( "Error creating new workflow", e );
    }
  }

  @Override public boolean isHandledBy( String filename, boolean checkContent ) throws HopException {
    try {
      if ( checkContent ) {
        Document document = XmlHandler.loadXmlFile( filename );
        Node workflowNode = XmlHandler.getSubNode( document, WorkflowMeta.XML_TAG );
        return workflowNode != null;
      } else {
        return super.isHandledBy( filename, checkContent );
      }
    } catch ( Exception e ) {
      throw new HopException( "Unable to verify file handling of file '" + filename + "'", e );
    }
  }

  @Override public boolean supportsFile( IHasFilename metaObject ) {
    return metaObject instanceof WorkflowMeta;
  }

  public static final String ACTION_ID_NEW_WORKFLOW = "NewWorkflow";

  @Override public List<IGuiContextHandler> getContextHandlers() {

    HopGui hopGui = HopGui.getInstance();

    List<IGuiContextHandler> handlers = new ArrayList<>();

    GuiAction newAction = new GuiAction( ACTION_ID_NEW_WORKFLOW, GuiActionType.Create, "Workflow",
      "Creates a workflow: a sequential set of actions where a path is followed based on the outcome of executions and conditions.",
      "ui/images/workflow.svg",
      ( shiftClicked, controlClicked, parameters ) -> {
        try {
          HopWorkflowFileType.this.newFile( hopGui, hopGui.getVariables() );
        } catch ( Exception e ) {
          new ErrorDialog( hopGui.getShell(), "Error", "Error creating new workflow", e );
        }
      } );
    newAction.setCategory( "File" );
    newAction.setCategoryOrder( "1" );

    handlers.add( new GuiContextHandler( ACTION_ID_NEW_WORKFLOW, Arrays.asList( newAction ) ) );

    return handlers;
  }
}
