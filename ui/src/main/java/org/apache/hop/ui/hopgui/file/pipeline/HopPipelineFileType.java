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

package org.apache.hop.ui.hopgui.file.pipeline;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.file.IHasFilename;
import org.apache.hop.core.gui.plugin.action.GuiAction;
import org.apache.hop.core.gui.plugin.action.GuiActionType;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.history.AuditManager;
import org.apache.hop.laf.BasePropertyHandler;
import org.apache.hop.pipeline.PipelineMeta;
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
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

@HopFileTypePlugin(
  id = "HopFile-Pipeline-Plugin",
  description = "The pipeline file information for the Hop GUI",
  image="ui/images/pipeline.svg"
)
public class HopPipelineFileType<T extends PipelineMeta> extends HopFileTypeBase<T> implements IHopFileType<T> {

  public static final String PIPELINE_FILE_TYPE_DESCRIPTION = "Pipeline";

  public HopPipelineFileType() {
  }

  @Override public String getName() {
    return PIPELINE_FILE_TYPE_DESCRIPTION;
  }

  @Override public String getDefaultFileExtension() {
  	return ".hpl";
  }
  
  @Override public String[] getFilterExtensions() {
    return new String[] { "*.hpl" };
  }

  @Override public String[] getFilterNames() {
    return new String[] { "Pipelines" };
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
    capabilities.setProperty( IHopFileType.CAPABILITY_PAUSE, "true" );
    capabilities.setProperty( IHopFileType.CAPABILITY_PREVIEW, "true" );
    capabilities.setProperty( IHopFileType.CAPABILITY_DEBUG, "true" );

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

      // See if the same pipeline isn't already open.
      // Other file types we might allow to open more than once but not pipelines for now.
      //
      TabItemHandler tabItemHandlerWithFilename = perspective.findTabItemHandlerWithFilename( filename );
      if (tabItemHandlerWithFilename!=null) {
        // Same file so we can simply switch to it.
        // This will prevent confusion.
        //
        perspective.switchToTab( tabItemHandlerWithFilename );
        return tabItemHandlerWithFilename.getTypeHandler();
      }

      // Load the pipeline
      //
      PipelineMeta pipelineMeta = new PipelineMeta( filename, hopGui.getMetadataProvider(), true, parentVariableSpace );

      // Pass the MetaStore for reference lookups
      //
      pipelineMeta.setMetadataProvider( hopGui.getMetadataProvider() );

      // Show it in the perspective
      //
      IHopFileTypeHandler typeHandler = perspective.addPipeline( hopGui, pipelineMeta, this );

      // Keep track of open...
      //
      AuditManager.registerEvent( HopNamespace.getNamespace(), "file", filename, "open" );
      
      // Inform those that want to know about it that we loaded a pipeline
      //
      ExtensionPointHandler.callExtensionPoint( hopGui.getLog(), parentVariableSpace, HopExtensionPoint.PipelineAfterOpen.id, pipelineMeta );

      return typeHandler;
    } catch ( Exception e ) {
      throw new HopException( "Error opening pipeline file '" + filename + "'", e );
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
      PipelineMeta pipelineMeta = new PipelineMeta();
      pipelineMeta.setName( "New pipeline" );

      // Pass the MetaStore for reference lookups
      //
      pipelineMeta.setMetadataProvider( hopGui.getMetadataProvider() );

      // Show it in the perspective
      //
      return perspective.addPipeline( hopGui, pipelineMeta, this );
    } catch ( Exception e ) {
      throw new HopException( "Error creating new pipeline", e );
    }
  }

  @Override public boolean isHandledBy( String filename, boolean checkContent ) throws HopException {
    try {
      if ( checkContent ) {
        Document document = XmlHandler.loadXmlFile( filename );
        Node pipelineNode = XmlHandler.getSubNode( document, PipelineMeta.XML_TAG );
        return pipelineNode != null;
      } else {
        return super.isHandledBy( filename, checkContent );
      }
    } catch ( Exception e ) {
      throw new HopException( "Unable to verify file handling of file '" + filename + "'", e );
    }
  }

  @Override public boolean supportsFile( IHasFilename metaObject ) {
    return metaObject instanceof PipelineMeta;
  }

  public static final String ACTION_ID_NEW_PIPELINE = "NewPipeline";

  @Override public List<IGuiContextHandler> getContextHandlers() {

    HopGui hopGui = HopGui.getInstance();

    List<IGuiContextHandler> handlers = new ArrayList<>();

    GuiAction newAction = new GuiAction( ACTION_ID_NEW_PIPELINE, GuiActionType.Create, "Pipeline", "Creates a new pipeline. Process your data using a network of transforms running in parallel",
        "ui/images/pipeline.svg",
      ( shiftClicked, controlClicked, parameters ) -> {
        try {
          HopPipelineFileType.this.newFile( hopGui, hopGui.getVariables() );
        } catch ( Exception e ) {
          new ErrorDialog( hopGui.getShell(), "Error", "Error creating new pipeline", e );
        }
      } );
    newAction.setCategory( "File" );
    newAction.setCategoryOrder( "1" );

    handlers.add( new GuiContextHandler( ACTION_ID_NEW_PIPELINE, Arrays.asList(newAction) ) );
    return handlers;
  }
}
