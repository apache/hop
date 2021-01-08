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

package org.apache.hop.ui.hopgui.search;

import org.apache.hop.core.search.ISearchable;
import org.apache.hop.core.search.ISearchableCallback;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.hopgui.file.pipeline.HopPipelineFileType;
import org.apache.hop.ui.hopgui.perspective.TabItemHandler;
import org.apache.hop.ui.hopgui.perspective.dataorch.HopDataOrchestrationPerspective;

public class HopGuiPipelineSearchable implements ISearchable<PipelineMeta> {

  private String location;
  private PipelineMeta pipelineMeta;

  public HopGuiPipelineSearchable( String location, PipelineMeta pipelineMeta ) {
    this.location = location;
    this.pipelineMeta = pipelineMeta;
  }

  @Override public String getLocation() {
    return location;
  }

  @Override public String getName() {
    return pipelineMeta.getName();
  }

  @Override public String getType() {
    return HopPipelineFileType.PIPELINE_FILE_TYPE_DESCRIPTION;
  }

  @Override public String getFilename() {
    return pipelineMeta.getFilename();
  }

  @Override public PipelineMeta getSearchableObject() {
    return pipelineMeta;
  }

  @Override public ISearchableCallback getSearchCallback() {
    return ( searchable, searchResult ) -> {
      HopDataOrchestrationPerspective perspective = HopGui.getDataOrchestrationPerspective();
      perspective.activate();

      HopGuiPipelineGraph pipelineGraph;

      // See if the same pipeline isn't already open.
      // Other file types we might allow to open more than once but not pipelines for now.
      //
      TabItemHandler tabItemHandlerWithFilename = perspective.findTabItemHandlerWithFilename( pipelineMeta.getFilename() );
      if (tabItemHandlerWithFilename!=null) {
        // Same file so we can simply switch to it.
        // This will prevent confusion.
        //
        perspective.switchToTab( tabItemHandlerWithFilename );
        pipelineGraph = (HopGuiPipelineGraph) tabItemHandlerWithFilename.getTypeHandler();
      } else {
        pipelineGraph = (HopGuiPipelineGraph) perspective.addPipeline( HopGui.getInstance(), pipelineMeta, perspective.getPipelineFileType() );
      }

      // Optionally select and open the matching transform component
      //
      if (searchResult.getComponent()!=null) {
        TransformMeta transformMeta = pipelineMeta.findTransform( searchResult.getComponent() );
        if (transformMeta!=null) {
          transformMeta.setSelected( true );
          pipelineGraph.editTransform(pipelineMeta, transformMeta);
        }
      }
    };
  }
}
