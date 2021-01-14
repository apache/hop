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

package org.apache.hop.pipeline;

import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.search.BaseSearchableAnalyser;
import org.apache.hop.core.search.ISearchQuery;
import org.apache.hop.core.search.ISearchResult;
import org.apache.hop.core.search.ISearchable;
import org.apache.hop.core.search.ISearchableAnalyser;
import org.apache.hop.core.search.SearchableAnalyserPlugin;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.ArrayList;
import java.util.List;

@SearchableAnalyserPlugin(
  id = "PipelineMetaSearchAnalyser",
  name = "Search in pipeline metadata"
)
public class PipelineMetaSearchAnalyser extends BaseSearchableAnalyser<PipelineMeta> implements ISearchableAnalyser<PipelineMeta> {

  @Override public Class<PipelineMeta> getSearchableClass() {
    return PipelineMeta.class;
  }

  @Override public List<ISearchResult> search( ISearchable<PipelineMeta> searchable, ISearchQuery searchQuery  ) {
    PipelineMeta pipelineMeta = searchable.getSearchableObject();

    List<ISearchResult> results = new ArrayList<>();

    matchProperty( searchable, results, searchQuery, "pipeline name", pipelineMeta.getName(), null );
    matchProperty( searchable, results, searchQuery, "pipeline description", pipelineMeta.getDescription(), null );

    // The transforms...
    //
    for ( TransformMeta transformMeta : pipelineMeta.getTransforms() ) {
      String transformName = transformMeta.getName();
      matchProperty( searchable, results, searchQuery, "pipeline transform name", transformName, transformName );
      matchProperty( searchable, results, searchQuery, "pipeline transform description", transformMeta.getDescription(), transformName );

      String transformPluginId = transformMeta.getTransformPluginId();
      if (transformPluginId!=null) {
        matchProperty( searchable, results, searchQuery, "pipeline transform plugin ID", transformPluginId, transformName );
        IPlugin transformPlugin = PluginRegistry.getInstance().findPluginWithId( TransformPluginType.class, transformPluginId );
        if (transformPlugin!=null) {
          matchProperty( searchable, results, searchQuery, "pipeline transform plugin name", transformPlugin.getName(), transformName );
        }
      }
      // Search the transform properties
      //
      ITransformMeta transform = transformMeta.getTransform();
      matchObjectFields( searchable, results, searchQuery, transform, "pipeline transform property", transformName );
    }

    // Search the notes...
    //
    for ( NotePadMeta note : pipelineMeta.getNotes() ) {
      matchProperty( searchable, results, searchQuery, "pipeline note", note.getNote(), null );
    }

    return results;
  }
}
