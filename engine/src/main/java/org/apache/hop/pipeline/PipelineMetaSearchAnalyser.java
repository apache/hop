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

    matchProperty( searchable, results, searchQuery, "pipeline name", pipelineMeta.getName() );
    matchProperty( searchable, results, searchQuery, "pipeline description", pipelineMeta.getDescription() );

    // The transforms...
    //
    for ( TransformMeta transformMeta : pipelineMeta.getTransforms() ) {
      matchProperty( searchable, results, searchQuery, "pipeline transform name", transformMeta.getName() );
      matchProperty( searchable, results, searchQuery, "pipeline transform description", transformMeta.getDescription() );

      String transformPluginId = transformMeta.getTransformPluginId();
      if (transformPluginId!=null) {
        matchProperty( searchable, results, searchQuery, "pipeline transform plugin ID", transformPluginId );
        IPlugin transformPlugin = PluginRegistry.getInstance().findPluginWithId( TransformPluginType.class, transformPluginId );
        if (transformPlugin!=null) {
          matchProperty( searchable, results, searchQuery, "pipeline transform plugin name", transformPlugin.getName() );
        }
      }
      // Search the transform properties
      //
      ITransformMeta transform = transformMeta.getTransform();
      matchObjectFields( searchable, results, searchQuery, transform, "pipeline transform property" );
    }

    // Search the notes...
    //
    for ( NotePadMeta note : pipelineMeta.getNotes() ) {
      matchProperty( searchable, results, searchQuery, "pipeline note", note.getNote() );
    }

    return results;
  }
}
