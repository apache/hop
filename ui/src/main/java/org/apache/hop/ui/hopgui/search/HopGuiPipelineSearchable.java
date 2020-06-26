package org.apache.hop.ui.hopgui.search;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.search.ISearchResult;
import org.apache.hop.core.search.ISearchable;
import org.apache.hop.core.search.ISearchableCallback;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.pipeline.HopPipelineFileType;
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
    return new ISearchableCallback() {
      @Override public void callback( ISearchable searchable, ISearchResult searchResult ) throws HopException {
        HopDataOrchestrationPerspective perspective = HopGui.getDataOrchestrationPerspective();
        perspective.addPipeline( perspective.getComposite(), HopGui.getInstance(), pipelineMeta, perspective.getPipelineFileType() );
      }
    };
  }
}
