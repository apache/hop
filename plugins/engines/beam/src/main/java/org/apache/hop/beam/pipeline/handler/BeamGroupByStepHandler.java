package org.apache.hop.beam.pipeline.handler;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.transform.GroupByTransform;
import org.apache.hop.beam.core.util.JsonRowMeta;
import org.apache.hop.beam.engines.IBeamPipelineEngineRunConfiguration;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.memgroupby.MemoryGroupByMeta;

import java.util.List;
import java.util.Map;

public class BeamGroupByStepHandler extends BeamBaseStepHandler implements BeamStepHandler {

  public BeamGroupByStepHandler( IBeamPipelineEngineRunConfiguration runConfiguration, IMetaStore metaStore, PipelineMeta pipelineMeta, List<String> transformPluginClasses, List<String> xpPluginClasses ) {
    super( runConfiguration, false, false, metaStore, pipelineMeta, transformPluginClasses, xpPluginClasses );
  }

  @Override public void handleStep( ILogChannel log, TransformMeta transformMeta, Map<String, PCollection<HopRow>> stepCollectionMap,
                                    Pipeline pipeline, IRowMeta rowMeta, List<TransformMeta> previousSteps,
                                    PCollection<HopRow> input ) throws HopException {

    MemoryGroupByMeta groupByMeta = (MemoryGroupByMeta) transformMeta.getTransform();

    String[] aggregates = new String[ groupByMeta.getAggregateType().length ];
    for ( int i = 0; i < aggregates.length; i++ ) {
      aggregates[ i ] = MemoryGroupByMeta.getTypeDesc( groupByMeta.getAggregateType()[ i ] );
    }

    PTransform<PCollection<HopRow>, PCollection<HopRow>> stepTransform = new GroupByTransform(
      transformMeta.getName(),
      JsonRowMeta.toJson( rowMeta ),  // The io row
      transformPluginClasses,
      xpPluginClasses,
      groupByMeta.getGroupField(),
      groupByMeta.getSubjectField(),
      aggregates,
      groupByMeta.getAggregateField()
    );

    // Apply the transform transform to the previous io transform PCollection(s)
    //
    PCollection<HopRow> stepPCollection = input.apply( transformMeta.getName(), stepTransform );

    // Save this in the map
    //
    stepCollectionMap.put( transformMeta.getName(), stepPCollection );
    log.logBasic( "Handled Group By (STEP) : " + transformMeta.getName() + ", gets data from " + previousSteps.size() + " previous transform(s)" );
  }
}
