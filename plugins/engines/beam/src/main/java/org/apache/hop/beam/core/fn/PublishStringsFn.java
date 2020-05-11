package org.apache.hop.beam.core.fn;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.util.JsonRowMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class PublishStringsFn extends DoFn<HopRow, String> {

  private String rowMetaJson;
  private int fieldIndex;
  private String transformName;
  private List<String> transformPluginClasses;
  private List<String> xpPluginClasses;

  private static final Logger LOG = LoggerFactory.getLogger( PublishStringsFn.class );
  private final Counter numErrors = Metrics.counter( "main", "BeamPublishTransformErrors" );

  private IRowMeta rowMeta;
  private transient Counter initCounter;
  private transient Counter readCounter;
  private transient Counter outputCounter;

  public PublishStringsFn( String transformName, int fieldIndex, String rowMetaJson, List<String> transformPluginClasses, List<String> xpPluginClasses ) {
    this.transformName = transformName;
    this.fieldIndex = fieldIndex;
    this.rowMetaJson = rowMetaJson;
    this.transformPluginClasses = transformPluginClasses;
    this.xpPluginClasses = xpPluginClasses;
  }

  @Setup
  public void setUp() {
    try {
      readCounter = Metrics.counter( Pipeline.METRIC_NAME_READ, transformName );
      outputCounter = Metrics.counter( Pipeline.METRIC_NAME_OUTPUT, transformName );

      // Initialize Hop Beam
      //
      BeamHop.init( transformPluginClasses, xpPluginClasses );
      rowMeta = JsonRowMeta.fromJson( rowMetaJson );

      Metrics.counter( Pipeline.METRIC_NAME_INIT, transformName ).inc();
    } catch ( Exception e ) {
      numErrors.inc();
      LOG.error( "Error in setup of pub/sub publish messages function", e );
      throw new RuntimeException( "Error in setup of pub/sub publish messages function", e );
    }
  }

  @ProcessElement
  public void processElement( ProcessContext processContext ) {

    try {

      HopRow hopRow = processContext.element();
      readCounter.inc();

      try {
        String string = rowMeta.getString( hopRow.getRow(), fieldIndex );
        processContext.output( string );
        outputCounter.inc();
      } catch ( Exception e ) {
        throw new RuntimeException( "Unable to pass string", e );
      }

    } catch ( Exception e ) {
      numErrors.inc();
      LOG.error( "Error in pub/sub publish messages function", e );
      throw new RuntimeException( "Error in pub/sub publish messages function", e );
    }
  }
}