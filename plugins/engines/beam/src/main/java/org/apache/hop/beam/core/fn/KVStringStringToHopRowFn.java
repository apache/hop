package org.apache.hop.beam.core.fn;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.util.JsonRowMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class KVStringStringToHopRowFn extends DoFn<KV<String,String>, HopRow> {

  private String rowMetaJson;
  private String transformName;
  private List<String> stepPluginClasses;
  private List<String> xpPluginClasses;

  private static final Logger LOG = LoggerFactory.getLogger( KVStringStringToHopRowFn.class );
  private final Counter numErrors = Metrics.counter( "main", "BeamSubscribeTransformErrors" );

  private IRowMeta rowMeta;
  private transient Counter initCounter;
  private transient Counter inputCounter;
  private transient Counter writtenCounter;

  public KVStringStringToHopRowFn( String transformName, String rowMetaJson, List<String> stepPluginClasses, List<String> xpPluginClasses ) {
    this.transformName = transformName;
    this.rowMetaJson = rowMetaJson;
    this.stepPluginClasses = stepPluginClasses;
    this.xpPluginClasses = xpPluginClasses;
  }

  @Setup
  public void setUp() {
    try {
      inputCounter = Metrics.counter( Pipeline.METRIC_NAME_INPUT, transformName );
      writtenCounter = Metrics.counter( Pipeline.METRIC_NAME_WRITTEN, transformName );

      // Initialize Kettle Beam
      //
      BeamHop.init( stepPluginClasses, xpPluginClasses );
      rowMeta = JsonRowMeta.fromJson( rowMetaJson );

      Metrics.counter( Pipeline.METRIC_NAME_INIT, transformName ).inc();
    } catch ( Exception e ) {
      numErrors.inc();
      LOG.error( "Error in setup of KV<Long,String> to Kettle Row conversion function", e );
      throw new RuntimeException( "Error in setup of KV<Long,String> to Kettle Row conversion function", e );
    }
  }

  @ProcessElement
  public void processElement( ProcessContext processContext ) {
    try {

      KV<String,String> kv = processContext.element();
      inputCounter.inc();

      Object[] outputRow = RowDataUtil.allocateRowData( rowMeta.size() );
      outputRow[ 0 ] = kv.getKey(); // String
      outputRow[ 1 ] = kv.getValue(); // String

      processContext.output( new HopRow( outputRow ) );
      writtenCounter.inc();

    } catch ( Exception e ) {
      numErrors.inc();
      LOG.error( "Error in KV<Long,String> to Kettle Row conversion function", e );
      throw new RuntimeException( "Error in KV<Long,String> to Kettle Row conversion function", e );
    }
  }
}