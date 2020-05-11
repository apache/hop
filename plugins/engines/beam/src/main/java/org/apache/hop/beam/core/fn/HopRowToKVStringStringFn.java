package org.apache.hop.beam.core.fn;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.util.JsonRowMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class HopRowToKVStringStringFn extends DoFn<HopRow, KV<String,String>> {

  private String rowMetaJson;
  private String transformName;
  private int keyIndex;
  private int valueIndex;
  private List<String> transformPluginClasses;
  private List<String> xpPluginClasses;

  private static final Logger LOG = LoggerFactory.getLogger( HopRowToKVStringStringFn.class );
  private final Counter numErrors = Metrics.counter( "main", "BeamSubscribeTransformErrors" );

  private IRowMeta rowMeta;
  private transient Counter initCounter;
  private transient Counter inputCounter;
  private transient Counter writtenCounter;

  public HopRowToKVStringStringFn( String transformName, int keyIndex, int valueIndex, String rowMetaJson, List<String> transformPluginClasses, List<String> xpPluginClasses ) {
    this.transformName = transformName;
    this.keyIndex = keyIndex;
    this.valueIndex = valueIndex;
    this.rowMetaJson = rowMetaJson;
    this.transformPluginClasses = transformPluginClasses;
    this.xpPluginClasses = xpPluginClasses;
  }

  @Setup
  public void setUp() {
    try {
      inputCounter = Metrics.counter( Pipeline.METRIC_NAME_INPUT, transformName );
      writtenCounter = Metrics.counter( Pipeline.METRIC_NAME_WRITTEN, transformName );

      // Initialize Hop Beam
      //
      BeamHop.init( transformPluginClasses, xpPluginClasses );
      rowMeta = JsonRowMeta.fromJson( rowMetaJson );

      Metrics.counter( Pipeline.METRIC_NAME_INIT, transformName ).inc();
    } catch ( Exception e ) {
      numErrors.inc();
      LOG.error( "Error in setup of HopRow to KV<String,String> function", e );
      throw new RuntimeException( "Error in setup of HopRow to KV<String,String> function", e );
    }
  }

  @ProcessElement
  public void processElement( ProcessContext processContext ) {
    try {
      HopRow hopRow = processContext.element();
      inputCounter.inc();

      String key = rowMeta.getString(hopRow.getRow(), keyIndex);
      String value = rowMeta.getString(hopRow.getRow(), valueIndex);

      processContext.output( KV.of( key, value ) );
      writtenCounter.inc();

    } catch ( Exception e ) {
      numErrors.inc();
      LOG.error( "Error in HopRow to KV<String,String> function", e );
      throw new RuntimeException( "Error in HopRow to KV<String,String> function", e );
    }
  }
}