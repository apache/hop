package org.apache.hop.beam.core.fn;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.util.JsonRowMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class StringToHopFn extends DoFn<String, HopRow> {

  private String transformName;
  private String rowMetaJson;
  private String separator;
  private List<String> stepPluginClasses;
  private List<String> xpPluginClasses;

  private transient Counter inputCounter;
  private transient Counter writtenCounter;

  // Log and count parse errors.
  private static final Logger LOG = LoggerFactory.getLogger( StringToHopFn.class );

  private transient IRowMeta rowMeta;

  public StringToHopFn( String transformName, String rowMetaJson, String separator, List<String> stepPluginClasses, List<String> xpPluginClasses ) {
    this.transformName = transformName;
    this.rowMetaJson = rowMetaJson;
    this.separator = separator;
    this.stepPluginClasses = stepPluginClasses;
    this.xpPluginClasses = xpPluginClasses;
  }

  @Setup
  public void setUp() {
    try {
      inputCounter = Metrics.counter( "input", transformName );
      writtenCounter = Metrics.counter( "written", transformName );

      // Initialize Kettle Beam
      //
      BeamHop.init( stepPluginClasses, xpPluginClasses );
      rowMeta = JsonRowMeta.fromJson( rowMetaJson );

      Metrics.counter( "init", transformName ).inc();
    } catch ( Exception e ) {
      Metrics.counter( "error", transformName ).inc();
      LOG.error( "Error in setup of converting input data into Kettle rows : " + e.getMessage() );
      throw new RuntimeException( "Error in setup of converting input data into Kettle rows", e );
    }
  }

  @ProcessElement
  public void processElement( ProcessContext processContext ) {

    try {

      String inputString = processContext.element();
      inputCounter.inc();

      String[] components = inputString.split( separator, -1 );

      // TODO: implement enclosure in FileDefinition
      //

      Object[] row = RowDataUtil.allocateRowData( rowMeta.size() );
      int index = 0;
      while ( index < rowMeta.size() && index < components.length ) {
        String sourceString = components[ index ];
        IValueMeta valueMeta = rowMeta.getValueMeta( index );
        IValueMeta stringMeta = new ValueMetaString( "SourceString" );
        stringMeta.setConversionMask( valueMeta.getConversionMask() );
        try {
          row[ index ] = valueMeta.convertDataFromString( sourceString, stringMeta, null, null, IValueMeta.TRIM_TYPE_NONE );
        } catch ( HopValueException ve ) {
          throw new HopException( "Unable to convert value '" + sourceString + "' to value : " + valueMeta.toStringMeta(), ve );
        }
        index++;
      }

      // Pass the row to the process context
      //
      processContext.output( new HopRow( row ) );
      writtenCounter.inc();

    } catch ( Exception e ) {
      Metrics.counter( "error", transformName ).inc();
      LOG.error( "Error converting input data into Kettle rows " + processContext.element() + ", " + e.getMessage() );
      throw new RuntimeException( "Error converting input data into Kettle rows", e );

    }
  }


}
