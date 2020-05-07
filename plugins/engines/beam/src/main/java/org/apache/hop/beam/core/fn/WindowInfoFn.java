package org.apache.hop.beam.core.fn;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.commons.lang.StringUtils;
import org.joda.time.Instant;
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.util.JsonRowMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class WindowInfoFn extends DoFn<HopRow, HopRow> {

  private String transformName;
  private String maxWindowField;
  private String startWindowField;
  private String endWindowField;
  private String rowMetaJson;
  private List<String> stepPluginClasses;
  private List<String> xpPluginClasses;

  private transient Counter initCounter;
  private transient Counter readCounter;
  private transient Counter writtenCounter;
  private transient Counter errorCounter;

  private transient int fieldIndex;

  // Log and count parse errors.
  private static final Logger LOG = LoggerFactory.getLogger( WindowInfoFn.class );

  private transient IRowMeta inputRowMeta;
  private transient IValueMeta fieldValueMeta;

  public WindowInfoFn( String transformName, String maxWindowField, String startWindowField, String endWindowField, String rowMetaJson, List<String> stepPluginClasses, List<String> xpPluginClasses ) {
    this.transformName = transformName;
    this.maxWindowField = maxWindowField;
    this.startWindowField = startWindowField;
    this.endWindowField = endWindowField;
    this.rowMetaJson = rowMetaJson;
    this.stepPluginClasses = stepPluginClasses;
    this.xpPluginClasses = xpPluginClasses;
  }

  @Setup
  public void setUp() {
    try {
      readCounter = Metrics.counter( "read", transformName );
      writtenCounter = Metrics.counter( "written", transformName );
      errorCounter = Metrics.counter( "error", transformName );

      // Initialize Kettle Beam
      //
      BeamHop.init( stepPluginClasses, xpPluginClasses );
      inputRowMeta = JsonRowMeta.fromJson( rowMetaJson );

      Metrics.counter( "init", transformName ).inc();
    } catch(Exception e) {
      errorCounter.inc();
      LOG.error( "Error in setup of adding window information to rows : " + e.getMessage() );
      throw new RuntimeException( "Error in setup of adding window information to rows", e );
    }
  }


  @ProcessElement
  public void processElement( ProcessContext processContext, BoundedWindow window ) {

    try {

      HopRow kettleRow = processContext.element();
      readCounter.inc();

      Instant instant = window.maxTimestamp();

      Object[] outputRow = RowDataUtil.createResizedCopy( kettleRow.getRow(), inputRowMeta.size()+3 );

      int fieldIndex = inputRowMeta.size();

      // Kettle "Date" type field output: java.util.Date.
      // Use the last field in the output
      //
      if ( StringUtils.isNotEmpty( startWindowField ) ) {
        if ( window instanceof IntervalWindow ) {
          IntervalWindow intervalWindow = (IntervalWindow) window;
          Instant start = intervalWindow.start();
          if ( start != null ) {
            outputRow[ fieldIndex ] = start.toDate();
          }
        }
        fieldIndex++;
      }
      if ( StringUtils.isNotEmpty( endWindowField ) ) {
        if ( window instanceof IntervalWindow ) {
          IntervalWindow intervalWindow = (IntervalWindow) window;
          Instant end = intervalWindow.end();
          if ( end != null ) {
            outputRow[ fieldIndex ] = end.toDate();
          }
        }
        fieldIndex++;
      }

      if ( StringUtils.isNotEmpty( maxWindowField ) ) {
        Instant maxTimestamp = window.maxTimestamp();
        if ( maxTimestamp != null ) {
          outputRow[ fieldIndex ] = maxTimestamp.toDate();
        }
        fieldIndex++;
      }

      // Pass the new row to the process context
      //
      HopRow outputHopRow = new HopRow( outputRow );
      processContext.outputWithTimestamp( outputHopRow, instant );
      writtenCounter.inc();

    } catch ( Exception e ) {
      errorCounter.inc();
      LOG.error( "Error adding window information to rows : " + processContext.element() + ", " + e.getMessage() );
      throw new RuntimeException( "Error adding window information to rows", e );
    }
  }

}
