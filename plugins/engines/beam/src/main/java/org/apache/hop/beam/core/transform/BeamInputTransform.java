package org.apache.hop.beam.core.transform;

import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.fn.StringToHopFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.List;

public class BeamInputTransform extends PTransform<PBegin, PCollection<HopRow>> {

  // These non-transient privates get serialized to spread across nodes
  //
  private String transformName;
  private String inputLocation;
  private String separator;
  private String rowMetaJson;
  private List<String> stepPluginClasses;
  private List<String> xpPluginClasses;

  // Log and count errors.
  private static final Logger LOG = LoggerFactory.getLogger( BeamInputTransform.class );
  private static final Counter numErrors = Metrics.counter( "main", "BeamInputError" );

  public BeamInputTransform() {
  }

  public BeamInputTransform( @Nullable String name, String transformName, String inputLocation, String separator, String rowMetaJson, List<String> stepPluginClasses, List<String> xpPluginClasses ) {
    super( name );
    this.transformName = transformName;
    this.inputLocation = inputLocation;
    this.separator = separator;
    this.rowMetaJson = rowMetaJson;
    this.stepPluginClasses = stepPluginClasses;
    this.xpPluginClasses = xpPluginClasses;
  }

  @Override public PCollection<HopRow> expand( PBegin input ) {

    try {
      // Only initialize once on this node/vm
      //
      BeamHop.init(stepPluginClasses, xpPluginClasses);

      // System.out.println("-------------- TextIO.Read from "+inputLocation+" (UNCOMPRESSED)");

      TextIO.Read ioRead = TextIO.read()
        .from( inputLocation )
        .withCompression( Compression.UNCOMPRESSED )
        ;

      StringToHopFn stringToHopFn = new StringToHopFn( transformName, rowMetaJson, separator, stepPluginClasses, xpPluginClasses );

      PCollection<HopRow> output = input

        // We read a bunch of Strings, one per line basically
        //
        .apply( transformName + " READ FILE",  ioRead )

        // We need to transform these lines into Kettle fields
        //
        .apply( transformName, ParDo.of( stringToHopFn ) );

      return output;

    } catch ( Exception e ) {
      numErrors.inc();
      LOG.error( "Error in beam input transform", e );
      throw new RuntimeException( "Error in beam input transform", e );
    }

  }


}
