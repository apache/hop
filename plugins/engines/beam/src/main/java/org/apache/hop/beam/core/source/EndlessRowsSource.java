package org.apache.hop.beam.core.source;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.hop.beam.core.HopRow;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class EndlessRowsSource extends UnboundedSource<HopRow, EndlessCheckpointMark > {
  @Override public List<? extends UnboundedSource<HopRow, EndlessCheckpointMark>> split( int desiredNumSplits, PipelineOptions options ) throws Exception {
    return Arrays.asList();
  }

  @Override public UnboundedReader<HopRow> createReader( PipelineOptions options, @Nullable EndlessCheckpointMark checkpointMark ) throws IOException {
    return new EndlessHopRowReader(this);
  }

  @Override public boolean requiresDeduping() {
    return false;
  }

  @Override public Coder<EndlessCheckpointMark> getCheckpointMarkCoder() {
    return null;
  }
}
