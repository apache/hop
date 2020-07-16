package org.apache.hop.beam.core.source;

import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.hop.beam.core.HopRow;
import org.joda.time.Instant;

import java.io.IOException;;
import java.util.NoSuchElementException;

public class EndlessHopRowReader extends UnboundedSource.UnboundedReader<HopRow> {

  private UnboundedSource<HopRow, ?> currentSource;

  public EndlessHopRowReader( UnboundedSource<HopRow, ?> currentSource ) {
    this.currentSource = currentSource;
  }

  @Override public boolean start() throws IOException {
    return true;
  }

  @Override public boolean advance() throws IOException {
    return true;
  }

  @Override public Instant getWatermark() {
    return null;
  }

  @Override public UnboundedSource.CheckpointMark getCheckpointMark() {
    return null;
  }

  @Override public UnboundedSource<HopRow, ?> getCurrentSource() {
    return currentSource;
  }

  @Override public HopRow getCurrent() throws NoSuchElementException {
    return null;
  }

  @Override public Instant getCurrentTimestamp() throws NoSuchElementException {
    return Instant.now();
  }

  @Override public void close() throws IOException {
    // Never stops
  }

}
