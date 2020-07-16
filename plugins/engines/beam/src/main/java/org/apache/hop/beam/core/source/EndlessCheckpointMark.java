package org.apache.hop.beam.core.source;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.CountingSource;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.hop.beam.core.HopRow;
import org.joda.time.Instant;

import java.io.IOException;

/**
 * The checkpoint for an unbounded {@link CountingSource} is simply the last value produced. The
 * associated source object encapsulates the information needed to produce the next value.
 */
@DefaultCoder( AvroCoder.class)
public class EndlessCheckpointMark implements UnboundedSource.CheckpointMark {
  /** The last value emitted. */
  private final HopRow lastHopRow;

  private final Instant startTime;

  /** Creates a checkpoint mark reflecting the last emitted value. */
  public EndlessCheckpointMark(HopRow lastHopRow, Instant startTime) {
    this.lastHopRow = lastHopRow;
    this.startTime = startTime;
  }

  /** Returns the last value emitted by the reader. */
  public HopRow getLastHopRow() {
    return lastHopRow;
  }

  /** Returns the time the reader was started. */
  public Instant getStartTime() {
    return startTime;
  }

  /////////////////////////////////////////////////////////////////////////////////////

  @SuppressWarnings("unused") // For AvroCoder
  private EndlessCheckpointMark() {
    this.lastHopRow = new HopRow();
    this.startTime = Instant.now();
  }

  @Override
  public void finalizeCheckpoint() throws IOException {}
}
