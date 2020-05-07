package org.apache.hop.beam.core.partition;

import org.apache.beam.sdk.transforms.Partition;
import org.apache.hop.beam.core.HopRow;

public class SinglePartitionFn implements Partition.PartitionFn<HopRow> {

  private static final long serialVersionUID = 95100000000000001L;

  @Override public int partitionFor( HopRow elem, int numPartitions ) {
    return 0;
  }
}
