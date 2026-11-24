/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.hop.core.annotations.PartitionerPlugin;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.w3c.dom.Node;

/**
 * Spreads the rows evenly over the partitions, whatever their values: the fix for skewed or
 * lopsided data (after a filter, after reading one big file). The local engine hands the rows out
 * round robin; Spark and Beam rebalance with their own shuffle.
 */
@PartitionerPlugin(
    id = RebalancePartitioner.ID,
    name = "Rebalance",
    description = "Spreads the rows evenly over the partitions")
public class RebalancePartitioner extends BasePartitioner {
  public static final String ID = "RebalancePartitioner";

  /** Shared by the copies of the transform that hand rows on: they take turns. */
  private final AtomicLong nextRow = new AtomicLong();

  public RebalancePartitioner() {
    super();
    setId(ID);
  }

  @Override
  public IPartitioner getInstance() {
    IPartitioner partitioner = new RebalancePartitioner();
    partitioner.setId(getId());
    partitioner.setDescription(getDescription());
    return partitioner;
  }

  /** Nothing to set: no dialog. */
  @Override
  public String getDialogClassName() {
    return null;
  }

  @Override
  public int getPartition(IVariables variables, IRowMeta rowMeta, Object[] row)
      throws HopException {
    init(variables, rowMeta);
    return (int) Math.floorMod(nextRow.getAndIncrement(), (long) nrPartitions);
  }

  @Override
  public String getDescription() {
    return "Rebalance";
  }

  @Override
  public String getXml() {
    return "";
  }

  @Override
  public void loadXml(Node partitioningMethodNode) {
    // No settings
  }
}
