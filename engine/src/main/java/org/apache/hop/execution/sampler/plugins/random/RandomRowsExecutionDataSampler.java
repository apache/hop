/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hop.execution.sampler.plugins.random;

import java.util.List;
import java.util.Random;

import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.execution.sampler.ExecutionDataSamplerMeta;
import org.apache.hop.execution.sampler.ExecutionDataSamplerPlugin;
import org.apache.hop.execution.sampler.IExecutionDataSampler;
import org.apache.hop.execution.sampler.plugins.ExecutionDataSamplerBase;
import org.apache.hop.pipeline.transform.stream.IStream;

@GuiPlugin
@ExecutionDataSamplerPlugin(
    id = "RandomRowsExecutionDataSampler",
    name = "Random output rows",
    description = "Do reservoir sampling on the output rows of a transform")
public class RandomRowsExecutionDataSampler
    extends ExecutionDataSamplerBase<RandomRowsExecutionDataSamplerStore>
    implements IExecutionDataSampler<RandomRowsExecutionDataSamplerStore> {

  public RandomRowsExecutionDataSampler() {
    super();
  }

  public RandomRowsExecutionDataSampler(RandomRowsExecutionDataSampler sampler) {
    super(sampler);
  }

  public RandomRowsExecutionDataSampler(String sampleSize) {
    super(sampleSize, "RandomRowsExecutionDataSampler", "Random output rows");
  }

  public RandomRowsExecutionDataSampler clone() {
    return new RandomRowsExecutionDataSampler(this);
  }

  private final Random random = new Random();

  @Override
  public RandomRowsExecutionDataSamplerStore createSamplerStore(
      ExecutionDataSamplerMeta samplerMeta) {
    return new RandomRowsExecutionDataSamplerStore(this, samplerMeta);
  }

  @Override
  public void sampleRow(
      RandomRowsExecutionDataSamplerStore samplerStore,
      IStream.StreamType streamType,
      IRowMeta rowMeta,
      Object[] row) throws HopValueException {

    synchronized (samplerStore.getRows()) {
      List<Object[]> rows = samplerStore.getRows();

      if (samplerStore.getMaxRows() <= 0 || streamType != IStream.StreamType.OUTPUT) {
        return;
      }

      // Do reservoir sampling to get a random data set from a stream of rows
      //
      if (rows.size() < samplerStore.getMaxRows()) {
        if (rows.isEmpty()) {
          samplerStore.setRowMeta(rowMeta);
        }
        rows.add(row);
      } else {
        int randomIndex = random.nextInt(samplerStore.getMaxRows());
        if (randomIndex < samplerStore.getMaxRows()) {
          rows.set(randomIndex, rowMeta.cloneRow(row));
        }
      }
    }
  }
}
