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

package org.apache.hop.execution.sampler.plugins.last;

import java.util.List;

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
    id = "LastRowsExecutionDataSampler",
    name = "Last output rows",
    description = "Samples the last rows of a transform output")
public class LastRowsExecutionDataSampler
    extends ExecutionDataSamplerBase<LastRowsExecutionDataSamplerStore>
    implements IExecutionDataSampler<LastRowsExecutionDataSamplerStore> {
  private static final Class<?> PKG = LastRowsExecutionDataSampler.class;

  public LastRowsExecutionDataSampler() {
    super();
  }

  public LastRowsExecutionDataSampler(LastRowsExecutionDataSampler sampler) {
    super(sampler);
  }

  public LastRowsExecutionDataSampler(String sampleSize) {
    super(sampleSize, "LastRowsExecutionDataSampler", "Last output rows");
  }

  public LastRowsExecutionDataSampler clone() {
    return new LastRowsExecutionDataSampler(this);
  }

  @Override
  public LastRowsExecutionDataSamplerStore createSamplerStore(
      ExecutionDataSamplerMeta samplerMeta) {
    return new LastRowsExecutionDataSamplerStore(this, samplerMeta);
  }

  @Override
  public void sampleRow(
      LastRowsExecutionDataSamplerStore samplerStore,
      IStream.StreamType streamType,
      IRowMeta rowMeta,
      Object[] row) throws HopValueException {

    synchronized (samplerStore.getRows()) {
      List<Object[]> rows = samplerStore.getRows();

      if (streamType != IStream.StreamType.OUTPUT || samplerStore.getMaxRows() <= 0) {
        return;
      }

      if (rows.isEmpty()) {
        samplerStore.setRowMeta(rowMeta);
      }

      // Add to the front, remove the last on overflow
      // This is why we use a linked list
      //
      rows.add(0, rowMeta.cloneRow(row));
      if (rows.size() > samplerStore.getMaxRows()) {
        rows.remove(rows.size() - 1);
      }
    }
  }
}
