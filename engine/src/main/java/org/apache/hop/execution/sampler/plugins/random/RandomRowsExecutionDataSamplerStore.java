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
import java.util.Map;
import org.apache.hop.core.Const;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowBuffer;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.execution.ExecutionDataSetMeta;
import org.apache.hop.execution.sampler.ExecutionDataSamplerMeta;
import org.apache.hop.execution.sampler.ExecutionDataSamplerStoreBase;
import org.apache.hop.execution.sampler.IExecutionDataSamplerStore;

/** A class meant to contain transform execution sampling data */
public class RandomRowsExecutionDataSamplerStore
    extends ExecutionDataSamplerStoreBase<RandomRowsExecutionDataSamplerStore>
    implements IExecutionDataSamplerStore {
  public static final String EXECUTION_DATA_SAMPLE_RANDOM_OUTPUT = "RandomOutput";

  private RandomRowsExecutionDataSampler dataSampler;

  public RandomRowsExecutionDataSamplerStore(
      RandomRowsExecutionDataSampler dataSampler,
      ExecutionDataSamplerMeta samplerMeta,
      IRowMeta rowMeta,
      List<Object[]> rows,
      int maxRows) {
    super(samplerMeta, rowMeta, rows, maxRows);
    this.dataSampler = dataSampler;
  }

  public RandomRowsExecutionDataSamplerStore(
      RandomRowsExecutionDataSampler dataSampler, ExecutionDataSamplerMeta samplerMeta) {
    this(dataSampler, samplerMeta, null, null, 0);
  }

  @Override
  public void init(IVariables variables, IRowMeta inputRowMeta, IRowMeta outputRowMeta) {
    super.init(variables, inputRowMeta, outputRowMeta);
    maxRows = Const.toInt(variables.resolve(dataSampler.getSampleSize()), 0);
  }

  @Override
  public RandomRowsExecutionDataSamplerStore getStore() {
    return this;
  }

  @Override
  public Map<String, RowBuffer> getSamples() {
    return Map.of(
        getKeyForStore(EXECUTION_DATA_SAMPLE_RANDOM_OUTPUT, samplerMeta),
        new RowBuffer(rowMeta, rows));
  }

  @Override
  public Map<String, ExecutionDataSetMeta> getSamplesMetadata() {
    String setKey = getKeyForStore(EXECUTION_DATA_SAMPLE_RANDOM_OUTPUT, samplerMeta);
    String description =
        "Random rows of "
            + getSamplerMeta().getTransformName()
            + "."
            + getSamplerMeta().getCopyNr();
    ExecutionDataSetMeta meta =
        new ExecutionDataSetMeta(
            setKey,
            samplerMeta.getLogChannelId(),
            samplerMeta.getTransformName(),
            samplerMeta.getCopyNr(),
            description);

    return Map.of(setKey, meta);
  }

  /**
   * Gets dataSampler
   *
   * @return value of dataSampler
   */
  public RandomRowsExecutionDataSampler getDataSampler() {
    return dataSampler;
  }

  /**
   * Sets dataSampler
   *
   * @param dataSampler value of dataSampler
   */
  public void setDataSampler(RandomRowsExecutionDataSampler dataSampler) {
    this.dataSampler = dataSampler;
  }

  /**
   * Gets samplerMeta
   *
   * @return value of samplerMeta
   */
  @Override
  public ExecutionDataSamplerMeta getSamplerMeta() {
    return samplerMeta;
  }
}
