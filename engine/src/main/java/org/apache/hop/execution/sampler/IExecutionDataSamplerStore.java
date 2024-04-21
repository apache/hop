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

package org.apache.hop.execution.sampler;

import java.util.Map;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowBuffer;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.execution.ExecutionDataSetMeta;
import org.apache.hop.pipeline.transform.IRowListener;

public interface IExecutionDataSamplerStore {

  /**
   * Initializes this sampler store
   *
   * @param variables The variables to resolve with
   * @param inputRowMeta Transform input row metadata
   * @param outputRowMeta Transform output row metadata
   */
  void init(IVariables variables, IRowMeta inputRowMeta, IRowMeta outputRowMeta);

  /**
   * Create a row listener to attach to a transform
   *
   * @return The row listener for this store.
   */
  IRowListener createRowListener(IExecutionDataSampler<?> dataSampler);

  /**
   * Return a map of keys and rows of data. Every key represents a different type of sampling data.
   *
   * @return The samples map for the sampler store
   */
  Map<String, RowBuffer> getSamples();

  /**
   * Get some extra information about the data samples.
   *
   * @return The metadata map where for every key more information (description) is given about the
   *     sampled data.
   */
  Map<String, ExecutionDataSetMeta> getSamplesMetadata();
}
