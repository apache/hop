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

package org.apache.hop.execution;

import org.apache.hop.core.row.RowBuffer;
import org.apache.hop.execution.sampler.IExecutionDataSamplerStore;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;

import java.util.*;

public final class ExecutionDataBuilder {
  private Date collectionDate;
  private String parentId;
  private String ownerId;
  private Map<String, RowBuffer> dataSets;
  private Map<String, ExecutionDataSetMeta> setMetaData;

  private ExecutionDataBuilder() {
    this.collectionDate = new Date();
    this.dataSets = Collections.synchronizedMap(new HashMap<>());
    this.setMetaData = Collections.synchronizedMap(new HashMap<>());
  }

  public static ExecutionDataBuilder anExecutionData() {
    return new ExecutionDataBuilder();
  }

  public static ExecutionDataBuilder fromAllTransformData(
      IPipelineEngine<PipelineMeta> pipeline, List<IExecutionDataSamplerStore> samplerStores) {
    ExecutionDataBuilder dataBuilder =
        ExecutionDataBuilder.anExecutionData()
            .withParentId(pipeline.getLogChannelId())
            .withOwnerId("all-transforms");

    for (IExecutionDataSamplerStore samplerStore : samplerStores) {
      dataBuilder =
          dataBuilder
              .addDataSets(samplerStore.getSamples())
              .addSetMeta(samplerStore.getSamplesMetadata());
    }
    dataBuilder = dataBuilder.withCollectionDate(new Date());
    return dataBuilder;
  }

  public ExecutionDataBuilder withCollectionDate(Date collectionDate) {
    this.collectionDate = collectionDate;
    return this;
  }

  public ExecutionDataBuilder withParentId(String parentId) {
    this.parentId = parentId;
    return this;
  }

  public ExecutionDataBuilder withOwnerId(String ownerId) {
    this.ownerId = ownerId;
    return this;
  }

  public ExecutionDataBuilder withDataSets(Map<String, RowBuffer> dataSets) {
    this.dataSets = dataSets;
    return this;
  }

  public ExecutionDataBuilder withSetDescriptions(
      Map<String, ExecutionDataSetMeta> setDescriptions) {
    synchronized (setDescriptions) {
      this.setMetaData = setDescriptions;
    }
    return this;
  }

  public ExecutionDataBuilder addDataSets(Map<String, RowBuffer> dataSets) {
    synchronized (dataSets) {
      this.dataSets.putAll(dataSets);
    }
    return this;
  }

  public ExecutionDataBuilder addSetMeta(Map<String, ExecutionDataSetMeta> setDescriptions) {
    this.setMetaData.putAll(setDescriptions);
    return this;
  }

  public ExecutionData build() {
    ExecutionData executionData = new ExecutionData();
    executionData.setCollectionDate(collectionDate);
    executionData.setParentId(parentId);
    executionData.setOwnerId(ownerId);
    executionData.setDataSets(dataSets);
    executionData.setSetMetaData(setMetaData);
    return executionData;
  }
}
