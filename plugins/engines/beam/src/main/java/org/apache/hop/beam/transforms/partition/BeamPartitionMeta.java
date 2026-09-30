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
 */

package org.apache.hop.beam.transforms.partition;

import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.hop.beam.core.BeamDefaults;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.partition.KeyedPartitionFn;
import org.apache.hop.beam.core.partition.SinglePartitionFn;
import org.apache.hop.beam.engines.IBeamPipelineEngineRunConfiguration;
import org.apache.hop.beam.pipeline.IBeamPipelineTransformHandler;
import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.JsonRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Issue #2040: partitioning support for Beam pipelines.
 *
 * <p>Mirrors what the local Hop engine does with its partitioning option: decide up front how many
 * partitions exist and which partition each row belongs to, so that the steps that follow can be
 * grouped per key instead of shuffled.
 *
 * <p>Two modes. {@code Single} sends everything to one partition, which serialises everything
 * downstream. {@code Key} hashes a field so rows sharing a value land together.
 */
@Transform(
    id = "BeamPartition",
    image = "beam-partition.svg",
    name = "i18n::BeamPartitionDialog.DialogTitle",
    description = "i18n::BeamPartitionDialog.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.BigData",
    keywords = "i18n::BeamPartitionMeta.keyword",
    documentationUrl = "/pipeline/transforms/beampartition.html",
    supportedEngines = {"Beam*"})
public class BeamPartitionMeta extends BaseTransformMeta<BeamPartition, BeamPartitionData>
    implements IBeamPipelineTransformHandler {

  @HopMetadataProperty(key = "partition_type")
  @Getter
  @Setter
  private String partitionType;

  /** The field to partition on. Required for the Key mode, ignored for Single. */
  @HopMetadataProperty(key = "key_field")
  @Getter
  @Setter
  private String keyField;

  /** How many partitions to create. */
  @HopMetadataProperty(key = "num_partitions")
  @Getter
  @Setter
  private String numPartitions;

  public BeamPartitionMeta() {
    super();
    setDefault();
  }

  @Override
  public void setDefault() {
    partitionType = BeamDefaults.PARTITION_TYPE_SINGLE;
    keyField = "";
    numPartitions = "1";
  }

  @Override
  public boolean isInput() {
    return false;
  }

  @Override
  public boolean isOutput() {
    return false;
  }

  @Override
  public void handleTransform(
      ILogChannel log,
      IVariables variables,
      String runConfigurationName,
      IBeamPipelineEngineRunConfiguration runConfiguration,
      String dataSamplersJson,
      IHopMetadataProvider metadataProvider,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      Map<String, PCollection<HopRow>> transformCollectionMap,
      Pipeline pipeline,
      IRowMeta rowMeta,
      List<TransformMeta> previousTransforms,
      PCollection<HopRow> input,
      String parentLogChannelId)
      throws HopException {

    int partitions = 1;
    if (org.apache.commons.lang3.StringUtils.isNotEmpty(numPartitions)) {
      partitions = Const.toInt(variables.resolve(numPartitions), 1);
    }
    if (partitions < 1) {
      throw new HopException(
          "The number of partitions of Beam Partition transform '"
              + transformMeta.getName()
              + "' has to be at least 1, but it is "
              + partitions);
    }

    Partition.PartitionFn<HopRow> partitionFn;
    if (BeamDefaults.PARTITION_TYPE_KEY.equals(partitionType)) {
      if (org.apache.commons.lang3.StringUtils.isEmpty(keyField)) {
        throw new HopException(
            "Please specify a key field for Beam Partition transform '"
                + transformMeta.getName()
                + "'");
      }
      String realKeyField = variables.resolve(keyField);
      int keyIndex = rowMeta == null ? -1 : rowMeta.indexOfValue(realKeyField);
      if (keyIndex < 0) {
        throw new HopException(
            "Unable to find the key field '"
                + realKeyField
                + "' in the input of Beam Partition transform '"
                + transformMeta.getName()
                + "'");
      }
      partitionFn =
          new KeyedPartitionFn(transformMeta.getName(), JsonRowMeta.toJson(rowMeta), keyIndex);
    } else {
      partitionFn = new SinglePartitionFn();
    }

    // Beam's Partition transform returns a PCollectionList, one collection per partition. The rows
    // have to be recombined, because Beam picks the partition per row at runtime and a pipeline
    // cannot branch on that. Taking a single partition would silently drop the rows that landed in
    // the others, so flatten them all back into one stream. What partitioning buys is that the
    // decision was made deterministically, before the following transforms ran.
    //
    PCollectionList<HopRow> partitionList = input.apply(Partition.of(partitions, partitionFn));

    PCollection<HopRow> result =
        partitions == 1 ? partitionList.get(0) : partitionList.apply(Flatten.pCollections());

    transformCollectionMap.put(transformMeta.getName(), result);
  }
}
