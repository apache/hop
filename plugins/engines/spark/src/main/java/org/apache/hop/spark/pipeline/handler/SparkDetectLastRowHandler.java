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
package org.apache.hop.spark.pipeline.handler;

import java.util.List;
import java.util.Map;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.detectlastrow.DetectLastRowMeta;
import org.apache.hop.spark.core.SparkNativeMetrics;
import org.apache.hop.spark.engines.ISparkPipelineEngineRunConfiguration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.storage.StorageLevel;

/**
 * Identify last row in a stream on the native Spark engine: only the last row of all rows is the
 * last row, not the last row of every partition.
 *
 * <p>The rows are numbered in partition order (the order of the rows of a source) and counted; the
 * row with the highest number gets true, all others false. The input is kept so the transforms
 * before this one run once for the count and the rows. The partitions still run in parallel.
 */
public class SparkDetectLastRowHandler extends SparkBaseTransformHandler {

  @Override
  public void handleTransform(
      ILogChannel log,
      IVariables variables,
      String runConfigurationName,
      ISparkPipelineEngineRunConfiguration runConfiguration,
      IHopMetadataProvider metadataProvider,
      String metastoreJson,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      Map<String, Dataset<Row>> transformDatasetMap,
      SparkSession spark,
      IRowMeta rowMeta,
      List<TransformMeta> previousTransforms,
      Dataset<Row> input)
      throws HopException {
    if (input == null) {
      throw new HopException("Identify last row '" + transformMeta.getName() + "' has no input");
    }
    String resultField =
        variables.resolve(
            transformMeta(DetectLastRowMeta.class, transformMeta, pipelineMeta)
                .getResultFieldName());
    Dataset<Row> output;
    if (Utils.isEmpty(resultField)) {
      // Nothing to flag: the rows pass on
      output = input;
    } else {
      Dataset<Row> kept = input.persist(StorageLevel.MEMORY_AND_DISK());
      long lastIndex = kept.count() - 1;
      JavaRDD<Row> flagged =
          kept.toJavaRDD()
              .zipWithIndex()
              .map(
                  pair -> {
                    Row row = pair._1();
                    Object[] values = new Object[row.length() + 1];
                    for (int i = 0; i < row.length(); i++) {
                      values[i] = row.get(i);
                    }
                    values[row.length()] = pair._2() == lastIndex;
                    return RowFactory.create(values);
                  });
      output =
          spark.createDataFrame(
              flagged, kept.schema().add(resultField, DataTypes.BooleanType, true));
    }
    transformDatasetMap.put(
        transformMeta.getName(),
        trackMetrics(output, transformMeta, SparkNativeMetrics.Role.TRANSFORM));
    log.logBasic(
        "Handled transform '"
            + transformMeta.getName()
            + "' natively: the last row of all partitions is flagged");
  }
}
