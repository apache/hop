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

import java.io.Serializable;
import java.util.ArrayList;
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
import org.apache.hop.pipeline.transforms.samplerows.SampleRowsMeta;
import org.apache.hop.spark.core.SparkNativeMetrics;
import org.apache.hop.spark.engines.ISparkPipelineEngineRunConfiguration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * Sample rows on the native Spark engine: the line numbers count all rows, not the rows of a
 * partition.
 *
 * <p>As a generic transform every partition kept its own line count, so "1..10" gave the first 10
 * rows of every partition. Here the rows are numbered in partition order (the order of the rows of
 * a source) and the rows whose number is in the range pass on, with that number in the line number
 * field. The partitions still run in parallel.
 */
public class SparkSampleRowsHandler extends SparkBaseTransformHandler {

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
      throw new HopException("Sample rows '" + transformMeta.getName() + "' has no input");
    }
    SampleRowsMeta meta = transformMeta(SampleRowsMeta.class, transformMeta, pipelineMeta);
    String range = variables.resolve(meta.getLinesRange());
    String lineNumberField = variables.resolve(meta.getLineNumberField());
    boolean addLineNumber = !Utils.isEmpty(lineNumberField);
    LineRanges ranges = LineRanges.parse(range);

    StructType schema =
        addLineNumber
            ? input.schema().add(lineNumberField, DataTypes.LongType, true)
            : input.schema();
    JavaRDD<Row> sampled =
        input
            .toJavaRDD()
            .zipWithIndex()
            .filter(pair -> ranges.contains(pair._2() + 1))
            .map(
                pair -> {
                  Row row = pair._1();
                  if (!addLineNumber) {
                    return row;
                  }
                  Object[] values = new Object[row.length() + 1];
                  for (int i = 0; i < row.length(); i++) {
                    values[i] = row.get(i);
                  }
                  values[row.length()] = pair._2() + 1;
                  return RowFactory.create(values);
                });
    transformDatasetMap.put(
        transformMeta.getName(),
        trackMetrics(
            spark.createDataFrame(sampled, schema),
            transformMeta,
            SparkNativeMetrics.Role.TRANSFORM));
    log.logBasic(
        "Handled transform '"
            + transformMeta.getName()
            + "' natively: the rows with line number "
            + range
            + " over all partitions");
  }

  /**
   * The lines of a Sample rows range such as {@code 1..10,15,20..30}, read as the transform reads
   * them: a part that is neither a number nor a range of two numbers is left out.
   */
  static final class LineRanges implements Serializable {
    private final List<long[]> ranges;

    private LineRanges(List<long[]> ranges) {
      this.ranges = ranges;
    }

    static LineRanges parse(String range) {
      List<long[]> ranges = new ArrayList<>();
      if (range != null) {
        for (String part : range.split(",")) {
          if (part.matches("\\d+")) {
            long line = Long.parseLong(part);
            ranges.add(new long[] {line, line});
          } else if (part.matches("\\d+\\.\\.\\d+")) {
            String[] bounds = part.split("\\.\\.");
            ranges.add(new long[] {Long.parseLong(bounds[0]), Long.parseLong(bounds[1])});
          }
        }
      }
      return new LineRanges(ranges);
    }

    boolean contains(long line) {
      for (long[] range : ranges) {
        if (line >= range[0] && line <= range[1]) {
          return true;
        }
      }
      return false;
    }
  }
}
