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
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.addsequence.AddSequenceMeta;
import org.apache.hop.spark.core.SparkNativeMetrics;
import org.apache.hop.spark.engines.ISparkPipelineEngineRunConfiguration;
import org.apache.hop.spark.pipeline.SparkTransformPlan;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * Add sequence with a counter on the native Spark engine: one sequence over all partitions.
 *
 * <p>As a generic transform every partition started its own counter at the start value, so the
 * values repeated once per partition. Here the rows are numbered in partition order (the order of
 * the rows of a source, see the source partitions option) and the value of row n is what the
 * counter of the local engine gives the n-th row, wrapping at the maximum the same way. The
 * partitions still run in parallel.
 *
 * <p>A database sequence stays the Hop transform on every partition: the database hands out the
 * values. A configuration transform does too, with a counter per partition.
 */
public class SparkAddSequenceHandler extends SparkBaseTransformHandler {

  private final SparkGenericTransformHandler genericHandler;

  public SparkAddSequenceHandler(SparkGenericTransformHandler genericHandler) {
    this.genericHandler = genericHandler;
  }

  @Override
  public SparkTransformPlan plan(
      PipelineMeta pipelineMeta, TransformMeta transformMeta, IVariables variables)
      throws HopException {
    AddSequenceMeta meta = transformMeta(AddSequenceMeta.class, transformMeta, pipelineMeta);
    if (usesDatabase(meta)) {
      return SparkTransformPlan.of(
          SparkTransformPlan.Kind.HOP, "SparkTransformPlan.AddSequence.Database");
    }
    if (!Utils.isEmpty(meta.getConfigurationTransform())) {
      return SparkTransformPlan.of(
          SparkTransformPlan.Kind.HOP, "SparkTransformPlan.AddSequence.CounterTransform");
    }
    if (pipelineMeta.findPreviousTransforms(transformMeta, false).isEmpty()) {
      return new SparkTransformPlan(SparkTransformPlan.Kind.HOP, null);
    }
    return SparkTransformPlan.SPARK_CODE;
  }

  /** A database sequence, or no counter: the transform then reads from a database too. */
  static boolean usesDatabase(AddSequenceMeta meta) {
    return meta.isDatabaseUsed() || !meta.isCounterUsed();
  }

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
    AddSequenceMeta meta = transformMeta(AddSequenceMeta.class, transformMeta, pipelineMeta);
    // The transform's counter: a database sequence, or neither, is Hop's
    boolean database = usesDatabase(meta);
    String configurationTransform = meta.getConfigurationTransform();
    if (input == null || database || !Utils.isEmpty(configurationTransform)) {
      if (!database && !Utils.isEmpty(configurationTransform)) {
        log.logBasic(
            "Add sequence '"
                + transformMeta.getName()
                + "' reads its counter settings from transform '"
                + configurationTransform
                + "': every partition keeps a counter of its own, the values are not unique");
      }
      genericHandler.handleTransform(
          log,
          variables,
          runConfigurationName,
          runConfiguration,
          metadataProvider,
          metastoreJson,
          pipelineMeta,
          transformMeta,
          transformDatasetMap,
          spark,
          rowMeta,
          previousTransforms,
          input);
      return;
    }

    String valueName = variables.resolve(meta.getValueName());
    if (Utils.isEmpty(valueName)) {
      throw new HopException("Add sequence '" + transformMeta.getName() + "' has no value name");
    }
    long start = parse(variables, meta.getStartAt(), 1L, transformMeta);
    long increment = parse(variables, meta.getIncrementBy(), 1L, transformMeta);
    long maximum = parse(variables, meta.getMaxValue(), 0L, transformMeta);
    long cycle = cycleLength(start, increment, maximum);

    StructType schema = input.schema().add(valueName, DataTypes.LongType, true);
    JavaRDD<Row> numbered =
        input
            .toJavaRDD()
            .zipWithIndex()
            .map(
                pair -> {
                  Row row = pair._1();
                  Object[] values = new Object[row.length() + 1];
                  for (int i = 0; i < row.length(); i++) {
                    values[i] = row.get(i);
                  }
                  values[row.length()] = valueAt(start, increment, cycle, pair._2());
                  return RowFactory.create(values);
                });
    Dataset<Row> output = spark.createDataFrame(numbered, schema);
    transformDatasetMap.put(
        transformMeta.getName(),
        trackMetrics(output, transformMeta, SparkNativeMetrics.Role.TRANSFORM));
    log.logBasic(
        "Handled transform '"
            + transformMeta.getName()
            + "' natively: one sequence over all partitions, from "
            + start
            + " by "
            + increment
            + (cycle > 0 ? ", back to the start after " + cycle + " values" : ""));
  }

  private static long parse(
      IVariables variables, String setting, long defaultValue, TransformMeta transformMeta)
      throws HopException {
    String value = variables.resolve(setting);
    if (Utils.isEmpty(Const.trim(value))) {
      return defaultValue;
    }
    try {
      return Long.parseLong(Const.trim(value));
    } catch (NumberFormatException e) {
      throw new HopException(
          "Add sequence '" + transformMeta.getName() + "': '" + value + "' is not a number", e);
    }
  }

  /**
   * The number of values before the counter returns to the start, 0 when it never does. As {@code
   * org.apache.hop.core.Counter}: once the following value passes the maximum, it is the start.
   */
  static long cycleLength(long start, long increment, long maximum) {
    if (increment > 0 && maximum > start) {
      return (maximum - start) / increment + 1;
    }
    if (increment < 0 && maximum < start) {
      return (start - maximum) / -increment + 1;
    }
    return 0;
  }

  /** The value of the row with index {@code index} (0 for the first row). */
  static long valueAt(long start, long increment, long cycle, long index) {
    return start + (cycle > 0 ? index % cycle : index) * increment;
  }
}
