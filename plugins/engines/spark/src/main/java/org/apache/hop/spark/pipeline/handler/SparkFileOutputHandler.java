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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.spark.core.SparkNativeMetrics;
import org.apache.hop.spark.engines.ISparkPipelineEngineRunConfiguration;
import org.apache.hop.spark.transforms.io.SparkFileInputMeta;
import org.apache.hop.spark.transforms.io.SparkFileOutputMeta;
import org.apache.hop.spark.util.SparkPathDialect;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * Native Spark file write for {@link SparkFileOutputMeta}. Executes the write as an action and
 * registers an empty leaf Dataset so a later engine-level {@code count()} does not re-run the
 * write.
 */
public class SparkFileOutputHandler extends SparkBaseTransformHandler {

  @Override
  public boolean isOutput() {
    return true;
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

    if (input == null) {
      throw new HopException(
          "Spark File Output '"
              + transformMeta.getName()
              + "' has no input Dataset. Connect an enabled hop from an upstream transform"
              + " (disabled hops and disconnected transforms are not executed on native Spark).");
    }

    SparkFileOutputMeta meta = new SparkFileOutputMeta();
    loadTransformMetadata(meta, transformMeta, metadataProvider, pipelineMeta);

    String resolved = variables.resolve(meta.getFilePath());
    String path = SparkPathDialect.toSparkUri(resolved, runConfiguration);
    if (StringUtils.isEmpty(path)) {
      throw new HopException(
          "Spark File Output '" + transformMeta.getName() + "' has no file path configured");
    }
    if (log != null && StringUtils.isNotEmpty(resolved) && !resolved.trim().equals(path)) {
      log.logBasic(
          "Spark File Output '"
              + transformMeta.getName()
              + "' path scheme map: '"
              + resolved
              + "' -> '"
              + path
              + "'");
    }

    String format = SparkFileIoSupport.normalizeFormat(meta.getFileFormat());
    SaveMode saveMode = SparkFileIoSupport.toSaveMode(meta.getSaveMode());

    Map<String, String> options =
        SparkFileIoSupport.parseExtraOptions(variables, meta.getExtraOptions());
    if (SparkFileInputMeta.FORMAT_CSV.equals(format)
        || SparkFileInputMeta.FORMAT_TEXT.equals(format)) {
      options.putIfAbsent("header", Boolean.toString(meta.isHeader()));
      if (StringUtils.isNotEmpty(meta.getSeparator())) {
        options.putIfAbsent("sep", variables.resolve(meta.getSeparator()));
      }
      if (StringUtils.isNotEmpty(meta.getQuote())) {
        options.putIfAbsent("quote", variables.resolve(meta.getQuote()));
      }
    }

    String[] partitionColumns = null;
    if (StringUtils.isNotEmpty(meta.getPartitionByColumns())) {
      List<String> parts = new ArrayList<>();
      for (String p : meta.getPartitionByColumns().split(",")) {
        if (StringUtils.isNotEmpty(p.trim())) {
          parts.add(p.trim());
        }
      }
      if (!parts.isEmpty()) {
        partitionColumns = parts.toArray(new String[0]);
      }
    }

    Integer coalesce = null;
    if (StringUtils.isNotEmpty(meta.getCoalescePartitions())) {
      coalesce = Const.toInt(variables.resolve(meta.getCoalescePartitions()), -1);
      if (coalesce <= 0) {
        coalesce = null;
      }
    }

    // Count rows as they flow into the write (action materializes this lineage)
    Dataset<Row> toWrite = trackMetrics(input, transformMeta, SparkNativeMetrics.Role.OUTPUT);
    SparkFileIoSupport.writeDataset(
        toWrite, format, path, saveMode, options, partitionColumns, coalesce);

    // Leaf marker: write already ran; avoid re-triggering via a later count()
    StructType emptySchema =
        new StructType().add("_spark_hop_written", DataTypes.BooleanType, false);
    Dataset<Row> leaf = spark.createDataFrame(new ArrayList<>(), emptySchema);
    transformDatasetMap.put(transformMeta.getName(), leaf);

    log.logBasic(
        "Handled Spark File Output : "
            + transformMeta.getName()
            + " format="
            + format
            + " mode="
            + saveMode
            + " path="
            + path);
  }
}
