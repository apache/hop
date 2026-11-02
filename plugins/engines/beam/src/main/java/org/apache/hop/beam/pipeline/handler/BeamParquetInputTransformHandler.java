/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.beam.pipeline.handler;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.coder.HopRowCoder;
import org.apache.hop.beam.core.fn.EmptyToNullRowFn;
import org.apache.hop.beam.core.fn.ParquetColumnMapping;
import org.apache.hop.beam.core.fn.ReadParquetFileFn;
import org.apache.hop.beam.engines.IBeamPipelineEngineRunConfiguration;
import org.apache.hop.beam.pipeline.IBeamPipelineTransformHandler;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.JsonRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.parquet.transforms.input.ParquetField;
import org.apache.hop.parquet.transforms.input.ParquetInputMeta;
import org.apache.hop.parquet.transforms.output.ParquetOutput;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Issue #2357: Parquet file input on Beam.
 *
 * <p>Each incoming row names a file. The file is opened with Beam file systems, so a local path or
 * a {@code file://}, {@code gs://}, {@code s3://} or {@code hdfs://} URI works. The configured
 * columns are appended to that row. The field list or a metadata filename is required, because the
 * output row layout has to exist when the graph is built. Sending a null row when the input is
 * empty needs a bounded collection in the global window.
 */
public class BeamParquetInputTransformHandler extends BeamBaseTransformHandler
    implements IBeamPipelineTransformHandler {

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

    if (input == null || previousTransforms == null || previousTransforms.isEmpty()) {
      throw new HopException(
          "Parquet input '" + transformMeta.getName() + "' needs one incoming transform");
    }
    ParquetInputMeta meta = new ParquetInputMeta();
    loadTransformMetadata(meta, transformMeta, metadataProvider, pipelineMeta);
    String filenameField = variables.resolve(meta.getFilenameField());
    if (Utils.isEmpty(filenameField)) {
      throw new HopException(
          "Parquet input '" + transformMeta.getName() + "' has no filename field");
    }
    if (rowMeta == null || rowMeta.indexOfValue(filenameField) < 0) {
      throw new HopException(
          "Parquet input '"
              + transformMeta.getName()
              + "' cannot find filename field '"
              + filenameField
              + "'");
    }

    List<org.apache.hop.parquet.transforms.output.ParquetField> schemaFields = new ArrayList<>();
    List<IValueMeta> valueMetas = new ArrayList<>();
    List<ParquetColumnMapping> columns = new ArrayList<>();
    if (meta.getFields() != null && !meta.getFields().isEmpty()) {
      for (ParquetField field : meta.getFields()) {
        IValueMeta valueMeta = field.createValueMeta();
        String column =
            Utils.isEmpty(field.getSourceField()) ? field.getTargetField() : field.getSourceField();
        schemaFields.add(new org.apache.hop.parquet.transforms.output.ParquetField(column, column));
        valueMetas.add(valueMeta);
        columns.add(new ParquetColumnMapping(column, valueMeta.getType()));
      }
    } else if (!Utils.isEmpty(meta.getMetadataFilename())) {
      IRowMeta fileRow =
          ParquetInputMeta.extractRowMeta(variables, variables.resolve(meta.getMetadataFilename()));
      for (int i = 0; i < fileRow.size(); i++) {
        IValueMeta valueMeta = fileRow.getValueMeta(i);
        schemaFields.add(
            new org.apache.hop.parquet.transforms.output.ParquetField(
                valueMeta.getName(), valueMeta.getName()));
        valueMetas.add(valueMeta);
        columns.add(new ParquetColumnMapping(valueMeta.getName(), valueMeta.getType()));
      }
    }
    if (columns.isEmpty()) {
      throw new HopException(
          "Parquet input '"
              + transformMeta.getName()
              + "' needs a field list or a metadata filename on Beam");
    }

    Schema schema = ParquetOutput.avroSchema(schemaFields, valueMetas);
    PCollection<HopRow> read =
        input
            .apply(
                transformMeta.getName() + " Read Parquet",
                ParDo.of(
                    new ReadParquetFileFn(
                        transformMeta.getName(),
                        JsonRowMeta.toJson(rowMeta),
                        filenameField,
                        schema.toString(),
                        columns)))
            .setCoder(new HopRowCoder());

    if (meta.isSendingNullsRowWhenEmpty()) {
      if (!(input.getWindowingStrategy().getWindowFn() instanceof GlobalWindows)
          || input.isBounded() != IsBounded.BOUNDED) {
        throw new HopException(
            "Parquet input '"
                + transformMeta.getName()
                + "' can send a null row when empty only for a bounded input in the global window. "
                + "Count.globally() fails while the graph is built for a streaming or otherwise windowed input.");
      }
      int width = rowMeta.size() + columns.size();
      PCollection<HopRow> whenEmpty =
          input
              .apply(transformMeta.getName() + " Count", Count.globally())
              .apply(transformMeta.getName() + " Null row", ParDo.of(new EmptyToNullRowFn(width)))
              .setCoder(new HopRowCoder());
      read =
          PCollectionList.of(read)
              .and(whenEmpty)
              .apply(transformMeta.getName() + " Empty row", Flatten.pCollections())
              .setCoder(new HopRowCoder());
    }

    transformCollectionMap.put(transformMeta.getName(), read);
    log.logBasic(
        "Handled transform (PARQUET INPUT) : "
            + transformMeta.getName()
            + ", filename field "
            + filenameField);
  }
}
