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

import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hop.avro.transforms.avroinput.AvroFileInputMeta;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.coder.HopRowCoder;
import org.apache.hop.beam.core.fn.ReadAvroFileFn;
import org.apache.hop.beam.engines.IBeamPipelineEngineRunConfiguration;
import org.apache.hop.beam.pipeline.IBeamPipelineTransformHandler;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.JsonRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Issue #2358: Avro file input on Beam.
 *
 * <p>Each incoming row names a file. The file is read on one worker and each record is appended to
 * that row as an Avro record field. The file keeps the schema stored in its header.
 */
public class BeamAvroInputTransformHandler extends BeamBaseTransformHandler
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
          "Avro file input '" + transformMeta.getName() + "' needs one incoming transform");
    }

    AvroFileInputMeta meta = new AvroFileInputMeta();
    loadTransformMetadata(meta, transformMeta, metadataProvider, pipelineMeta);

    String filenameField = variables.resolve(meta.getDataFilenameField());
    if (Utils.isEmpty(filenameField)) {
      throw new HopException(
          "Avro file input '" + transformMeta.getName() + "' has no filename field");
    }
    if (rowMeta == null || rowMeta.indexOfValue(filenameField) < 0) {
      throw new HopException(
          "Avro file input '"
              + transformMeta.getName()
              + "' cannot find filename field '"
              + filenameField
              + "'");
    }

    int rowsLimit = -1;
    if (!Utils.isEmpty(meta.getRowsLimit())) {
      rowsLimit = Const.toIntExpanded(variables.resolve(meta.getRowsLimit()), -1);
    }

    PCollection<HopRow> read =
        input
            .apply(
                transformMeta.getName() + " Read Avro",
                ParDo.of(
                    new ReadAvroFileFn(
                        transformMeta.getName(),
                        JsonRowMeta.toJson(rowMeta),
                        filenameField,
                        rowsLimit)))
            .setCoder(new HopRowCoder());

    transformCollectionMap.put(transformMeta.getName(), read);
    log.logBasic(
        "Handled transform (AVRO FILE INPUT) : "
            + transformMeta.getName()
            + ", filename field "
            + filenameField);
  }
}
