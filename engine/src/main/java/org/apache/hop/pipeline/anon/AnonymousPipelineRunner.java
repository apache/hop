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

package org.apache.hop.pipeline.anon;

import java.io.InputStream;
import java.io.StringReader;
import java.util.List;
import lombok.Getter;
import org.apache.commons.io.input.ReaderInputStream;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engine.PipelineEngineFactory;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.engines.local.LocalPipelineRunConfiguration;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.RowAdapter;

@Getter
public class AnonymousPipelineRunner {

  public static AnonymousPipelineResults executePipeline(
      PipelineMeta pipelineMeta,
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      String resultsTransformName)
      throws HopException {
    try {
      PipelineRunConfiguration runConfiguration =
          new PipelineRunConfiguration(
              "anonymous",
              "Internal pipeline execution run configuration",
              null,
              List.of(),
              new LocalPipelineRunConfiguration(),
              null,
              false);
      runConfiguration.getEngineRunConfiguration().setEnginePluginId("Local");

      // To avoid classpath issues we need to serialize the pipeline metadata to XML and re-inflate
      // it.
      // When we do the transforms will be re-created with the correct parent classpath.
      //
      PipelineMeta meta;
      String xml = pipelineMeta.getXml(variables);
      try (StringReader stringReader = new StringReader(xml)) {
        try (InputStream inputStream =
            ReaderInputStream.builder()
                .setCharset(Const.XML_ENCODING)
                .setReader(stringReader)
                .get()) {
          meta = new PipelineMeta(inputStream, metadataProvider, variables);
          meta.setMetadataProvider(metadataProvider);
          LocalPipelineEngine pipeline =
              (LocalPipelineEngine)
                  PipelineEngineFactory.createPipelineEngine(runConfiguration, meta);
          pipeline.setParent(new LoggingObject("AnonymousPipelineRunner"));
          pipeline.setMetadataProvider(metadataProvider);

          final AnonymousPipelineResults results = new AnonymousPipelineResults();

          // Run the pipeline
          pipeline.prepareExecution();
          ITransform thread = pipeline.getRunThread(resultsTransformName, 0);
          if (thread == null) {
            throw new HopException(
                "Unable to find transform '" + resultsTransformName + "' to get result rows from");
          }
          thread.addRowListener(
              new RowAdapter() {
                @Override
                public void rowWrittenEvent(IRowMeta rowMeta, Object[] row) {
                  // Collect the result rows
                  results.setResultRowMeta(rowMeta);
                  results.getResultRows().add(row);
                }
              });

          pipeline.startThreads();
          pipeline.waitUntilFinished();

          results.setPipeline(pipeline);
          results.setResult(pipeline.getResult());

          return results;
        }
      }
    } catch (Exception e) {
      throw new HopException("Error running anonymous local pipeline", e);
    }
  }
}
