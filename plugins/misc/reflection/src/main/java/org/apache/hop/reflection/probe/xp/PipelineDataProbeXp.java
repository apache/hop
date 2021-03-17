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
 *
 */

package org.apache.hop.reflection.probe.xp;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.RowProducer;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transform.TransformMetaDataCombi;
import org.apache.hop.reflection.pipeline.xp.PipelineStartLoggingXp;
import org.apache.hop.reflection.probe.meta.DataProbeLocation;
import org.apache.hop.reflection.probe.meta.PipelineProbe;
import org.apache.hop.reflection.probe.transform.PipelineDataProbe;

import java.util.List;

@ExtensionPoint(
    id = "PipelineDataProbeXp",
    extensionPointId = "PipelineStartThreads",
    description = "Before the start of a pipeline, after init, attach the data probes needed")
public class PipelineDataProbeXp implements IExtensionPoint<Pipeline> {

  public static final String PIPELINE_DATA_PROBE_FLAG = "PipelineDataProbeActive";

  @Override
  public void callExtensionPoint(ILogChannel log, IVariables variables, Pipeline pipeline)
      throws HopException {

    // Prevent recursive data probing of the pipeline
    //
    if (pipeline.getExtensionDataMap().get(PIPELINE_DATA_PROBE_FLAG) != null) {
      return;
    }

    IHopMetadataProvider metadataProvider = pipeline.getMetadataProvider();
    IHopMetadataSerializer<PipelineProbe> serializer =
        metadataProvider.getSerializer(PipelineProbe.class);
    List<PipelineProbe> pipelineDataProbes = serializer.loadAll();

    for (PipelineProbe pipelineProbe : pipelineDataProbes) {
      handlePipelineProbe(log, pipelineProbe, pipeline, variables);
    }
  }

  private void handlePipelineProbe(
      final ILogChannel log,
      final PipelineProbe pipelineProbe,
      final IPipelineEngine<PipelineMeta> pipeline,
      final IVariables variables)
      throws HopException {
    try {

      // See if we need to do anything at all...
      //
      if (!pipelineProbe.isEnabled()) {
        return;
      }

      // Load the pipeline filename specified in the Pipeline Probe object...
      //
      final String probingPipelineFilename = variables.resolve(pipelineProbe.getPipelineFilename());

      // See if the file exists...
      FileObject probingFileObject = HopVfs.getFileObject(probingPipelineFilename);
      if (!probingFileObject.exists()) {
        log.logBasic(
            "WARNING: The Pipeline Probe pipeline file '"
                + probingPipelineFilename
                + "' couldn't be found to execute.");
        return;
      }

      // Try all data probe combinations to see if any fits...
      //
      for (DataProbeLocation dataProbeLocation : pipelineProbe.getDataProbeLocations()) {

        // If the parent pipeline matches the probe location and the transform exists
        // execute a probing pipeline...
        //
        if (probeLocationExists(variables, dataProbeLocation, pipeline)) {
          // Execute a probing pipeline in the background.
          // We do not wait for the pipeline to finish!
          //
          executeProbingPipeline(
              pipelineProbe, dataProbeLocation, probingPipelineFilename, pipeline, variables);
        }
      }

    } catch (Exception e) {
      pipeline.stopAll();
      throw new HopException(
          "Error handling Pipeline Probe metadata object '"
              + pipelineProbe.getName()
              + "' at the start of pipeline: "
              + pipeline,
          e);
    }
  }

  private boolean probeLocationExists( IVariables variables, DataProbeLocation dataProbeLocation, IPipelineEngine<PipelineMeta> pipeline ) throws HopException {

    // Not saved to a file, let's not probe.
    //
    if ( StringUtils.isEmpty(pipeline.getFilename())) {
      return false;
    }

    // Match filenames...
    //
    FileObject parentFileObject = HopVfs.getFileObject(pipeline.getFilename());
    String parentFilename = parentFileObject.getName().getPath();

    FileObject locationFileObject = HopVfs.getFileObject( variables.resolve(dataProbeLocation.getSourcePipelineFilename()) );
    String locationFilename = locationFileObject.getName().getPath();
    if (!parentFilename.equals(locationFilename)) {
      // No match
      return false;
    }

    List<IEngineComponent> componentCopies = pipeline.getComponentCopies( dataProbeLocation.getSourceTransformName() );
    if (componentCopies==null || componentCopies.isEmpty()) {
      return false;
    }

    return true;
  }

  /** Execute a probing pipeline for the current pipeline.
   *  Add a listener to the transform copies.
   *  Send the data to the PipelineDataProbe transform(s) in the probing pipeline
   *
   * @param pipelineProbe
   * @param dataProbeLocation
   * @param loggingPipelineFilename The pipeline to start for the location
   * @param pipeline The parent pipeline to listen to
   * @param variables
   * @throws HopException
   */
  private synchronized void executeProbingPipeline(
      PipelineProbe pipelineProbe,
      DataProbeLocation dataProbeLocation,
      String loggingPipelineFilename,
      IPipelineEngine<PipelineMeta> pipeline,
      IVariables variables)
      throws HopException {

    PipelineMeta probingPipelineMeta =
        new PipelineMeta(loggingPipelineFilename, pipeline.getMetadataProvider(), true, variables);

    // Create a local pipeline engine...
    //
    LocalPipelineEngine probingPipeline =
        new LocalPipelineEngine(probingPipelineMeta, variables, pipeline);

    // Flag it as a probing and logging pipeline so we don't try to probe or log ourselves...
    //
    probingPipeline.getExtensionDataMap().put(PIPELINE_DATA_PROBE_FLAG, "Y");
    probingPipeline.getExtensionDataMap().put( PipelineStartLoggingXp.PIPELINE_LOGGING_FLAG, "Y");

    // Only log errors
    //
    probingPipeline.setLogLevel(LogLevel.ERROR);
    probingPipeline.prepareExecution();

    List<IEngineComponent> componentCopies = pipeline.getComponentCopies( dataProbeLocation.getSourceTransformName() );
    for (IEngineComponent componentCopy : componentCopies) {

      // We need to send rows from this component copy to
      // all instances of the PipelineDataProbe transform
      //
      for (TransformMetaDataCombi combi : probingPipeline.getTransforms()) {
        if (combi.transform instanceof PipelineDataProbe) {
          // Give the transform a bit more information to work with...
          //
          PipelineDataProbe pipelineDataProbe = (PipelineDataProbe) combi.transform;
          pipelineDataProbe.setSourcePipelineName(pipeline.getPipelineMeta().getName());
          pipelineDataProbe.setSourceTransformLogChannelId(pipeline.getLogChannelId());
          pipelineDataProbe.setSourceTransformName(componentCopy.getName());
          pipelineDataProbe.setSourceTransformCopy(componentCopy.getCopyNr());

          try {
            final RowProducer rowProducer = probingPipeline.addRowProducer( combi.transformName, combi.copy );

            // For every copy of the component, add an input row set to the parent pipeline...
            //
            componentCopy.addRowListener(
              new RowAdapter() {
                @Override
                public void rowWrittenEvent(IRowMeta rowMeta, Object[] row)
                  throws HopTransformException {
                  // Pass this row to the row producer...
                  //
                  rowProducer.putRow( rowMeta, row );
                }
              });

            // If the pipeline we're the transform is and we can safely stop streaming...
            //
            pipeline.addExecutionFinishedListener( ( pe ) -> {
              rowProducer.finished();
            } );

          } catch ( HopException e ) {
            throw new HopTransformException("Error adding row producer to transform '"+combi.transformName+"'", e);
          }
        }
      }
    }

    // Execute the logging pipeline to save the logging information
    //
    probingPipeline.startThreads();

    // We'll not wait around until this is finished...
    // The pipeline should stop automatically when the parent does
    //
    pipeline.addExecutionStoppedListener( e->probingPipeline.stopAll() );

  }
}
