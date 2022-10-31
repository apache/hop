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

package org.apache.hop.beam.core.transform;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowBuffer;
import org.apache.hop.core.row.RowMetaBuilder;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.execution.ExecutionDataBuilder;
import org.apache.hop.execution.ExecutionDataSetMeta;
import org.apache.hop.execution.ExecutionInfoLocation;
import org.apache.hop.execution.ExecutionType;
import org.apache.hop.execution.profiling.ExecutionDataProfile;
import org.apache.hop.execution.sampler.ExecutionDataSamplerMeta;
import org.apache.hop.execution.sampler.IExecutionDataSampler;
import org.apache.hop.execution.sampler.IExecutionDataSamplerStore;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.SingleThreadedPipelineExecutor;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transform.stream.IStream;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

public abstract class TransformBaseFn extends DoFn<HopRow, HopRow> {

  protected static final Logger LOG = LoggerFactory.getLogger(TransformBaseFn.class);

  protected String transformName;
  protected String parentLogChannelId;
  protected String runConfigName;
  protected String dataSamplersJson;

  protected transient SingleThreadedPipelineExecutor executor;

  protected transient ExecutionInfoLocation executionInfoLocation;
  protected transient List<IExecutionDataSampler> dataSamplers;
  protected transient List<IExecutionDataSamplerStore> dataSamplerStores;
  protected transient Timer executionInfoTimer;
  protected transient BoundedWindow batchWindow;

  public TransformBaseFn(String parentLogChannelId, String runConfigName, String dataSamplersJson) {
    this.parentLogChannelId = parentLogChannelId;
    this.runConfigName = runConfigName;
    this.dataSamplersJson = dataSamplersJson;
  }

  protected void sendSamplesToLocation(boolean finished) throws HopException {
    if (executor==null || executor.getPipeline()==null) {
      return;
    }

    String logChannelId = executor.getPipeline().getLogChannelId();

    ExecutionDataBuilder dataBuilder =
        ExecutionDataBuilder.of()
            .withOwnerId(logChannelId)
            .withParentId(parentLogChannelId)
            .withExecutionType(ExecutionType.Transform)
            .withCollectionDate(new Date())
            .withFinished(finished);
    for (IExecutionDataSamplerStore store : dataSamplerStores) {
      dataBuilder.addDataSets(store.getSamples()).addSetMeta(store.getSamplesMetadata());
    }
    // Add some metadata about the transform being sampled
    //
    ITransform transform = executor.getPipeline().findRunThread(transformName);

    dataBuilder.addSetMeta(
        logChannelId,
        new ExecutionDataSetMeta(
            logChannelId,
            logChannelId,
            transformName,
            logChannelId,
            transformName + "." + logChannelId + " (Metrics)"));
    dataBuilder.addDataSet(
        logChannelId,
        new RowBuffer(
            new RowMetaBuilder().addString("metric").addInteger("value").build(),
            List.of(
                new Object[] {Pipeline.METRIC_NAME_INPUT, transform.getLinesInput()},
                new Object[] {Pipeline.METRIC_NAME_OUTPUT, transform.getLinesOutput()},
                new Object[] {Pipeline.METRIC_NAME_READ, transform.getLinesRead()},
                new Object[] {Pipeline.METRIC_NAME_WRITTEN, transform.getLinesWritten()},
                new Object[] {Pipeline.METRIC_NAME_REJECTED, transform.getLinesRejected()},
                new Object[] {Pipeline.METRIC_NAME_ERROR, transform.getErrors()})));

    // Register this data in the execution information location
    //
    executionInfoLocation.getExecutionInfoLocation().registerData(dataBuilder.build());
  }

  protected void lookupExecutionInformation(
      IVariables variables, IHopMetadataProvider metadataProvider)
      throws HopException, JsonProcessingException {
    executionInfoLocation = null;
    dataSamplers = new ArrayList<>();
    dataSamplerStores = new ArrayList<>();
    PipelineRunConfiguration runConf =
        metadataProvider.getSerializer(PipelineRunConfiguration.class).load(runConfigName);
    if (runConf != null) {
      String locationName = runConf.getExecutionInfoLocationName();
      if (StringUtils.isNotEmpty(locationName)) {
        ExecutionInfoLocation location =
            metadataProvider.getSerializer(ExecutionInfoLocation.class).load(locationName);
        if (location != null) {
          // See if we have a data profile.
          // If not there's nothing we have to do in this transform really
          //
          String profileName = runConf.getExecutionDataProfileName();
          if (StringUtils.isNotEmpty(profileName)) {
            ExecutionDataProfile dataProfile =
                metadataProvider.getSerializer(ExecutionDataProfile.class).load(profileName);
            if (dataProfile != null) {
              dataSamplers.addAll(dataProfile.getSamplers());
            }

            // Also inflate the samplers JSON
            //
            if (StringUtils.isNotEmpty(dataSamplersJson)) {
              IExecutionDataSampler<?>[] extraSamplers =
                  new ObjectMapper().readValue(dataSamplersJson, IExecutionDataSampler[].class);
              dataSamplers.addAll(Arrays.asList(extraSamplers));
            }

            executionInfoLocation = location;

            // Initialize the location
            //
            executionInfoLocation
                .getExecutionInfoLocation()
                .initialize(variables, metadataProvider);
          }
        }
      }
    }
  }

  protected void attachExecutionSamplersToOutput(
      IVariables variables,
      String transformName,
      String logChannelId,
      IRowMeta inputRowMeta,
      IRowMeta outputRowMeta,
      ITransform transform)
      throws HopTransformException {
    // If we're sending execution information to a location we should do it differently from a
    // Beam node.
    // We're only going to go through the effort if we actually have any rows to sample.
    //
    if (executionInfoLocation != null && !dataSamplers.isEmpty()) {

      // The sampler metadata.
      //
      ExecutionDataSamplerMeta dataSamplerMeta =
          new ExecutionDataSamplerMeta(transformName, logChannelId, logChannelId, false, false);

      // Create a sampler store for every sampler
      //
      for (IExecutionDataSampler<?> dataSampler : dataSamplers) {
        IExecutionDataSamplerStore dataSamplerStore =
            dataSampler.createSamplerStore(dataSamplerMeta);
        dataSamplerStore.init(variables, inputRowMeta, outputRowMeta);
        dataSamplerStores.add(dataSamplerStore);
      }

      // We always only have a single transform copy here.
      //
      transform.addRowListener(
          new RowAdapter() {
            @Override
            public void rowWrittenEvent(IRowMeta rowMeta, Object[] row)
                throws HopTransformException {
              for (int s = 0; s < dataSamplers.size(); s++) {
                IExecutionDataSampler sampler = dataSamplers.get(s);
                IExecutionDataSamplerStore store = dataSamplerStores.get(s);
                try {
                  sampler.sampleRow(store, IStream.StreamType.OUTPUT, rowMeta, row);
                } catch (HopException e) {
                  throw new RuntimeException("Error sampling row", e);
                }
              }
            }
          });

      // We want to send the data collected from the execution data stores over to the
      // location on a regular
      // basis.  To do so we'll add a timer here.
      //
      TimerTask task =
          new TimerTask() {
            @Override
            public void run() {
              try {
                sendSamplesToLocation(false);
              } catch (HopException e) {
                LOG.error("Error sending transform samples to location (non-fatal)", e);
              }
            }
          };
      executionInfoTimer = new Timer(transformName);
      executionInfoTimer.schedule(
          task,
          Const.toLong(executionInfoLocation.getDataLoggingDelay(), 5000L),
          Const.toLong(executionInfoLocation.getDataLoggingInterval(), 10000L));
    }
  }

  protected interface TupleOutputContext<T> {
    void output(TupleTag<T> tupleTag, T output);
  }

  protected class TransformProcessContext implements TupleOutputContext<HopRow> {

    private DoFn.ProcessContext context;

    public TransformProcessContext(DoFn.ProcessContext processContext) {
      this.context = processContext;
    }

    @Override
    public void output(TupleTag<HopRow> tupleTag, HopRow output) {
      context.output(tupleTag, output);
    }
  }

  protected class TransformFinishBundleContext implements TupleOutputContext<HopRow> {

    private DoFn.FinishBundleContext context;
    private BoundedWindow batchWindow;

    public TransformFinishBundleContext(
        DoFn.FinishBundleContext context, BoundedWindow batchWindow) {
      this.context = context;
      this.batchWindow = batchWindow;
    }

    @Override
    public void output(TupleTag<HopRow> tupleTag, HopRow output) {
      context.output(tupleTag, output, Instant.now(), batchWindow);
    }
  }
}
