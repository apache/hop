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

package org.apache.hop.pipeline.engines.local;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.IExtensionData;
import org.apache.hop.core.Result;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.map.DatabaseConnectionMap;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.parameters.INamedParameters;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.execution.*;
import org.apache.hop.execution.profiling.ExecutionDataProfile;
import org.apache.hop.execution.sampler.ExecutionDataSamplerMeta;
import org.apache.hop.execution.sampler.IExecutionDataSampler;
import org.apache.hop.execution.sampler.IExecutionDataSamplerStore;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.IExecutionFinishedListener;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.IPipelineEngineRunConfiguration;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engine.PipelineEngineCapabilities;
import org.apache.hop.pipeline.engine.PipelineEnginePlugin;
import org.apache.hop.pipeline.transform.IRowListener;
import org.apache.hop.pipeline.transform.TransformMetaDataCombi;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

@PipelineEnginePlugin(
    id = "Local",
    name = "Hop local pipeline engine",
    description = "Executes your pipeline locally in a multi-threaded fashion")
public class LocalPipelineEngine extends Pipeline implements IPipelineEngine<PipelineMeta> {

  private PipelineEngineCapabilities engineCapabilities = new LocalPipelineEngineCapabilities();

  private ExecutionInfoLocation executionInfoLocation;
  private Timer transformExecutionInfoTimer;

  private List<IExecutionDataSamplerStore> samplerStores;

  public LocalPipelineEngine() {
    super();
    setDefaultRunConfiguration();
  }

  public LocalPipelineEngine(PipelineMeta pipelineMeta) {
    super(pipelineMeta);
    setDefaultRunConfiguration();
  }

  public LocalPipelineEngine(
      PipelineMeta pipelineMeta, IVariables variables, ILoggingObject parent) {
    super(pipelineMeta, variables, parent);
    setDefaultRunConfiguration();
  }

  public <Parent extends IVariables & INamedParameters> LocalPipelineEngine(
      Parent parent, String name, String filename, IHopMetadataProvider metadataProvider)
      throws HopException {
    super(parent, name, filename, metadataProvider);
    setDefaultRunConfiguration();
  }

  @Override
  public IPipelineEngineRunConfiguration createDefaultPipelineEngineRunConfiguration() {
    return new LocalPipelineRunConfiguration();
  }

  private void setDefaultRunConfiguration() {
    setPipelineRunConfiguration(
        new PipelineRunConfiguration(
            "local", "", "", new ArrayList<>(), createDefaultPipelineEngineRunConfiguration()));
  }

  @Override
  public void prepareExecution() throws HopException {

    if (!(pipelineRunConfiguration.getEngineRunConfiguration()
        instanceof LocalPipelineRunConfiguration)) {
      throw new HopException(
          "A local pipeline execution expects a local pipeline configuration, not an instance of class "
              + pipelineRunConfiguration.getEngineRunConfiguration().getClass().getName());
    }

    LocalPipelineRunConfiguration config =
        (LocalPipelineRunConfiguration) pipelineRunConfiguration.getEngineRunConfiguration();

    int sizeRowsSet = Const.toInt(resolve(config.getRowSetSize()), Const.ROWS_IN_ROWSET);
    setRowSetSize(sizeRowsSet);
    setSafeModeEnabled(config.isSafeModeEnabled());
    setSortingTransformsTopologically(config.isSortingTransformsTopologically());
    setGatheringMetrics(config.isGatheringMetrics());
    setFeedbackShown(config.isFeedbackShown());
    setFeedbackSize(Const.toInt(resolve(config.getFeedbackSize()), Const.ROWS_UPDATE));

    // See if we need to enable transactions...
    //
    IExtensionData parentExtensionData = getParentPipeline();
    if (parentExtensionData == null) {
      parentExtensionData = getParentWorkflow();
    }
    String connectionGroup = null;
    if (parentExtensionData != null) {
      connectionGroup =
          (String) parentExtensionData.getExtensionDataMap().get(Const.CONNECTION_GROUP);
    }

    // Create a new transaction group?
    //
    if (config.isTransactional() && connectionGroup == null) {
      // Store a value in the parent...
      //
      connectionGroup = getPipelineMeta().getName() + " - " + UUID.randomUUID();

      // We also need to commit/rollback at the end of this pipeline...
      // We only do this when we created a new group.  Never in a child
      //
      addExecutionFinishedListener(
          (IExecutionFinishedListener<IPipelineEngine>)
              pipeline -> {
                String group = (String) pipeline.getExtensionDataMap().get(Const.CONNECTION_GROUP);
                List<Database> databases = DatabaseConnectionMap.getInstance().getDatabases(group);
                Result result = pipeline.getResult();
                for (Database database : databases) {
                  // All fine?  Commit!
                  //
                  try {
                    if (result.getResult() && !result.isStopped() && result.getNrErrors() == 0) {
                      try {
                        database.commit(true);
                        pipeline
                            .getLogChannel()
                            .logBasic(
                                "All transactions of database connection '"
                                    + database.getDatabaseMeta().getName()
                                    + "' were committed at the end of the pipeline!");
                      } catch (HopDatabaseException e) {
                        throw new HopException(
                            "Error committing database connection "
                                + database.getDatabaseMeta().getName(),
                            e);
                      }
                    } else {
                      try {
                        database.rollback(true);
                        pipeline
                            .getLogChannel()
                            .logBasic(
                                "All transactions of database connection '"
                                    + database.getDatabaseMeta().getName()
                                    + "' were rolled back at the end of the pipeline!");
                      } catch (HopDatabaseException e) {
                        throw new HopException(
                            "Error rolling back database connection "
                                + database.getDatabaseMeta().getName(),
                            e);
                      }
                    }
                  } finally {
                    // Always close connection!
                    try {
                      database.closeConnectionOnly();
                      pipeline
                          .getLogChannel()
                          .logDebug(
                              "Database connection '"
                                  + database.getDatabaseMeta().getName()
                                  + "' closed successfully!");
                    } catch (HopDatabaseException hde) {
                      pipeline
                          .getLogChannel()
                          .logError(
                              "Error disconnecting from database - closeConnectionOnly failed:"
                                  + Const.CR
                                  + hde.getMessage());
                      pipeline.getLogChannel().logError(Const.getStackTracker(hde));
                    }
                  }
                  // Definitely remove the connection reference the connections map
                  DatabaseConnectionMap.getInstance().removeConnection(group, null, database);
                }
              });
    }

    // Signal that we're dealing with a connection group
    // We'll have extension points to pass the connection group down the hierarchy: workflow -
    // action - pipeline - transform - database
    //
    if (connectionGroup != null && getExtensionDataMap() != null) {
      // Set the connection group for this pipeline
      //
      getExtensionDataMap().put(Const.CONNECTION_GROUP, connectionGroup);
    }

    super.prepareExecution();

    // Do the lookup of the execution information only once
    lookupExecutionInformationLocation();

    // Register the pipeline
    registerPipelineExecutionInformation();

    // Attach samplers to the transforms if needed
    //
    addTransformExecutionSamplers();

    //
  }

  /**
   * If needed, register this pipeline at the specified execution information location. The name of
   * the location is specified in the run configuration.
   *
   * @throws HopException In case something goes wrong
   */
  public void registerPipelineExecutionInformation() throws HopException {
    if (executionInfoLocation != null) {
      // Register the execution at this location
      // This adds metadata, variables, parameters, ...
      executionInfoLocation
          .getExecutionInfoLocation()
          .registerExecution(ExecutionBuilder.fromExecutor(this).build());
    }
  }

  public void addTransformExecutionSamplers() throws HopException {
    if (executionInfoLocation == null) {
      return;
    }
    // No data profile to work with
    //
    ExecutionDataProfile profile = executionInfoLocation.getExecutionDataProfile();
    if (profile == null) {
      return;
    }

    // Nothing to sample
    //
    if (profile.getSamplers().isEmpty()) {
      return;
    }

    samplerStores = new ArrayList<>();

    // Attach the samplers to the transforms.
    //
    for (IExecutionDataSampler<?> sampler : profile.getSamplers()) {
      for (TransformMetaDataCombi combi : getTransforms()) {
        // Create a sampler store for the sampler
        //
        boolean firstTransform = pipelineMeta.findPreviousTransforms(combi.transformMeta).isEmpty();
        boolean lastTransform = pipelineMeta.findNextTransforms(combi.transformMeta).isEmpty();

        ExecutionDataSamplerMeta samplerMeta =
            new ExecutionDataSamplerMeta(
                combi.transformName,
                combi.copy,
                combi.transform.getLogChannelId(),
                firstTransform,
                lastTransform);

        IExecutionDataSamplerStore samplerStore = sampler.createSamplerStore(samplerMeta);
        IRowMeta inputRowMeta = getPipelineMeta().getPrevTransformFields(this, combi.transformMeta);
        IRowMeta outputRowMeta = getPipelineMeta().getTransformFields(this, combi.transformMeta);
        samplerStore.init(this, inputRowMeta, outputRowMeta);
        IRowListener rowListener = samplerStore.createRowListener(sampler);

        // Keep the stores and samplers safe. We'll need them later on.
        //
        samplerStores.add(samplerStore);

        combi.transform.addRowListener(rowListener);
      }
    }

    // The sampling buffers will fill up automatically now during execution
    // We start the timer tasks which periodically send the data to the location in execute().
  }

  @Override
  public void startThreads() throws HopException {
    // Add a timer to send sampler information to the execution info location
    //
    startTransformExecutionInfoTimer();

    // Start execution of the pipeline
    //
    super.startThreads();

    // Make sure to kill the timer when this pipeline is finished.
    //
    super.addExecutionFinishedListener(engine -> stopTransformExecutionInfoTimer());
  }

  public void startTransformExecutionInfoTimer() throws HopException {
    if (executionInfoLocation == null) {
      return;
    }
    // No data profile to work with
    //
    ExecutionDataProfile profile = executionInfoLocation.getExecutionDataProfile();
    if (profile == null) {
      return;
    }

    // Nothing to sample
    //
    if (profile.getSamplers().isEmpty()) {
      return;
    }

    long delay = Const.toLong(resolve(executionInfoLocation.getDataLoggingDelay()), 2000L);
    long interval = Const.toLong(resolve(executionInfoLocation.getDataLoggingInterval()), 5000L);
    final AtomicInteger lastLogLineNr = new AtomicInteger(0);

    final IExecutionInfoLocation iLocation = executionInfoLocation.getExecutionInfoLocation();
    //
    TimerTask sampleTask =
        new TimerTask() {
          @Override
          public void run() {
            try {
              // Collect data from all the sampler stores.
              //
              ExecutionDataBuilder dataBuilder =
                  ExecutionDataBuilder.fromAllTransformData(
                      LocalPipelineEngine.this, samplerStores);

              // Send it to the location once
              //
              iLocation.registerData(dataBuilder.build());

              // Also update the pipeline execution state regularly
              //
              ExecutionState executionState =
                  ExecutionStateBuilder.fromExecutor(LocalPipelineEngine.this, lastLogLineNr.get())
                      .build();
              iLocation.updateExecutionState(executionState);
              if (executionState.getLastLogLineNr() != null) {
                lastLogLineNr.set(executionState.getLastLogLineNr());
              }
            } catch (Exception e) {
              throw new RuntimeException(
                  "Error registering execution info data from transforms at location "
                      + executionInfoLocation.getName(),
                  e);
            }
          }
        };

    // Schedule the task to run regularly
    //
    transformExecutionInfoTimer = new Timer();
    transformExecutionInfoTimer.schedule(sampleTask, delay, interval);
  }

  public void stopTransformExecutionInfoTimer() throws HopException {
    if (transformExecutionInfoTimer != null) {
      transformExecutionInfoTimer.cancel();
    }

    if (executionInfoLocation == null) {
      return;
    }

    IExecutionInfoLocation iLocation = executionInfoLocation.getExecutionInfoLocation();

    // Register the collected transform data for the last time
    //
    ExecutionDataBuilder dataBuilder =
        ExecutionDataBuilder.fromAllTransformData(LocalPipelineEngine.this, samplerStores);
    iLocation.registerData(dataBuilder.build());

    // Register one final last state of the pipeline
    //
    ExecutionState executionState =
        ExecutionStateBuilder.fromExecutor(LocalPipelineEngine.this, -1).build();
    iLocation.updateExecutionState(executionState);
  }

  /**
   * This method looks up the execution information location specified in the run configuration.
   *
   * @return The location or null
   * @throws HopException In case something fundamental went wrong.
   */
  public void lookupExecutionInformationLocation() throws HopException {
    String locationName = resolve(pipelineRunConfiguration.getExecutionInfoLocationName());
    if (StringUtils.isNotEmpty(locationName)) {
      ExecutionInfoLocation location =
          metadataProvider.getSerializer(ExecutionInfoLocation.class).load(locationName);
      if (location != null) {
        executionInfoLocation = location;
      } else {
        log.logError(
            "Execution information location '"
                + locationName
                + "' could not be found in the metadata");
      }
    }
  }

  /**
   * Gets engineCapabilities
   *
   * @return value of engineCapabilities
   */
  @Override
  public PipelineEngineCapabilities getEngineCapabilities() {
    return engineCapabilities;
  }

  /**
   * @param engineCapabilities The engineCapabilities to set
   */
  public void setEngineCapabilities(PipelineEngineCapabilities engineCapabilities) {
    this.engineCapabilities = engineCapabilities;
  }

  @Override
  public String getStatusDescription() {
    return super.getStatus();
  }
}
