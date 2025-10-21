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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
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
import org.apache.hop.core.util.ExecutorUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.execution.ExecutionBuilder;
import org.apache.hop.execution.ExecutionDataBuilder;
import org.apache.hop.execution.ExecutionInfoLocation;
import org.apache.hop.execution.ExecutionState;
import org.apache.hop.execution.ExecutionStateBuilder;
import org.apache.hop.execution.IExecutionInfoLocation;
import org.apache.hop.execution.profiling.ExecutionDataProfile;
import org.apache.hop.execution.sampler.ExecutionDataSamplerMeta;
import org.apache.hop.execution.sampler.IExecutionDataSampler;
import org.apache.hop.execution.sampler.IExecutionDataSamplerStore;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.IPipelineEngineRunConfiguration;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engine.PipelineEngineCapabilities;
import org.apache.hop.pipeline.engine.PipelineEnginePlugin;
import org.apache.hop.pipeline.transform.IRowListener;
import org.apache.hop.pipeline.transform.TransformMetaDataCombi;

@PipelineEnginePlugin(
    id = "Local",
    name = "Hop local pipeline engine",
    description = "Executes your pipeline locally in a multi-threaded fashion")
public class LocalPipelineEngine extends Pipeline implements IPipelineEngine<PipelineMeta> {

  private PipelineEngineCapabilities engineCapabilities = new LocalPipelineEngineCapabilities();

  private ExecutionInfoLocation executionInfoLocation;
  private Timer transformExecutionInfoTimer;
  private TimerTask transformExecutionInfoTimerTask;

  private Map<String, List<IExecutionDataSamplerStore>> samplerStoresMap;

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
            "local",
            "",
            "",
            new ArrayList<>(),
            createDefaultPipelineEngineRunConfiguration(),
            null,
            false));
  }

  @Override
  public void prepareExecution() throws HopException {

    if (!(pipelineRunConfiguration.getEngineRunConfiguration()
        instanceof LocalPipelineRunConfiguration config)) {
      throw new HopException(
          "A local pipeline execution expects a local pipeline configuration, not an instance of class "
              + pipelineRunConfiguration.getEngineRunConfiguration().getClass().getName());
    }

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
          pipeline -> {
            String group = (String) pipeline.getExtensionDataMap().get(Const.CONNECTION_GROUP);
            List<Database> databases = DatabaseConnectionMap.getInstance().getDatabases(group);
            Result result = pipeline.getResult();
            for (Database database : databases) {
              // All fine?  Commit!
              //
              try {
                if (result.isResult() && !result.isStopped() && result.getNrErrors() == 0) {
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

      // Also register an execution node for every transform
      //
      for (TransformMetaDataCombi c : getTransforms()) {
        executionInfoLocation
            .getExecutionInfoLocation()
            .registerExecution(ExecutionBuilder.fromTransform(this, c.transform).build());
      }
    }
  }

  public void addTransformExecutionSamplers() throws HopException {
    if (executionInfoLocation == null) {
      return;
    }
    // No data profile to work with
    //
    String profileName = resolve(pipelineRunConfiguration.getExecutionDataProfileName());
    if (StringUtils.isEmpty(profileName)) {
      return;
    }

    ExecutionDataProfile profile =
        metadataProvider.getSerializer(ExecutionDataProfile.class).load(profileName);
    if (profile == null) {
      log.logError("Unable to find data profile '" + profileName + "' (non-fatal)");
      return;
    }

    List<IExecutionDataSampler> samplers = new ArrayList<>();
    samplers.addAll(profile.getSamplers());
    samplers.addAll(dataSamplers);

    // Nothing to sample
    //
    if (samplers.isEmpty()) {
      return;
    }

    samplerStoresMap = new HashMap<>();

    // Attach all the samplers to all the transform copies.
    //
    for (TransformMetaDataCombi combi : getTransforms()) {
      List<IExecutionDataSamplerStore> samplerStores = new ArrayList<>();
      samplerStoresMap.put(combi.transformName, samplerStores);

      for (IExecutionDataSampler<?> sampler : samplers) {
        // Create a sampler store for the sampler
        //
        boolean firstTransform = pipelineMeta.findPreviousTransforms(combi.transformMeta).isEmpty();
        boolean lastTransform = pipelineMeta.findNextTransforms(combi.transformMeta).isEmpty();

        ExecutionDataSamplerMeta samplerMeta =
            new ExecutionDataSamplerMeta(
                combi.transformName,
                Integer.toString(combi.copy),
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
    // We do this with an extension point.
  }

  @Override
  public void waitUntilFinished() {
    super.waitUntilFinished();
  }

  public void startTransformExecutionInfoTimer() throws HopException {
    if (executionInfoLocation == null) {
      return;
    }

    final ExecutionDataProfile dataProfile;

    // No data profile to work with
    //
    String profileName = resolve(pipelineRunConfiguration.getExecutionDataProfileName());
    if (StringUtils.isNotEmpty(profileName)) {
      dataProfile = metadataProvider.getSerializer(ExecutionDataProfile.class).load(profileName);
      if (dataProfile == null) {
        log.logError("Unable to find data profile '" + profileName + "' (non-fatal)");
      }
    } else {
      dataProfile = null;
    }

    long delay = Const.toLong(resolve(executionInfoLocation.getDataLoggingDelay()), 2000L);
    long interval = Const.toLong(resolve(executionInfoLocation.getDataLoggingInterval()), 5000L);

    final IExecutionInfoLocation iLocation = executionInfoLocation.getExecutionInfoLocation();
    //
    TimerTask transformExecutionInfoTimerTask =
        new TimerTask() {
          @Override
          public void run() {
            try {
              // Collect data from all the sampler stores.
              //
              if (dataProfile != null) {
                ExecutionDataBuilder dataBuilder =
                    ExecutionDataBuilder.fromAllTransformData(
                        LocalPipelineEngine.this, samplerStoresMap, false);

                // Send it to the location once
                //
                iLocation.registerData(dataBuilder.build());
              }

              // Update the pipeline execution state regularly
              //
              ExecutionState pipelineState =
                  ExecutionStateBuilder.fromExecutor(LocalPipelineEngine.this, -1).build();
              iLocation.updateExecutionState(pipelineState);

              // Update the state of all the transforms
              //
              for (IEngineComponent component : getComponents()) {
                ExecutionState transformState =
                    ExecutionStateBuilder.fromTransform(LocalPipelineEngine.this, component)
                        .build();
                iLocation.updateExecutionState(transformState);
              }
            } catch (Exception e) {
              // This is probably cause by a race condition triggering this code after the pipeline
              // finished.  We're just going to log this as a warning.
              //
              log.logBasic(
                  "Warning: unable to register execution info (data and state) at location "
                      + executionInfoLocation.getName()
                      + "(non-fatal)");
            }
          }
        };

    // Schedule the task to run regularly
    //
    transformExecutionInfoTimer = new Timer();
    transformExecutionInfoTimer.schedule(transformExecutionInfoTimerTask, delay, interval);
  }

  /**
   * This method looks up the execution information location specified in the run configuration.
   *
   * @throws HopException In case something fundamental went wrong.
   */
  public void lookupExecutionInformationLocation() throws HopException {
    String locationName = resolve(pipelineRunConfiguration.getExecutionInfoLocationName());
    if (StringUtils.isNotEmpty(locationName)) {
      ExecutionInfoLocation location =
          metadataProvider.getSerializer(ExecutionInfoLocation.class).load(locationName);
      if (location != null) {
        executionInfoLocation = location;

        // Initialize the location.
        // The location is closed when the last piece of information is saved.
        // See: stopTransformExecutionInfoTimer()
        //
        executionInfoLocation.getExecutionInfoLocation().initialize(this, metadataProvider);
      } else {
        log.logError(
            "Execution information location '"
                + locationName
                + "' could not be found in the metadata");
      }
    }
  }

  @Override
  public void pipelineCompleted() throws HopException {
    stopTransformExecutionInfoTimer();
    super.pipelineCompleted();
  }

  public void stopTransformExecutionInfoTimer() {
    try {
      if (transformExecutionInfoTimer != null) {
        if (transformExecutionInfoTimerTask != null) {
          transformExecutionInfoTimerTask.cancel();
        }
        ExecutorUtil.cleanup(transformExecutionInfoTimer);
        transformExecutionInfoTimer = null;
      }

      if (executionInfoLocation == null) {
        return;
      }

      IExecutionInfoLocation iLocation = executionInfoLocation.getExecutionInfoLocation();

      // Register one final last state of the pipeline
      //
      IPipelineEngine pipelineEngine = LocalPipelineEngine.this;

      ExecutionStateBuilder stateBuilder = ExecutionStateBuilder.fromExecutor(pipelineEngine, -1);
      ExecutionState executionState = stateBuilder.build();
      iLocation.updateExecutionState(executionState);

      // Update the state of all the transforms one final time
      //
      for (IEngineComponent component : getComponents()) {
        ExecutionState transformState =
            ExecutionStateBuilder.fromTransform(LocalPipelineEngine.this, component).build();
        iLocation.updateExecutionState(transformState);
      }

      String dataProfileName = resolve(pipelineRunConfiguration.getExecutionDataProfileName());
      if (StringUtils.isNotEmpty(dataProfileName)) {
        // Register the collected transform data for the last time
        //
        ExecutionDataBuilder dataBuilder =
            ExecutionDataBuilder.fromAllTransformData(
                LocalPipelineEngine.this, samplerStoresMap, true);
        iLocation.registerData(dataBuilder.build());
      }
    } catch (Throwable e) {
      log.logError("Error handling writing final pipeline state to location (non-fatal)", e);
    } finally {
      // We're now certain all listeners fired. We can close the location.
      //
      if (executionInfoLocation != null) {
        try {
          executionInfoLocation.getExecutionInfoLocation().close();
        } catch (Exception e) {
          log.logError(
              "Error closing execution information location: " + executionInfoLocation.getName(),
              e);
        }
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
