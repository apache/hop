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

import org.apache.hop.core.Const;
import org.apache.hop.core.IExtensionData;
import org.apache.hop.core.Result;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.map.DatabaseConnectionMap;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.parameters.INamedParameters;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.IExecutionFinishedListener;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.IPipelineEngineRunConfiguration;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engine.PipelineEngineCapabilities;
import org.apache.hop.pipeline.engine.PipelineEnginePlugin;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@PipelineEnginePlugin(
    id = "Local",
    name = "Hop local pipeline engine",
    description = "Executes your pipeline locally in a multi-threaded fashion")
public class LocalPipelineEngine extends Pipeline implements IPipelineEngine<PipelineMeta> {

  private PipelineEngineCapabilities engineCapabilities = new LocalPipelineEngineCapabilities();

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
            "local", "", new ArrayList<>(), createDefaultPipelineEngineRunConfiguration()));
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
                    } finally {
                      // Close connection always!
                      database.closeConnectionOnly();
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
                    } finally {
                      // Close connection always!
                      database.closeConnectionOnly();
                    }
                  }
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

  /** @param engineCapabilities The engineCapabilities to set */
  public void setEngineCapabilities(PipelineEngineCapabilities engineCapabilities) {
    this.engineCapabilities = engineCapabilities;
  }

  @Override
  public String getStatusDescription() {
    return super.getStatus();
  }
}
