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

package org.apache.hop.neo4j.execution;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.JsonRowMeta;
import org.apache.hop.core.row.RowBuffer;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.execution.Execution;
import org.apache.hop.execution.ExecutionBuilder;
import org.apache.hop.execution.ExecutionData;
import org.apache.hop.execution.ExecutionDataBuilder;
import org.apache.hop.execution.ExecutionDataSetMeta;
import org.apache.hop.execution.ExecutionInfoLocation;
import org.apache.hop.execution.ExecutionState;
import org.apache.hop.execution.ExecutionStateBuilder;
import org.apache.hop.execution.ExecutionStateComponentMetrics;
import org.apache.hop.execution.ExecutionType;
import org.apache.hop.execution.IExecutionInfoLocation;
import org.apache.hop.execution.IExecutionMatcher;
import org.apache.hop.execution.plugin.ExecutionInfoLocationPlugin;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.neo4j.actions.index.IndexUpdate;
import org.apache.hop.neo4j.actions.index.Neo4jIndex;
import org.apache.hop.neo4j.actions.index.ObjectType;
import org.apache.hop.neo4j.actions.index.UpdateType;
import org.apache.hop.neo4j.execution.builder.CypherCreateBuilder;
import org.apache.hop.neo4j.execution.builder.CypherDeleteBuilder;
import org.apache.hop.neo4j.execution.builder.CypherMergeBuilder;
import org.apache.hop.neo4j.execution.builder.CypherQueryBuilder;
import org.apache.hop.neo4j.execution.builder.CypherRelationshipBuilder;
import org.apache.hop.neo4j.execution.builder.ICypherBuilder;
import org.apache.hop.neo4j.shared.NeoConnection;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.workflow.delegates.HopGuiWorkflowClipboardDelegate;
import org.apache.hop.workflow.action.ActionMeta;
import org.eclipse.swt.SWT;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.TransactionContext;
import org.neo4j.driver.Value;

@GuiPlugin(description = "Neo4j execution information location GUI elements")
@ExecutionInfoLocationPlugin(
    id = "neo4j-location",
    name = "Neo4j location",
    description = "Stores execution information in a Neo4j graph database")
public class NeoExecutionInfoLocation implements IExecutionInfoLocation {
  public static final String EL_EXECUTION = "Execution";
  public static final String EP_ID = "id";
  public static final String EP_NAME = "name";
  public static final String EP_COPY_NR = "copyNr";
  public static final String EP_FILENAME = "filename";
  public static final String EP_PARENT_ID = "parentId";
  public static final String EP_EXECUTION_TYPE = "executionType";
  public static final String EP_EXECUTOR_XML = "executorXml";
  public static final String EP_METADATA_JSON = "metadataJson";
  public static final String EP_RUN_CONFIG_NAME = "runConfigName";
  public static final String EP_LOG_LEVEL = "logLevel";
  public static final String EP_REGISTRATION_DATE = "registrationDate";
  public static final String EP_EXECUTION_START_DATE = "executionStartDate";

  // The status fields
  public static final String EP_LOGGING_TEXT = "loggingText";
  public static final String EP_STATUS_DESCRIPTION = "statusDescription";
  public static final String EP_UPDATE_TIME = "updateTime";
  public static final String EP_CHILD_IDS = "childIds";
  public static final String EP_FAILED = "failed";
  public static final String EP_DETAILS = "details";
  public static final String EP_CONTAINER_ID = "containerId";
  public static final String EP_EXECUTION_END_DATE = "executionEndDate";

  public static final String CL_EXECUTION_METRIC = "ExecutionMetric";
  public static final String CP_ID = "id";
  public static final String CP_NAME = "name";
  public static final String CP_COPY_NR = "copyNr";
  public static final String CP_METRIC_KEY = "metricKey";
  public static final String CP_METRIC_VALUE = "metricValue";

  public static final String DL_EXECUTION_DATA = "ExecutionData";
  public static final String DP_PARENT_ID = "parentId";
  public static final String DP_OWNER_ID = "ownerId";
  public static final String DP_FINISHED = "finished";
  public static final String DP_EXECUTION_TYPE = "executionType";
  public static final String DP_COLLECTION_DATE = "collectionDate";

  public static final String ML_EXECUTION_DATA_SET_META = "ExecutionDataSetMeta";
  public static final String MP_PARENT_ID = "parentId";
  public static final String MP_OWNER_ID = "ownerId";
  public static final String MP_SET_KEY = "setKey";
  public static final String MP_NAME = "name";
  public static final String MP_COPY_NR = "copyNr";
  public static final String MP_DESCRIPTION = "description";
  public static final String MP_FIELD_NAME = "fieldName";
  public static final String MP_LOG_CHANNEL_ID = "logChannelId";
  public static final String MP_SAMPLE_DESCRIPTION = "sampleDescription";

  public static final String TL_EXECUTION_DATA_SET = "ExecutionDataSet";
  public static final String TP_PARENT_ID = "parentId";
  public static final String TP_OWNER_ID = "ownerId";
  public static final String TP_SET_KEY = "setKey";
  public static final String TP_ROW_META_JSON = "rowMetaJson";

  public static final String OL_EXECUTION_DATA_SET_ROW = "ExecutionDataSetRow";
  public static final String OP_PARENT_ID = "parentId";
  public static final String OP_OWNER_ID = "ownerId";
  public static final String OP_SET_KEY = "setKey";
  public static final String OP_ROW_NR = "rowNr";

  public static final String R_EXECUTES = "EXECUTES";
  public static final String R_HAS_DATA = "HAS_DATA";
  public static final String R_HAS_METADATA = "HAS_METADATA";
  public static final String R_HAS_DATASET = "HAS_DATASET";
  public static final String R_HAS_ROW = "HAS_ROW";
  public static final String CONST_ERROR_GETTING_EXECUTION_FROM_NEO_4_J =
      "Error getting execution from Neo4j";

  @HopMetadataProperty protected String pluginId;

  @HopMetadataProperty protected String pluginName;

  @GuiWidgetElement(
      id = "connectionName",
      order = "010",
      parentId = ExecutionInfoLocation.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.METADATA,
      metadata = NeoConnection.class,
      toolTip = "i18n::NeoExecutionInfoLocation.Connection.Tooltip",
      label = "i18n::NeoExecutionInfoLocation.Connection.Label")
  @HopMetadataProperty(key = "connection")
  protected String connectionName;

  private ILogChannel log;
  private Driver driver;
  private Session session;

  public NeoExecutionInfoLocation() {}

  public NeoExecutionInfoLocation(NeoExecutionInfoLocation location) {
    this.pluginId = location.pluginId;
    this.pluginName = location.pluginName;
    this.connectionName = location.connectionName;
  }

  @Override
  public NeoExecutionInfoLocation clone() {
    return new NeoExecutionInfoLocation(this);
  }

  @Override
  public void initialize(IVariables variables, IHopMetadataProvider metadataProvider)
      throws HopException {
    this.log = LogChannel.GENERAL;

    validateSettings();

    try {
      NeoConnection connection =
          metadataProvider
              .getSerializer(NeoConnection.class)
              .load(variables.resolve(connectionName));

      if (connection == null) {
        throw new HopException("Unable to find Neo4j connection " + connectionName);
      }

      // Connect to the database
      //
      this.driver = connection.getDriver(log, variables);
      this.session = connection.getSession(log, driver, variables);
    } catch (Exception e) {
      throw new HopException(
          "Error initializing Neo4j Execution Information location for connection "
              + connectionName,
          e);
    }
  }

  @Override
  public void close() throws HopException {
    try {
      session.close();
      driver.close();
    } catch (Exception e) {
      throw new HopException("Error closing Neo4j execution information location", e);
    }
  }

  @Override
  public void unBuffer(String executionId) {
    // There is nothing to remove from a buffer or cache.
  }

  /** Simply show the DDL to create the indexes */
  @GuiWidgetElement(
      id = "createIndexesButton",
      order = "020",
      parentId = ExecutionInfoLocation.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.BUTTON,
      label = "i18n::NeoExecutionInfoLocation.CreateIndexes.Label",
      toolTip = "i18n::NeoExecutionInfoLocation.CreateIndexes.Tooltip")
  public void createIndexesButton(Object object) {
    StringBuilder cypher = new StringBuilder();

    addIndex(cypher, "idx_execution_id", EL_EXECUTION, EP_ID);
    addIndex(cypher, "idx_execution_failed", EL_EXECUTION, EP_FAILED);
    addIndex(cypher, "idx_execution_parent_id", EL_EXECUTION, EP_PARENT_ID);
    addIndex(cypher, "idx_execution_metric_id", EL_EXECUTION, EP_ID, EP_NAME, EP_COPY_NR);
    addIndex(cypher, "idx_execution_data_id", DL_EXECUTION_DATA, DP_PARENT_ID, DP_OWNER_ID);
    addIndex(
        cypher,
        "idx_execution_data_set_meta_id",
        ML_EXECUTION_DATA_SET_META,
        MP_PARENT_ID,
        MP_OWNER_ID,
        MP_SET_KEY);
    addIndex(
        cypher,
        "idx_execution_data_set_id",
        TL_EXECUTION_DATA_SET,
        TP_PARENT_ID,
        TP_OWNER_ID,
        TP_SET_KEY);
    addIndex(
        cypher,
        "idx_execution_data_set_row_id",
        OL_EXECUTION_DATA_SET_ROW,
        OP_PARENT_ID,
        OP_OWNER_ID,
        OP_SET_KEY);

    EnterTextDialog textDialog =
        new EnterTextDialog(
            HopGui.getInstance().getShell(),
            "Indexes DDL",
            "Here is the list of Cypher statements to execute for the Neo4j location",
            cypher.toString());
    textDialog.open();
  }

  private void addIndex(StringBuilder cypher, String indexName, String label, String... keys) {
    assert keys != null && keys.length > 0 : "specify one or more keys";

    String keysClause = "ON ";
    boolean firstKey = true;
    for (String key : keys) {
      if (firstKey) {
        firstKey = false;
        keysClause += "( ";
      } else {
        keysClause += ", ";
      }
      keysClause += "n." + key;
    }
    keysClause += ") ";
    cypher
        .append("CREATE INDEX ")
        .append(indexName)
        .append(" IF NOT EXISTS FOR (n:")
        .append(label)
        .append(") ")
        .append(keysClause);
    cypher.append(Const.CR).append(";").append(Const.CR);
  }

  /** Copy a Neo4j Indexes action to the clipboard */
  @GuiWidgetElement(
      id = "createIndexesAction",
      order = "030",
      parentId = ExecutionInfoLocation.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.BUTTON,
      label = "i18n::NeoExecutionInfoLocation.CreateIndexAction.Label",
      toolTip = "i18n::NeoExecutionInfoLocation.CreateIndexAction.Tooltip")
  public void copyIndexActionToClipboardButton(Object object) {
    try {
      Neo4jIndex neo4jIndex = new Neo4jIndex("Neo4j Index", null);

      String connectionName = ((NeoExecutionInfoLocation) object).getConnectionName();
      if (StringUtils.isNotEmpty(connectionName)) {
        // Load the connection
        //
        NeoConnection neoConnection =
            HopGui.getInstance()
                .getMetadataProvider()
                .getSerializer(NeoConnection.class)
                .load(connectionName);
        neo4jIndex.setConnection(neoConnection);
      }

      addIndex(neo4jIndex, "idx_execution_id", EL_EXECUTION, EP_ID);
      addIndex(neo4jIndex, "idx_execution_failed", EL_EXECUTION, EP_FAILED);
      addIndex(neo4jIndex, "idx_execution_parent_id", EL_EXECUTION, EP_PARENT_ID);
      addIndex(neo4jIndex, "idx_execution_metric_id", EL_EXECUTION, EP_ID, EP_NAME, EP_COPY_NR);
      addIndex(neo4jIndex, "idx_execution_data_id", DL_EXECUTION_DATA, DP_PARENT_ID, DP_OWNER_ID);
      addIndex(
          neo4jIndex,
          "idx_execution_data_set_meta_id",
          ML_EXECUTION_DATA_SET_META,
          MP_PARENT_ID,
          MP_OWNER_ID,
          MP_SET_KEY);
      addIndex(
          neo4jIndex,
          "idx_execution_data_set_id",
          TL_EXECUTION_DATA_SET,
          TP_PARENT_ID,
          TP_OWNER_ID,
          TP_SET_KEY);
      addIndex(
          neo4jIndex,
          "idx_execution_data_set_row_id",
          OL_EXECUTION_DATA_SET_ROW,
          OP_PARENT_ID,
          OP_OWNER_ID,
          OP_SET_KEY);

      // Wrap it in an ActionMeta object
      //
      ActionMeta actionMeta = new ActionMeta(neo4jIndex);
      actionMeta.setLocation(50, 50);

      // Copy to clipboard
      HopGuiWorkflowClipboardDelegate.copyActionsToClipboard(List.of(actionMeta));

      // Show the message box
      //
      MessageBox box =
          new MessageBox(HopGui.getInstance().getShell(), SWT.OK | SWT.ICON_INFORMATION);
      box.setText("Copied to clipboard");
      box.setMessage(
          "A Neo4j Index action was copied to the clipboard.  You can paste this in a workflow to make sure you have great performance when updating execution information in this Neo4j location.");
      box.open();
    } catch (Exception e) {
      new ErrorDialog(
          HopGui.getInstance().getShell(),
          "Error",
          "Error copying Neo4j Index action to the clipboard",
          e);
    }
  }

  private void addIndex(Neo4jIndex neo4jIndex, String indexName, String label, String... keys) {
    StringBuilder properties = new StringBuilder();
    for (String key : keys) {
      if (!properties.isEmpty()) {
        properties.append(",");
      }
      properties.append(key);
    }
    IndexUpdate indexUpdate =
        new IndexUpdate(
            UpdateType.CREATE, ObjectType.NODE, indexName, label, properties.toString());
    neo4jIndex.getIndexUpdates().add(indexUpdate);
  }

  private void validateSettings() throws HopException {
    if (StringUtils.isEmpty(connectionName)) {
      throw new HopException("Please specify a Neo4j connection to send execution information to.");
    }
  }

  @Override
  public void registerExecution(Execution execution) throws HopException {
    synchronized (this) {
      try {
        assert execution.getName() != null : "Please register executions with a name";
        assert execution.getExecutionType() != null
            : "Please register executions with an execution type";

        session.executeWrite(transaction -> registerNeo4jExecution(transaction, execution));
      } catch (Exception e) {
        throw new HopException("Error registering execution in Neo4j", e);
      }
    }
  }

  private boolean registerNeo4jExecution(TransactionContext transaction, Execution execution) {
    try {
      CypherMergeBuilder builder =
          CypherMergeBuilder.of()
              .withLabelAndKey(EL_EXECUTION, EP_ID, execution.getId())
              .withValue(EP_NAME, execution.getName())
              .withValue(EP_COPY_NR, execution.getCopyNr())
              .withValue(EP_FILENAME, execution.getFilename())
              .withValue(EP_PARENT_ID, execution.getParentId())
              .withValue(EP_EXECUTION_TYPE, execution.getExecutionType().name())
              .withValue(EP_EXECUTOR_XML, execution.getExecutorXml())
              .withValue(EP_METADATA_JSON, execution.getMetadataJson())
              .withValue(EP_RUN_CONFIG_NAME, execution.getRunConfigurationName())
              .withValue(
                  EP_LOG_LEVEL,
                  execution.getLogLevel() == null ? null : execution.getLogLevel().getCode())
              .withValue(EP_REGISTRATION_DATE, execution.getRegistrationDate())
              .withValue(EP_EXECUTION_START_DATE, execution.getExecutionStartDate());
      execute(transaction, builder);

      if (StringUtils.isNotEmpty(execution.getParentId())) {
        CypherRelationshipBuilder selfRelationshipBuilder =
            CypherRelationshipBuilder.of()
                .withMatch(EL_EXECUTION, "e1", EP_ID, execution.getId())
                .withMatch(EL_EXECUTION, "e2", EP_ID, execution.getParentId())
                .withMerge("e2", "e1", R_EXECUTES);
        execute(transaction, selfRelationshipBuilder);
      }

      // Transaction is automatically committed by executeWrite
      return true;
    } catch (Exception e) {
      // Transaction is automatically rolled back by executeWrite on exception
      throw e;
    }
  }

  @Override
  public boolean deleteExecution(String executionId) throws HopException {
    synchronized (this) {
      try {
        return session.executeWrite(transaction -> deleteNeo4jExecution(transaction, executionId));
      } catch (Exception e) {
        throw new HopException("Error deleting execution with id " + executionId + " in Neo4j", e);
      }
    }
  }

  private boolean deleteNeo4jExecution(TransactionContext transaction, String executionId) {
    // Get the children of this execution. Delete those first
    //
    List<Execution> childExecutions = findNeo4jExecutions(transaction, executionId);
    for (Execution childExecution : childExecutions) {
      deleteNeo4jExecution(transaction, childExecution.getId());
    }

    // Now delete any data, data sets, rows and metadata with the given execution ID as a parent
    //
    // Rows
    //
    execute(
        transaction,
        CypherDeleteBuilder.of()
            .withMatch(OL_EXECUTION_DATA_SET_ROW, "n", Map.of(OP_PARENT_ID, executionId))
            .withDetachDelete("n"));

    // SetMeta
    //
    execute(
        transaction,
        CypherDeleteBuilder.of()
            .withMatch(ML_EXECUTION_DATA_SET_META, "n", Map.of(MP_PARENT_ID, executionId))
            .withDetachDelete("n"));

    // DataSet
    //
    execute(
        transaction,
        CypherDeleteBuilder.of()
            .withMatch(TL_EXECUTION_DATA_SET, "n", Map.of(TP_PARENT_ID, executionId))
            .withDetachDelete("n"));

    // Data
    //
    execute(
        transaction,
        CypherDeleteBuilder.of()
            .withMatch(DL_EXECUTION_DATA, "n", Map.of(DP_PARENT_ID, executionId))
            .withDetachDelete("n"));

    // The execution metrics
    //
    execute(
        transaction,
        CypherDeleteBuilder.of()
            .withMatch(CL_EXECUTION_METRIC, "n", Map.of(CP_ID, executionId))
            .withDetachDelete("n"));

    // The execution itself
    //
    execute(
        transaction,
        CypherDeleteBuilder.of()
            .withMatch(EL_EXECUTION, "n", Map.of(EP_ID, executionId))
            .withDetachDelete("n"));

    return true;
  }

  @Override
  public Execution getExecution(String executionId) throws HopException {
    synchronized (this) {
      try {
        return session.executeRead(transaction -> getNeo4jExecution(transaction, executionId));
      } catch (Exception e) {
        throw new HopException(CONST_ERROR_GETTING_EXECUTION_FROM_NEO_4_J, e);
      }
    }
  }

  private Execution getNeo4jExecution(TransactionContext transaction, String executionId) {
    CypherQueryBuilder builder =
        CypherQueryBuilder.of()
            .withLabelAndKey("n", EL_EXECUTION, EP_ID, executionId)
            .withReturnValues(
                "n",
                EP_NAME,
                EP_COPY_NR,
                EP_FILENAME,
                EP_PARENT_ID,
                EP_EXECUTION_TYPE,
                EP_EXECUTOR_XML,
                EP_METADATA_JSON,
                EP_RUN_CONFIG_NAME,
                EP_LOG_LEVEL,
                EP_REGISTRATION_DATE,
                EP_EXECUTION_START_DATE);
    Result result = transaction.run(builder.cypher(), builder.parameters());

    // We expect exactly one result
    //
    if (!result.hasNext()) {
      return null;
    }
    org.neo4j.driver.Record record = result.next();

    return ExecutionBuilder.of()
        .withId(executionId)
        .withParentId(getString(record, EP_PARENT_ID))
        .withName(getString(record, EP_NAME))
        .withCopyNr(getString(record, EP_COPY_NR))
        .withFilename(getString(record, EP_FILENAME))
        .withExecutorType(ExecutionType.valueOf(getString(record, EP_EXECUTION_TYPE)))
        .withExecutorXml(getString(record, EP_EXECUTOR_XML))
        .withMetadataJson(getString(record, EP_METADATA_JSON))
        .withRunConfigurationName(getString(record, EP_RUN_CONFIG_NAME))
        .withLogLevel(LogLevel.lookupCode(getString(record, EP_LOG_LEVEL)))
        .withRegistrationDate(getDate(record, EP_REGISTRATION_DATE))
        .withExecutionStartDate(getDate(record, EP_EXECUTION_START_DATE))
        .build();
  }

  @Override
  public List<String> getExecutionIds(boolean includeChildren, int limit) throws HopException {
    synchronized (this) {
      try {
        return session.executeRead(
            transaction -> getNeo4jExecutionIds(transaction, includeChildren, limit));
      } catch (Exception e) {
        throw new HopException(CONST_ERROR_GETTING_EXECUTION_FROM_NEO_4_J, e);
      }
    }
  }

  private List<String> getNeo4jExecutionIds(
      TransactionContext transaction, boolean includeChildren, int limit) {
    List<String> ids = new ArrayList<>();

    CypherQueryBuilder builder =
        CypherQueryBuilder.of().withLabelWithoutKey("n", EL_EXECUTION) // Get all nodes
        ;
    if (!includeChildren) {
      builder.withWhereIsNull("n", EP_PARENT_ID);
    }
    builder
        .withReturnValues("n", EP_ID) // Just return the execution ID
        .withOrderBy("n", EP_REGISTRATION_DATE, false)
        .withLimit(limit);

    Result result = transaction.run(builder.cypher());
    while (result.hasNext()) {
      org.neo4j.driver.Record record = result.next();
      ids.add(getString(record, EP_ID));
    }
    return ids;
  }

  @Override
  public void updateExecutionState(ExecutionState executionState) throws HopException {
    synchronized (this) {
      try {
        assert executionState.getName() != null : "Please update execution states with a name";
        assert executionState.getExecutionType() != null
            : "Please update execution states with an execution type";

        session.executeWrite(transaction -> updateNeo4jExecutionState(transaction, executionState));
      } catch (Exception e) {
        throw new HopException("Error updating execution state in Neo4j", e);
      }
    }
  }

  private boolean updateNeo4jExecutionState(TransactionContext transaction, ExecutionState state) {
    try {
      // Update information in the Execution node
      //
      CypherMergeBuilder stateCypherBuilder =
          CypherMergeBuilder.of()
              .withLabelAndKey(EL_EXECUTION, EP_ID, state.getId())
              .withValue(EP_STATUS_DESCRIPTION, state.getStatusDescription())
              .withValue(EP_LOGGING_TEXT, state.getLoggingText())
              .withValue(EP_UPDATE_TIME, state.getUpdateTime())
              .withValue(EP_CHILD_IDS, state.getChildIds())
              .withValue(EP_FAILED, state.isFailed())
              .withValue(EP_DETAILS, state.getDetails())
              .withValue(EP_CONTAINER_ID, state.getContainerId())
              .withValue(EP_EXECUTION_END_DATE, state.getExecutionEndDate());

      transaction.run(stateCypherBuilder.cypher(), stateCypherBuilder.parameters());

      // Save the metrics as well...
      //
      if (state.getMetrics() != null) {
        for (ExecutionStateComponentMetrics metric : state.getMetrics()) {
          // Save all the metrics in this map in there...
          //
          for (String metricKey : metric.getMetrics().keySet()) {
            CypherCreateBuilder metricBuilder =
                CypherCreateBuilder.of()
                    .withLabelAndKeys(
                        CL_EXECUTION_METRIC,
                        Map.of(
                            CP_ID, state.getId(),
                            CP_NAME, metric.getComponentName(),
                            CP_COPY_NR, metric.getComponentCopy(),
                            CP_METRIC_KEY, metricKey))
                    .withValue(CP_METRIC_VALUE, metric.getMetrics().get(metricKey));
            execute(transaction, metricBuilder);
          }
        }
      }

      // Transaction is automatically committed by executeWrite
      return true;
    } catch (Exception e) {
      // Transaction is automatically rolled back by executeWrite on exception
      throw e;
    }
  }

  @Override
  public ExecutionState getExecutionState(String executionId) throws HopException {
    return getExecutionState(executionId, true);
  }

  @Override
  public ExecutionState getExecutionState(String executionId, boolean includeLogging)
      throws HopException {
    synchronized (this) {
      try {
        return session.executeRead(
            transaction -> getNeo4jExecutionState(transaction, executionId, includeLogging));
      } catch (Exception e) {
        throw new HopException(CONST_ERROR_GETTING_EXECUTION_FROM_NEO_4_J, e);
      }
    }
  }

  private ExecutionState getNeo4jExecutionState(
      TransactionContext transaction, String executionId, boolean includeLogging) {
    CypherQueryBuilder executionBuilder =
        CypherQueryBuilder.of()
            .withLabelAndKey("n", EL_EXECUTION, EP_ID, executionId)
            .withReturnValues(
                "n",
                EP_NAME,
                EP_EXECUTION_TYPE,
                EP_COPY_NR,
                EP_PARENT_ID,
                EP_LOGGING_TEXT,
                EP_STATUS_DESCRIPTION,
                EP_UPDATE_TIME,
                EP_CHILD_IDS,
                EP_FAILED,
                EP_DETAILS,
                EP_CONTAINER_ID,
                EP_EXECUTION_END_DATE);
    Result result = transaction.run(executionBuilder.cypher(), executionBuilder.parameters());

    // We expect exactly one result
    //
    if (!result.hasNext()) {
      return null;
    }
    org.neo4j.driver.Record record = result.next();

    // Only load logging text if we're asking for it specifically.
    //
    String loggingText = null;
    if (includeLogging) {
      loggingText = getString(record, EP_LOGGING_TEXT);
    }

    ExecutionStateBuilder stateBuilder =
        ExecutionStateBuilder.of()
            .withId(executionId)
            .withName(getString(record, EP_NAME))
            .withCopyNr(getString(record, EP_COPY_NR))
            .withParentId(getString(record, EP_PARENT_ID))
            .withLoggingText(loggingText)
            .withExecutionType(ExecutionType.valueOf(getString(record, EP_EXECUTION_TYPE)))
            .withStatusDescription(getString(record, EP_STATUS_DESCRIPTION))
            .withUpdateTime(getDate(record, EP_UPDATE_TIME))
            .withChildIds(getList(record, EP_CHILD_IDS))
            .withFailed(getBoolean(record, EP_FAILED))
            .withDetails(getMap(record, EP_DETAILS))
            .withContainerId(getString(record, EP_CONTAINER_ID))
            .withExecutionEndDate(getDate(record, EP_EXECUTION_END_DATE));

    // Add the metrics to the state...
    //
    Result metricsResult =
        execute(
            transaction,
            CypherQueryBuilder.of()
                .withLabelAndKeys("n", CL_EXECUTION_METRIC, Map.of(CP_ID, executionId))
                .withReturnValues("n", CP_NAME, CP_COPY_NR, CP_METRIC_KEY, CP_METRIC_VALUE));

    Map<String, ExecutionStateComponentMetrics> metricsMap = new HashMap<>();

    while (metricsResult.hasNext()) {
      org.neo4j.driver.Record metricsRecord = metricsResult.next();
      String componentName = getString(metricsRecord, CP_NAME);
      String componentCopy = getString(metricsRecord, CP_COPY_NR);
      String componentKey = componentName + "." + componentCopy;
      String metricKey = getString(metricsRecord, CP_METRIC_KEY);
      Long metricValue = getLong(metricsRecord, CP_METRIC_VALUE);
      if (metricValue != null) {
        // Add it to the map...
        //
        ExecutionStateComponentMetrics metric =
            metricsMap.computeIfAbsent(
                componentKey,
                f -> new ExecutionStateComponentMetrics(componentName, componentCopy));

        // Add the value
        metric.getMetrics().put(metricKey, metricValue);
      }
    }

    stateBuilder.withMetrics(new ArrayList(metricsMap.values()));

    return stateBuilder.build();
  }

  @Override
  public String getExecutionStateLoggingText(String executionId, int sizeLimit)
      throws HopException {
    synchronized (this) {
      try {
        return session.executeRead(
            transaction -> getNeo4jExecutionStateLoggingText(transaction, executionId, sizeLimit));
      } catch (Exception e) {
        throw new HopException(CONST_ERROR_GETTING_EXECUTION_FROM_NEO_4_J, e);
      }
    }
  }

  private String getNeo4jExecutionStateLoggingText(
      TransactionContext transaction, String executionId, int sizeLimit) {
    CypherQueryBuilder executionBuilder =
        CypherQueryBuilder.of()
            .withLabelAndKey("n", EL_EXECUTION, EP_ID, executionId)
            .withReturnValues("n", EP_LOGGING_TEXT);
    Result result = transaction.run(executionBuilder.cypher(), executionBuilder.parameters());

    // We expect exactly one result
    //
    if (!result.hasNext()) {
      return null;
    }
    org.neo4j.driver.Record record = result.next();

    // Only return the string up to the size limit
    //
    String loggingText = getString(record, EP_LOGGING_TEXT);
    if (loggingText == null) {
      return null;
    }
    if (sizeLimit <= 0) {
      return loggingText;
    }
    // Return at most 20MB
    //
    return loggingText.substring(0, Math.min(sizeLimit, loggingText.length()));
  }

  @Override
  public List<Execution> findExecutions(String parentExecutionId) throws HopException {
    synchronized (this) {
      try {
        return session.executeRead(
            transaction -> findNeo4jExecutions(transaction, parentExecutionId));
      } catch (Exception e) {
        throw new HopException(CONST_ERROR_GETTING_EXECUTION_FROM_NEO_4_J, e);
      }
    }
  }

  /**
   * Go over the executions and match those with type and name. They are reverse ordered by date so
   * we look for the first in the list that doesn't have a failed state.
   *
   * @param executionType The type of execution to look for
   * @param name The name of the executor
   * @return
   * @throws HopException
   */
  @Override
  public Execution findPreviousSuccessfulExecution(ExecutionType executionType, String name)
      throws HopException {
    synchronized (this) {
      try {
        return session.executeRead(
            transaction -> findNeo4jPreviousSuccessfulExecution(transaction, executionType, name));
      } catch (Exception e) {
        throw new HopException("Error find previous successful execution in Neo4j", e);
      }
    }
  }

  private Execution findNeo4jPreviousSuccessfulExecution(
      TransactionContext transaction, ExecutionType executionType, String name) {
    List<Execution> executions =
        findNeo4jExecutions(
            transaction, e -> e.getExecutionType() == executionType && name.equals(e.getName()));
    for (Execution execution : executions) {
      ExecutionState executionState = getNeo4jExecutionState(transaction, execution.getId(), false);
      if (executionState != null && !executionState.isFailed()) {
        return execution;
      }
    }
    return null;
  }

  /**
   * Find those Executions that have a matching parentId
   *
   * @param transaction
   * @param parentExecutionId
   * @return The list of executions or an empty list if nothing was found
   */
  private List<Execution> findNeo4jExecutions(
      TransactionContext transaction, String parentExecutionId) {
    List<Execution> executions = new ArrayList<>();

    Result result =
        execute(
            transaction,
            CypherQueryBuilder.of()
                .withLabelAndKey("n", EL_EXECUTION, EP_PARENT_ID, parentExecutionId)
                .withReturnValues("n", EP_ID));
    while (result.hasNext()) {
      org.neo4j.driver.Record record = result.next();
      String executionId = getString(record, EP_ID);

      try {
        Execution execution = getNeo4jExecution(transaction, executionId);
        if (execution != null) {
          executions.add(execution);
        }
      } catch (Exception e) {
        log.logError(
            "Error loading execution with id : " + executionId + " from Neo4j (non-fatal)", e);
      }
    }
    return executions;
  }

  @Override
  public List<Execution> findExecutions(IExecutionMatcher matcher) throws HopException {
    synchronized (this) {
      try {
        return session.executeRead(transaction -> findNeo4jExecutions(transaction, matcher));
      } catch (Exception e) {
        throw new HopException(CONST_ERROR_GETTING_EXECUTION_FROM_NEO_4_J, e);
      }
    }
  }

  private List<Execution> findNeo4jExecutions(
      TransactionContext transaction, IExecutionMatcher matcher) {
    List<Execution> executions = new ArrayList<>();

    // Get all
    List<String> ids = getNeo4jExecutionIds(transaction, true, 0);
    for (String id : ids) {
      Execution execution = getNeo4jExecution(transaction, id);
      if (execution != null && matcher.matches(execution)) {
        executions.add(execution);
      }
    }
    return executions;
  }

  @Override
  public void registerData(ExecutionData data) throws HopException {
    synchronized (this) {
      try {
        session.executeWrite(transaction -> registerNeo4jData(transaction, data));
      } catch (Exception e) {
        throw new HopException(CONST_ERROR_GETTING_EXECUTION_FROM_NEO_4_J, e);
      }
    }
  }

  private boolean registerNeo4jData(TransactionContext transaction, ExecutionData data) {
    try {
      assert data != null : "no execution data provided";
      assert data.getExecutionType() != null : "execution data has no type";

      // Merge the ExecutionData node
      //
      CypherMergeBuilder stateCypherBuilder =
          CypherMergeBuilder.of()
              .withLabelAndKeys(
                  DL_EXECUTION_DATA,
                  Map.of(DP_PARENT_ID, data.getParentId(), DP_OWNER_ID, data.getOwnerId()))
              .withValue(DP_EXECUTION_TYPE, data.getExecutionType().name())
              .withValue(DP_FINISHED, data.isFinished())
              .withValue(DP_COLLECTION_DATE, data.getCollectionDate());
      execute(transaction, stateCypherBuilder);

      // Merge the relationship between Execution and ExecutionData and ExecutionState
      //
      CypherRelationshipBuilder executionDataRelCypherBuilder =
          CypherRelationshipBuilder.of()
              .withMatch(EL_EXECUTION, "n", EP_ID, data.getParentId())
              .withMatch(
                  DL_EXECUTION_DATA,
                  "d",
                  Map.of(DP_PARENT_ID, data.getParentId(), DP_OWNER_ID, data.getOwnerId()))
              .withMerge("n", "d", R_HAS_DATA);
      execute(transaction, executionDataRelCypherBuilder);

      // Do we have metadata directly at the top level of the execution data?
      //
      ExecutionDataSetMeta dataSetMeta = data.getDataSetMeta();
      if (dataSetMeta != null) {
        saveDataSetMeta(transaction, data.getParentId(), data.getOwnerId(), dataSetMeta);

        // Also save the relationship between the execution data and the metadata
        //
        execute(
            transaction,
            CypherRelationshipBuilder.of()
                .withMatch(
                    DL_EXECUTION_DATA,
                    "s",
                    Map.of(DP_PARENT_ID, data.getParentId(), DP_OWNER_ID, data.getOwnerId()))
                .withMatch(
                    ML_EXECUTION_DATA_SET_META,
                    "m",
                    Map.of(
                        MP_PARENT_ID,
                        data.getParentId(),
                        MP_OWNER_ID,
                        data.getOwnerId(),
                        MP_SET_KEY,
                        dataSetMeta.getSetKey()))
                .withMerge("s", "m", R_HAS_METADATA));
      }

      // Do we have any data sets?
      //
      for (String setKey : data.getDataSets().keySet()) {
        RowBuffer rowBuffer = data.getDataSets().get(setKey);
        ExecutionDataSetMeta setMeta = data.getSetMetaData().get(setKey);
        saveNeo4jRowsAndMeta(
            transaction, data.getParentId(), data.getOwnerId(), rowBuffer, setMeta);
      }

      // Transaction is automatically committed by executeWrite
      return true;
    } catch (Exception e) {
      // Transaction is automatically rolled back by executeWrite on exception
      throw e;
    }
  }

  private void saveNeo4jRowsAndMeta(
      TransactionContext transaction,
      String parentId,
      String ownerId,
      RowBuffer rowBuffer,
      ExecutionDataSetMeta setMeta) {

    try {
      IRowMeta rowMeta = rowBuffer.getRowMeta();
      String rowMetaJson = rowMeta == null ? null : JsonRowMeta.toJson(rowMeta);

      // Save the Execution Data Set node
      //
      execute(
          transaction,
          CypherMergeBuilder.of()
              .withLabelAndKeys(
                  TL_EXECUTION_DATA_SET,
                  Map.of(
                      TP_PARENT_ID,
                      parentId,
                      TP_OWNER_ID,
                      ownerId,
                      TP_SET_KEY,
                      setMeta.getSetKey()))
              .withValue(TP_ROW_META_JSON, rowMetaJson));

      // Create a relationship between the execution data and the data set
      //
      execute(
          transaction,
          CypherRelationshipBuilder.of()
              .withMatch(
                  DL_EXECUTION_DATA, "d", Map.of(DP_PARENT_ID, parentId, DP_OWNER_ID, ownerId))
              .withMatch(
                  TL_EXECUTION_DATA_SET,
                  "s",
                  Map.of(
                      TP_PARENT_ID,
                      parentId,
                      TP_OWNER_ID,
                      ownerId,
                      TP_SET_KEY,
                      setMeta.getSetKey()))
              .withMerge("d", "s", R_HAS_DATASET));

      // Save the metadata for this data set
      //
      saveDataSetMeta(transaction, parentId, ownerId, setMeta);

      // Create the relationship between this data set and the metadata
      //
      execute(
          transaction,
          CypherRelationshipBuilder.of()
              .withMatch(
                  TL_EXECUTION_DATA_SET,
                  "s",
                  Map.of(
                      TP_PARENT_ID,
                      parentId,
                      TP_OWNER_ID,
                      ownerId,
                      TP_SET_KEY,
                      setMeta.getSetKey()))
              .withMatch(
                  ML_EXECUTION_DATA_SET_META,
                  "m",
                  Map.of(
                      MP_PARENT_ID,
                      parentId,
                      MP_OWNER_ID,
                      ownerId,
                      MP_SET_KEY,
                      setMeta.getSetKey()))
              .withMerge("s", "m", R_HAS_METADATA));

      // Now save rows for this data set
      //
      // First delete old rows
      //
      execute(
          transaction,
          CypherDeleteBuilder.of()
              .withMatch(
                  TL_EXECUTION_DATA_SET,
                  "s",
                  Map.of(
                      TP_PARENT_ID,
                      parentId,
                      TP_OWNER_ID,
                      ownerId,
                      TP_SET_KEY,
                      setMeta.getSetKey()))
              .withMatch(
                  OL_EXECUTION_DATA_SET_ROW,
                  "r",
                  Map.of(
                      OP_PARENT_ID,
                      parentId,
                      OP_OWNER_ID,
                      ownerId,
                      OP_SET_KEY,
                      setMeta.getSetKey()))
              .withRelationshipMatch(R_HAS_ROW, "rel", "s", "r")
              .withDelete("r", "rel"));
      // Add the new rows
      //
      for (int rowNr = 1; rowNr <= rowBuffer.getBuffer().size(); rowNr++) {
        Object[] row = rowBuffer.getBuffer().get(rowNr - 1);
        CypherCreateBuilder builder =
            CypherCreateBuilder.of()
                .withLabelAndKeys(
                    OL_EXECUTION_DATA_SET_ROW,
                    Map.of(
                        OP_PARENT_ID,
                        parentId,
                        OP_OWNER_ID,
                        ownerId,
                        OP_SET_KEY,
                        setMeta.getSetKey(),
                        OP_ROW_NR,
                        rowNr));
        for (int v = 0; v < rowMeta.size(); v++) {
          IValueMeta valueMeta = rowMeta.getValueMeta(v);
          builder.withValue("field" + v, valueMeta.getNativeDataType(row[v]));
        }
        execute(transaction, builder);

        // Create a relationship from the data set to this row
        //
        execute(
            transaction,
            CypherRelationshipBuilder.of()
                .withMatch(
                    TL_EXECUTION_DATA_SET,
                    "s",
                    Map.of(
                        TP_PARENT_ID,
                        parentId,
                        TP_OWNER_ID,
                        ownerId,
                        TP_SET_KEY,
                        setMeta.getSetKey()))
                .withMatch(
                    OL_EXECUTION_DATA_SET_ROW,
                    "r",
                    Map.of(
                        OP_PARENT_ID,
                        parentId,
                        OP_OWNER_ID,
                        ownerId,
                        OP_SET_KEY,
                        setMeta.getSetKey(),
                        OP_ROW_NR,
                        rowNr))
                .withCreate("s", "r", R_HAS_ROW));
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void saveDataSetMeta(
      TransactionContext transaction,
      String parentId,
      String ownerId,
      ExecutionDataSetMeta dataSetMeta) {
    // Save the data set meta node
    //
    execute(
        transaction,
        CypherMergeBuilder.of()
            .withLabelAndKeys(
                ML_EXECUTION_DATA_SET_META,
                Map.of(
                    MP_PARENT_ID,
                    parentId,
                    MP_OWNER_ID,
                    ownerId,
                    MP_SET_KEY,
                    dataSetMeta.getSetKey()))
            .withValue(MP_NAME, dataSetMeta.getName())
            .withValue(MP_COPY_NR, dataSetMeta.getCopyNr())
            .withValue(MP_DESCRIPTION, dataSetMeta.getDescription())
            .withValue(MP_FIELD_NAME, dataSetMeta.getFieldName())
            .withValue(MP_LOG_CHANNEL_ID, dataSetMeta.getLogChannelId())
            .withValue(MP_SAMPLE_DESCRIPTION, dataSetMeta.getSampleDescription()));
  }

  @Override
  public ExecutionData getExecutionData(String parentExecutionId, String executionId)
      throws HopException {
    synchronized (this) {
      try {
        return session.executeRead(
            transaction -> getNeo4jExecutionData(transaction, parentExecutionId, executionId));
      } catch (Exception e) {
        throw new HopException(CONST_ERROR_GETTING_EXECUTION_FROM_NEO_4_J, e);
      }
    }
  }

  private ExecutionData getNeo4jExecutionData(
      TransactionContext transaction, String parentExecutionId, String executionId) {
    // Find the Execution Data node information.
    //
    ExecutionDataBuilder builder =
        ExecutionDataBuilder.of().withParentId(parentExecutionId).withOwnerId(executionId);

    Map<String, Object> keyValueMap = new HashMap<>();
    keyValueMap.put(DP_PARENT_ID, parentExecutionId);
    if (StringUtils.isNotEmpty(executionId)) {
      keyValueMap.put(DP_OWNER_ID, executionId);
    }

    // Get the execution data node(s) attached for the given parent execution ID
    //
    Result result =
        execute(
            transaction,
            CypherQueryBuilder.of()
                .withLabelAndKeys("n", DL_EXECUTION_DATA, keyValueMap)
                .withReturnValues(
                    "n", DP_EXECUTION_TYPE, DP_OWNER_ID, DP_FINISHED, DP_COLLECTION_DATE));
    boolean foundData = false;
    boolean allFinished = true;
    while (result.hasNext()) {
      org.neo4j.driver.Record dataRecord = result.next();
      foundData = true;
      boolean finished = getBoolean(dataRecord, DP_FINISHED);
      if (!finished) {
        allFinished = false;
      }
      String ownerId = getString(dataRecord, DP_OWNER_ID);
      builder.withExecutionType(ExecutionType.valueOf(getString(dataRecord, DP_EXECUTION_TYPE)));
      builder.withCollectionDate(getDate(dataRecord, DP_COLLECTION_DATE));

      // See if there's any data set metadata associated with this data set .
      // Typically, this is used to describe the action metadata in a workflow.
      // The parentId would be the execution ID of a workflow.
      // The executionId would be of the action.
      //
      ExecutionDataSetMeta setMeta =
          getNeo4jExecutionDataSetMeta(transaction, parentExecutionId, ownerId);
      if (setMeta != null) {
        if (StringUtils.isNotEmpty(executionId)) {
          builder.withDataSetMeta(setMeta);
        } else {
          builder.addSetMeta(setMeta.getSetKey(), setMeta);
        }
      }

      // Now find all the associated execution data sets
      // For pipelines, the parentId would be the log channel ID of the pipeline.
      // The ownerId would be "all-transforms" (optimization to write rows in larger groups).
      //
      Result dataSetsResults =
          execute(
              transaction,
              CypherQueryBuilder.of()
                  .withLabelAndKeys(
                      "n",
                      TL_EXECUTION_DATA_SET,
                      Map.of(TP_PARENT_ID, parentExecutionId, TP_OWNER_ID, ownerId))
                  .withReturnValues("n", TP_SET_KEY, TP_ROW_META_JSON));
      while (dataSetsResults.hasNext()) {
        org.neo4j.driver.Record record = dataSetsResults.next();
        String setKey = getString(record, TP_SET_KEY);
        String rowMetaJson = getString(record, TP_ROW_META_JSON);
        IRowMeta rowMeta;
        if (StringUtils.isEmpty(rowMetaJson)) {
          rowMeta = new RowMeta();
        } else {
          rowMeta = JsonRowMeta.fromJson(rowMetaJson);
        }
        if (!rowMeta.isEmpty()) {
          ExecutionDataSetMeta dataSetMeta =
              getNeo4jExecutionDataSetMeta(transaction, parentExecutionId, ownerId, setKey);
          if (dataSetMeta != null) {
            builder.addSetMeta(setKey, dataSetMeta);

            // Load the rows for the set straight from the row nodes.  The row metadata is already
            // known.
            //
            List<String> fieldNames = new ArrayList<>();
            Map<String, String> fieldMap = new HashMap<>();
            for (int v = 0; v < rowMeta.size(); v++) {
              String fieldName = "field" + v;
              fieldNames.add(fieldName);
              fieldMap.put(fieldName, rowMeta.getValueMeta(v).getName());
            }
            Result rowsResult =
                execute(
                    transaction,
                    CypherQueryBuilder.of()
                        .withLabelAndKeys(
                            "n",
                            OL_EXECUTION_DATA_SET_ROW,
                            Map.of(
                                OP_PARENT_ID,
                                parentExecutionId,
                                OP_OWNER_ID,
                                ownerId,
                                OP_SET_KEY,
                                setKey))
                        .withReturnValues("n", fieldNames.toArray(new String[0]))
                        .withOrderBy("n", OP_ROW_NR, true));
            RowBuffer rowBuffer = new RowBuffer(rowMeta);
            while (rowsResult.hasNext()) {
              org.neo4j.driver.Record rowsRecord = rowsResult.next();
              Object[] row = new Object[rowMeta.size()];
              for (int v = 0; v < rowMeta.size(); v++) {
                IValueMeta valueMeta = rowMeta.getValueMeta(v);
                Value value = rowsRecord.get("n.field" + v);
                row[v] = extractHopValue(valueMeta, value);
              }
              rowBuffer.addRow(row);
            }
            // Store the row buffer
            //
            builder.addDataSet(setKey, rowBuffer);
          }
        }
      }
    }

    builder.withFinished(allFinished);

    if (!foundData) {
      return null;
    }
    return builder.build();
  }

  private Object extractHopValue(IValueMeta valueMeta, Value value) {
    if (value == null) {
      return null;
    }
    if (value.isNull()) {
      return null;
    }
    switch (valueMeta.getType()) {
      case IValueMeta.TYPE_STRING:
        return value.asString();
      case IValueMeta.TYPE_INTEGER:
        return value.asLong();
      case IValueMeta.TYPE_DATE:
        LocalDateTime localDateTime = value.asLocalDateTime();
        return Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant());
      case IValueMeta.TYPE_BOOLEAN:
        return value.asBoolean();
      case IValueMeta.TYPE_NUMBER:
        return value.asDouble();
      default:
        log.logError(
            "Data type not yet supported : "
                + valueMeta.getTypeDesc()
                + " (non-fatal, returning null)");
        return null;
    }
  }

  private ExecutionDataSetMeta getNeo4jExecutionDataSetMeta(
      TransactionContext transaction, String parentExecutionId, String ownerId) {
    // If there is a direct relationship between Data and DataSetMeta we can
    // follow that relationship and get the metadata from the result.
    // There should always just be one node found.  There's no need to include the set key to do
    // this
    // as there is no data set associated with this information.
    //
    Result result =
        execute(
            transaction,
            CypherQueryBuilder.of()
                .withLabelAndKeys(
                    "d",
                    DL_EXECUTION_DATA,
                    Map.of(MP_PARENT_ID, parentExecutionId, MP_OWNER_ID, ownerId))
                .withMatch(
                    "n",
                    ML_EXECUTION_DATA_SET_META,
                    Map.of(
                        MP_PARENT_ID, parentExecutionId, MP_OWNER_ID, ownerId, MP_SET_KEY, ownerId))
                .withRelationship("d", "n", R_HAS_METADATA)
                .withReturnValues(
                    "n",
                    MP_SET_KEY,
                    MP_NAME,
                    MP_COPY_NR,
                    MP_DESCRIPTION,
                    MP_FIELD_NAME,
                    MP_LOG_CHANNEL_ID,
                    MP_SAMPLE_DESCRIPTION));

    if (result.hasNext()) {
      return extractDataSetMeta(result.next());
    } else {
      return null;
    }
  }

  /**
   * Here we load the description of a particular data set and we have a set key for it.
   *
   * @param transaction
   * @param parentExecutionId
   * @param ownerId
   * @param setKey
   * @return
   */
  private ExecutionDataSetMeta getNeo4jExecutionDataSetMeta(
      TransactionContext transaction, String parentExecutionId, String ownerId, String setKey) {
    // If there is a direct relationship between Data and DataSetMeta we can
    // follow that relationship and get the metadata from the result.
    // There should always just be one node found.  There's no need to include the set key to do
    // this
    // as there is no data set associated with this information.
    //
    Result result =
        execute(
            transaction,
            CypherQueryBuilder.of()
                .withLabelAndKeys(
                    "n",
                    ML_EXECUTION_DATA_SET_META,
                    Map.of(
                        MP_PARENT_ID, parentExecutionId, MP_OWNER_ID, ownerId, MP_SET_KEY, setKey))
                .withReturnValues(
                    "n",
                    MP_SET_KEY,
                    MP_NAME,
                    MP_COPY_NR,
                    MP_DESCRIPTION,
                    MP_FIELD_NAME,
                    MP_LOG_CHANNEL_ID,
                    MP_SAMPLE_DESCRIPTION));

    if (result.hasNext()) {
      return extractDataSetMeta(result.next());
    } else {
      return null;
    }
  }

  private ExecutionDataSetMeta extractDataSetMeta(org.neo4j.driver.Record record) {
    ExecutionDataSetMeta setMeta = new ExecutionDataSetMeta();
    setMeta.setSetKey(getString(record, MP_SET_KEY));
    setMeta.setName(getString(record, MP_NAME));
    setMeta.setCopyNr(getString(record, MP_COPY_NR));
    setMeta.setLogChannelId(getString(record, MP_LOG_CHANNEL_ID));
    setMeta.setDescription(getString(record, MP_DESCRIPTION));
    setMeta.setSampleDescription(getString(record, MP_SAMPLE_DESCRIPTION));
    setMeta.setFieldName(getString(record, MP_FIELD_NAME));
    return setMeta;
  }

  @Override
  public Execution findLastExecution(ExecutionType executionType, String name) throws HopException {
    try {
      List<String> ids = getExecutionIds(true, 100);
      for (String id : ids) {
        Execution execution = getExecution(id);
        if (execution.getExecutionType() == executionType && name.equals(execution.getName())) {
          return execution;
        }
      }
      return null;
    } catch (Exception e) {
      throw new HopException(
          "Error looking up the last execution of type " + executionType + " and name " + name, e);
    }
  }

  @Override
  public List<String> findChildIds(ExecutionType parentExecutionType, String parentExecutionId)
      throws HopException {
    try {
      ExecutionState state = getExecutionState(parentExecutionId);
      if (state != null) {
        return state.getChildIds();
      }
      return Collections.emptyList();
    } catch (Exception e) {
      throw new HopException(
          "Error finding children of "
              + parentExecutionType.name()
              + " execution "
              + parentExecutionId,
          e);
    }
  }

  @Override
  public String findParentId(String childId) throws HopException {
    try {
      for (String id : getExecutionIds(true, 100)) {
        ExecutionState executionState = getExecutionState(id);
        List<String> childIds = executionState.getChildIds();
        if (childIds != null && childIds.contains(childId)) {
          return id;
        }
      }
      return null;
    } catch (Exception e) {
      throw new HopException("Error finding parent execution for child ID " + childId, e);
    }
  }

  private Result execute(TransactionContext transaction, ICypherBuilder builder) {
    return transaction.run(builder.cypher(), builder.parameters());
  }

  private Date getDate(org.neo4j.driver.Record record, String key) {
    return getDate("n", record, key);
  }

  private Date getDate(String nodeAlias, org.neo4j.driver.Record record, String key) {
    Value value = record.get(nodeAlias + "." + key);
    if (value == null) {
      return null;
    }
    if (value.isNull()) {
      return null;
    }
    LocalDateTime localDateTime = value.asLocalDateTime();
    if (localDateTime == null) {
      return null;
    }
    return Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant());
  }

  private String getString(org.neo4j.driver.Record record, String key) {
    return getString("n", record, key);
  }

  private String getString(String nodeAlias, org.neo4j.driver.Record record, String key) {
    Value value = record.get(nodeAlias + "." + key);
    if (value == null) {
      return null;
    }
    if (value.isNull()) {
      return null;
    }
    return value.asString();
  }

  private boolean getBoolean(org.neo4j.driver.Record record, String key) {
    return getBoolean("n", record, key);
  }

  private boolean getBoolean(String nodeAlias, org.neo4j.driver.Record record, String key) {
    Value value = record.get(nodeAlias + "." + key);
    if (value == null) {
      return false;
    }
    if (value.isNull()) {
      return false;
    }
    return value.asBoolean();
  }

  private Long getLong(org.neo4j.driver.Record record, String key) {
    return getLong("n", record, key);
  }

  private Long getLong(String nodeAlias, org.neo4j.driver.Record record, String key) {
    Value value = record.get(nodeAlias + "." + key);
    if (value == null) {
      return null;
    }
    if (value.isNull()) {
      return null;
    }
    return value.asLong();
  }

  private List<String> getList(org.neo4j.driver.Record record, String key) {
    return getList("n", record, key);
  }

  private List<String> getList(String nodeAlias, org.neo4j.driver.Record record, String key) {
    Value value = record.get(nodeAlias + "." + key);
    if (value == null) {
      return null;
    }
    if (value.isNull()) {
      return null;
    }
    return value.asList(Value::asString);
  }

  private Map<String, String> getMap(org.neo4j.driver.Record record, String key) {
    return getMap("n", record, key);
  }

  private Map<String, String> getMap(String nodeAlias, org.neo4j.driver.Record record, String key) {
    Value value = record.get(nodeAlias + "." + key);
    if (value == null) {
      return null;
    }
    if (value.isNull()) {
      return null;
    }
    // We get a JSON String as a result
    //
    String jsonString = value.asString();

    // Convert this to a Map<String,String>
    //
    ObjectMapper objectMapper = HopJson.newMapper();
    TypeReference<HashMap<String, String>> typeRef = new TypeReference<>() {};

    try {
      return objectMapper.readValue(jsonString, typeRef);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Error reading converting JSON String to a Map: " + jsonString, e);
    }
  }

  /**
   * Gets pluginId
   *
   * @return value of pluginId
   */
  @Override
  public String getPluginId() {
    return pluginId;
  }

  /**
   * Sets pluginId
   *
   * @param pluginId value of pluginId
   */
  @Override
  public void setPluginId(String pluginId) {
    this.pluginId = pluginId;
  }

  /**
   * Gets pluginName
   *
   * @return value of pluginName
   */
  @Override
  public String getPluginName() {
    return pluginName;
  }

  /**
   * Sets pluginName
   *
   * @param pluginName value of pluginName
   */
  @Override
  public void setPluginName(String pluginName) {
    this.pluginName = pluginName;
  }

  /**
   * Gets connectionName
   *
   * @return value of connectionName
   */
  public String getConnectionName() {
    return connectionName;
  }

  /**
   * Sets connectionName
   *
   * @param connectionName value of connectionName
   */
  public void setConnectionName(String connectionName) {
    this.connectionName = connectionName;
  }

  /**
   * Gets driver
   *
   * @return value of driver
   */
  public Driver getDriver() {
    return driver;
  }

  /**
   * Gets session
   *
   * @return value of session
   */
  public Session getSession() {
    return session;
  }
}
