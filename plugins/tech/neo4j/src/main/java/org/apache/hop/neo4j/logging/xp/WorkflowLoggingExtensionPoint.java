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

package org.apache.hop.neo4j.logging.xp;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.graph.IGraphConnection;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LoggingHierarchy;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.neo4j.logging.util.LoggingCore;
import org.apache.hop.neo4j.shared.NamedGraphConnection;
import org.apache.hop.workflow.ActionResult;
import org.apache.hop.workflow.WorkflowHopMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;

@ExtensionPoint(
    id = "WorkflowLoggingExtensionPoint",
    extensionPointId = "WorkflowStart",
    description = "Handle logging to a graph database for a workflow")
public class WorkflowLoggingExtensionPoint
    implements IExtensionPoint<IWorkflowEngine<WorkflowMeta>> {

  public static final String WORKFLOW_START_DATE = "WORKFLOW_START_DATE";
  public static final String WORKFLOW_END_DATE = "WORKFLOW_END_DATE";

  public static final String EXECUTION_TYPE_WORKFLOW = LoggingObjectType.WORKFLOW.name();
  public static final String EXECUTION_TYPE_ACTION = LoggingObjectType.ACTION.name();
  public static final String CONST_WORKFLOW_NAME = "workflowName";

  @Override
  public void callExtensionPoint(
      ILogChannel log, IVariables variables, IWorkflowEngine<WorkflowMeta> workflow)
      throws HopException {
    // See if logging is enabled
    //
    if (!LoggingCore.isEnabled(workflow)) {
      return;
    }

    // Keep the start date
    //
    workflow.getExtensionDataMap().put(WORKFLOW_START_DATE, new Date());

    String connectionName = LoggingCore.getConnectionName(workflow);

    try {
      final NamedGraphConnection namedConnection =
          LoggingCore.findConnection(log, workflow.getMetadataProvider(), workflow);
      if (namedConnection == null) {
        return;
      }
      log.logDetailed(
          "Logging workflow information to graph database connection : " + connectionName);

      final IGraphConnection connection = namedConnection.connect(log, variables);

      logWorkflowMetadata(log, connection, connectionName, workflow);
      logStartOfWorkflow(log, connection, connectionName, workflow);

      workflow.addWorkflowFinishedListener(
          workflowMetaIWorkflowEngine -> {
            try {
              logEndOfWorkflow(log, connection, connectionName, workflow);

              // If there are no other parents, we now have the complete log channel hierarchy
              //
              if (workflow.getParentWorkflow() == null && workflow.getParentPipeline() == null) {
                String logChannelId = workflow.getLogChannelId();
                List<LoggingHierarchy> loggingHierarchy =
                    LoggingCore.getLoggingHierarchy(logChannelId);
                logHierarchy(log, connection, connectionName, loggingHierarchy, logChannelId);
              }
            } finally {
              LoggingCore.close(log, connection);
            }
          });

    } catch (Exception e) {
      // Let's not kill the workflow just yet, just log the error
      // otherwise: throw new HopException(...)
      //
      log.logError("Error logging to graph database connection " + connectionName, e);
    }
  }

  private void logWorkflowMetadata(
      final ILogChannel log,
      final IGraphConnection connection,
      final String connectionName,
      final IWorkflowEngine<WorkflowMeta> workflow) {
    log.logDetailed("Logging workflow metadata to graph database connection : " + connectionName);

    final WorkflowMeta workflowMeta = workflow.getWorkflowMeta();

    LoggingCore.write(
        log,
        connection,
        transaction -> {
          try {

            Map<String, Object> workflowPars = new HashMap<>();
            workflowPars.put(CONST_WORKFLOW_NAME, workflowMeta.getName());
            workflowPars.put("description", workflowMeta.getDescription());
            workflowPars.put("filename", workflowMeta.getFilename());
            StringBuilder workflowCypher = new StringBuilder();
            workflowCypher.append("MERGE (w:Workflow { name : $workflowName} ) ");
            workflowCypher.append("SET w.filename = $filename, w.description = $description ");

            log.logDetailed("Workflow metadata cypher : " + workflowCypher);
            transaction.execute(workflowCypher.toString(), workflowPars);

            for (ActionMeta actionMeta : workflowMeta.getActions()) {

              Map<String, Object> actionPars = new HashMap<>();
              actionPars.put(CONST_WORKFLOW_NAME, workflowMeta.getName());
              actionPars.put("name", actionMeta.getName());
              actionPars.put("description", actionMeta.getDescription());
              actionPars.put("pluginId", actionMeta.getAction().getPluginId());
              actionPars.put("evaluation", actionMeta.isEvaluation());
              actionPars.put("launchingParallel", actionMeta.isLaunchingInParallel());
              actionPars.put("start", actionMeta.isStart());
              actionPars.put("unconditional", actionMeta.isUnconditional());
              actionPars.put("locationX", actionMeta.getLocation().x);
              actionPars.put("locationY", actionMeta.getLocation().y);

              StringBuilder actionCypher = new StringBuilder();
              actionCypher.append("MATCH (w:Workflow { name : $workflowName } ) ");
              actionCypher.append(
                  "MERGE (a:Action { workflowName : $workflowName, name : $name }) ");
              actionCypher.append("MERGE (a)-[rel:ACTION_OF_WORKFLOW]->(w) ");
              actionCypher.append("SET ");
              actionCypher.append("  a.description = $description ");
              actionCypher.append(", a.pluginId = $pluginId ");
              actionCypher.append(", a.evaluation = $evaluation ");
              actionCypher.append(", a.launchingParallel = $launchingParallel ");
              actionCypher.append(", a.start = $start ");
              actionCypher.append(", a.unconditional = $unconditional ");
              actionCypher.append(", a.locationX = $locationX ");
              actionCypher.append(", a.locationY = $locationY ");

              // run it
              //
              log.logDetailed(
                  "Action copy '" + actionMeta.getName() + "' cypher : " + actionCypher);
              transaction.execute(actionCypher.toString(), actionPars);
            }

            // Save hops
            //
            for (int i = 0; i < workflowMeta.nrWorkflowHops(); i++) {
              WorkflowHopMeta hopMeta = workflowMeta.getWorkflowHop(i);

              Map<String, Object> hopPars = new HashMap<>();
              hopPars.put("fromAction", hopMeta.getFromAction().getName());
              hopPars.put("toAction", hopMeta.getToAction().getName());
              hopPars.put(CONST_WORKFLOW_NAME, workflowMeta.getName());

              StringBuilder hopCypher = new StringBuilder();
              hopCypher.append(
                  "MATCH (f:Action { workflowName : $workflowName, name : $fromAction}) ");
              hopCypher.append(
                  "MATCH (t:Action { workflowName : $workflowName, name : $toAction}) ");
              hopCypher.append("MERGE (f)-[rel:PRECEDES]->(t) ");
              transaction.execute(hopCypher.toString(), hopPars);
            }

            // Transaction is automatically committed by executeWrite
          } catch (Exception e) {
            // Transaction is automatically rolled back by executeWrite on exception
            log.logError("Error logging workflow metadata", e);
          }
          return null;
        });
  }

  private void logStartOfWorkflow(
      final ILogChannel log,
      final IGraphConnection connection,
      final String connectionName,
      final IWorkflowEngine<WorkflowMeta> workflow) {
    log.logDetailed(
        "Logging execution start of workflow to graph database connection : " + connectionName);

    final WorkflowMeta workflowMeta = workflow.getWorkflowMeta();

    LoggingCore.write(
        log,
        connection,
        transaction -> {
          try {
            // Create a new node for each log channel and it's owner
            // Start with the workflow
            //
            ILogChannel channel = workflow.getLogChannel();
            Date startDate = (Date) workflow.getExtensionDataMap().get(WORKFLOW_START_DATE);

            Map<String, Object> workflowPars = new HashMap<>();
            workflowPars.put(CONST_WORKFLOW_NAME, workflowMeta.getName());
            workflowPars.put("id", channel.getLogChannelId());
            workflowPars.put("type", EXECUTION_TYPE_WORKFLOW);
            workflowPars.put("containerId", workflow.getContainerId());
            workflowPars.put(
                "executionStart", new SimpleDateFormat("yyyy/MM/dd'T'HH:mm:ss").format(startDate));

            StringBuilder workflowCypher = new StringBuilder();
            workflowCypher.append("MATCH (w:Workflow { name : $workflowName} ) ");
            workflowCypher.append(
                "MERGE (e:Execution { name : $workflowName, type : $type, id : $id} ) ");
            workflowCypher.append("SET ");
            workflowCypher.append("  e.executionStart = $executionStart ");
            workflowCypher.append(", e.containerId = $containerId ");
            workflowCypher.append("MERGE (e)-[r:EXECUTION_OF_WORKFLOW]->(w) ");

            transaction.execute(workflowCypher.toString(), workflowPars);

            // Transaction is automatically committed by executeWrite
          } catch (Exception e) {
            // Transaction is automatically rolled back by executeWrite on exception
            log.logError("Error logging workflow start", e);
          }

          return null;
        });
  }

  private void logEndOfWorkflow(
      final ILogChannel log,
      final IGraphConnection connection,
      final String connectionName,
      final IWorkflowEngine<WorkflowMeta> workflow) {
    log.logDetailed(
        "Logging execution end of workflow to graph database connection : " + connectionName);

    final WorkflowMeta workflowMeta = workflow.getWorkflowMeta();

    LoggingCore.write(
        log,
        connection,
        transaction -> {
          try {

            // Create a new node for each log channel and it's owner
            // Start with the workflow
            //
            ILogChannel channel = workflow.getLogChannel();
            Result workflowResult = workflow.getResult();
            String workflowLogChannelId = workflow.getLogChannelId();
            String workflowLoggingText =
                HopLogStore.getAppender().getBuffer(workflowLogChannelId, true).toString();

            Date endDate = new Date();
            workflow.getExtensionDataMap().put(WORKFLOW_END_DATE, new Date());
            Date startDate = (Date) workflow.getExtensionDataMap().get(WORKFLOW_START_DATE);

            Map<String, Object> workflowPars = new HashMap<>();
            workflowPars.put(CONST_WORKFLOW_NAME, workflowMeta.getName());
            workflowPars.put("type", EXECUTION_TYPE_WORKFLOW);
            workflowPars.put("id", channel.getLogChannelId());
            workflowPars.put(
                "executionEnd", new SimpleDateFormat("yyyy/MM/dd'T'HH:mm:ss").format(endDate));
            workflowPars.put("durationMs", endDate.getTime() - startDate.getTime());
            workflowPars.put("errors", workflowResult.getNrErrors());
            workflowPars.put("linesInput", workflowResult.getNrLinesInput());
            workflowPars.put("linesOutput", workflowResult.getNrLinesOutput());
            workflowPars.put("linesRead", workflowResult.getNrLinesRead());
            workflowPars.put("linesWritten", workflowResult.getNrLinesWritten());
            workflowPars.put("linesRejected", workflowResult.getNrLinesRejected());
            workflowPars.put("loggingText", workflowLoggingText);
            workflowPars.put("result", workflowResult.isResult());
            workflowPars.put("nrResultRows", workflowResult.getRows().size());
            workflowPars.put("nrResultFiles", workflowResult.getResultFilesList().size());
            workflowPars.put("containerId", workflowResult.getContainerId());
            workflowPars.put("status", workflow.getStatusDescription());

            String execCypher =
                "MERGE (e:Execution { name : $workflowName, type : $type, id : $id } ) "
                    + "SET "
                    + "  e.executionEnd = $executionEnd "
                    + ", e.durationMs = $durationMs "
                    + ", e.errors = $errors "
                    + ", e.linesInput = $linesInput "
                    + ", e.linesOutput = $linesOutput "
                    + ", e.linesRead = $linesRead "
                    + ", e.linesWritten = $linesWritten "
                    + ", e.linesRejected = $linesRejected "
                    + ", e.loggingText = $loggingText "
                    + ", e.result = $result "
                    + ", e.nrResultRows = $nrResultRows "
                    + ", e.nrResultFiles = $nrResultFiles "
                    + ", e.containerId = $containerId "
                    + ", e.status = $status ";
            transaction.execute(execCypher, workflowPars);

            String relCypher =
                "MATCH (w:Workflow { name : $workflowName } ) "
                    + "MATCH (e:Execution { name : $workflowName, type : $type, id : $id } ) "
                    + "MERGE (e)-[r:EXECUTION_OF_WORKFLOW]->(w) ";
            transaction.execute(relCypher, workflowPars);

            // Also log every workflow action execution results.
            //
            List<ActionResult> actionResults = workflow.getActionResults();
            for (ActionResult actionResult : actionResults) {
              String actionLogChannelId = actionResult.getLogChannelId();
              String transformLoggingText =
                  HopLogStore.getAppender().getBuffer(actionLogChannelId, true).toString();
              Result result = actionResult.getResult();
              Map<String, Object> actionPars = new HashMap<>();
              actionPars.put(CONST_WORKFLOW_NAME, workflowMeta.getName());
              actionPars.put("name", actionResult.getActionName());
              actionPars.put("type", EXECUTION_TYPE_ACTION);
              actionPars.put("id", actionLogChannelId);
              actionPars.put("workflowId", workflowLogChannelId);
              actionPars.put("comment", actionResult.getComment());
              actionPars.put("reason", actionResult.getReason());
              actionPars.put("loggingText", transformLoggingText);
              actionPars.put("errors", result.getNrErrors());
              actionPars.put("linesRead", result.getNrLinesRead());
              actionPars.put("linesWritten", result.getNrLinesWritten());
              actionPars.put("linesInput", result.getNrLinesInput());
              actionPars.put("linesOutput", result.getNrLinesOutput());
              actionPars.put("linesRejected", result.getNrLinesRejected());

              String actionExecCypher =
                  "MERGE (e:Execution { name : $name, type : $type, id : $id } ) "
                      + "SET "
                      + "  e.workflowId = $workflowId "
                      + ", e.loggingText = $loggingText "
                      + ", e.comment = $comment "
                      + ", e.reason = $reason "
                      // Without the error count a failed action never shows up in the
                      // error path of the Neo4j perspective. See issue #7662.
                      //
                      + ", e.errors = $errors "
                      + ", e.linesRead = $linesRead "
                      + ", e.linesWritten = $linesWritten "
                      + ", e.linesInput = $linesInput "
                      + ", e.linesOutput = $linesOutput "
                      + ", e.linesRejected = $linesRejected ";
              transaction.execute(actionExecCypher, actionPars);

              String actionRelCypher =
                  "MATCH (a:Action { workflowName : $workflowName, name : $name } ) "
                      + "MATCH (e:Execution { name : $name, type : $type, id : $id } ) "
                      + "MERGE (e)-[r:EXECUTION_OF_ACTION]->(a) ";
              transaction.execute(actionRelCypher, actionPars);
            }

            // Transaction is automatically committed by executeWrite
          } catch (Exception e) {
            log.logError("Error logging workflow end", e);
            // Transaction is automatically rolled back by executeWrite on exception
          }
          return null;
        });
  }

  private void logHierarchy(
      final ILogChannel log,
      final IGraphConnection connection,
      final String connectionName,
      final List<LoggingHierarchy> hierarchies,
      String rootLogChannelId) {

    LoggingCore.write(
        log,
        connection,
        transaction -> {
          // Update create the Execution relationships
          //
          LoggingCore.writeHierarchies(log, transaction, hierarchies, rootLogChannelId);
          return null;
        });
  }
}
