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

import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LoggingHierarchy;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.neo4j.logging.Defaults;
import org.apache.hop.neo4j.logging.util.LoggingCore;
import org.apache.hop.neo4j.shared.NeoConnection;
import org.apache.hop.pipeline.IExecutionFinishedListener;
import org.apache.hop.workflow.ActionResult;
import org.apache.hop.workflow.WorkflowHopMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionWork;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ExtensionPoint(
    id = "WorkflowLoggingExtensionPoint",
    extensionPointId = "WorkflowStart",
    description = "Handle logging to Neo4j for a workflow")
public class WorkflowLoggingExtensionPoint
    implements IExtensionPoint<IWorkflowEngine<WorkflowMeta>> {

  public static final String WORKFLOW_START_DATE = "WORKFLOW_START_DATE";
  public static final String WORKFLOW_END_DATE = "WORKFLOW_END_DATE";

  public static final String EXECUTION_TYPE_WORKFLOW = LoggingObjectType.WORKFLOW.name();
  public static final String EXECUTION_TYPE_ACTION = LoggingObjectType.ACTION.name();

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

    String connectionName = workflow.getVariable(Defaults.VARIABLE_NEO4J_LOGGING_CONNECTION);

    try {
      final NeoConnection connection =
          LoggingCore.getConnection(workflow.getMetadataProvider(), workflow);
      if (connection == null) {
        log.logBasic("Warning! Unable to find Neo4j connection to log to : " + connectionName);
        return;
      }
      log.logDetailed("Logging workflow information to Neo4j connection : " + connection.getName());

      final Driver driver = connection.getDriver(log, variables);
      final Session session = connection.getSession(log, driver, variables);

      logWorkflowMetadata(log, session, connection, workflow);
      logStartOfWorkflow(log, session, connection, workflow);

      workflow.addWorkflowFinishedListener(
          new IExecutionFinishedListener<IWorkflowEngine<WorkflowMeta>>() {
            @Override
            public void finished(IWorkflowEngine<WorkflowMeta> workflowMetaIWorkflowEngine)
                throws HopException {
              logEndOfWorkflow(log, session, connection, workflow);

              // If there are no other parents, we now have the complete log channel hierarchy
              //
              if (workflow.getParentWorkflow() == null && workflow.getParentPipeline() == null) {
                String logChannelId = workflow.getLogChannelId();
                List<LoggingHierarchy> loggingHierarchy =
                    LoggingCore.getLoggingHierarchy(logChannelId);
                logHierarchy(log, session, connection, loggingHierarchy, logChannelId);
              }

              // Let's not forget to close the session and driver...
              //
              if (session != null) {
                session.close();
              }
              if (driver != null) {
                driver.close();
              }
            }
          });

    } catch (Exception e) {
      // Let's not kill the workflow just yet, just log the error
      // otherwise: throw new HopException(...);
      //
      log.logError("Error logging to Neo4j:", e);
    }
  }

  private void logWorkflowMetadata(
      final ILogChannel log,
      final Session session,
      final NeoConnection connection,
      final IWorkflowEngine<WorkflowMeta> workflow)
      throws HopException {
    log.logDetailed("Logging workflow metadata to Neo4j server : " + connection.getName());

    final WorkflowMeta workflowMeta = workflow.getWorkflowMeta();

    synchronized (session) {
      session.writeTransaction(
          (TransactionWork<Void>)
              transaction -> {
                try {

                  Map<String, Object> workflowPars = new HashMap<>();
                  workflowPars.put("workflowName", workflowMeta.getName());
                  workflowPars.put("description", workflowMeta.getDescription());
                  workflowPars.put("filename", workflowMeta.getFilename());
                  StringBuilder workflowCypher = new StringBuilder();
                  workflowCypher.append("MERGE (w:Workflow { name : $workflowName} ) ");
                  workflowCypher.append(
                      "SET w.filename = $filename, w.description = $description ");

                  log.logDetailed("Workflow metadata cypher : " + workflowCypher);
                  transaction.run(workflowCypher.toString(), workflowPars);

                  for (ActionMeta actionMeta : workflowMeta.getActions()) {

                    Map<String, Object> actionPars = new HashMap<>();
                    actionPars.put("workflowName", workflowMeta.getName());
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
                    transaction.run(actionCypher.toString(), actionPars);
                  }

                  // Save hops
                  //
                  for (int i = 0; i < workflowMeta.nrWorkflowHops(); i++) {
                    WorkflowHopMeta hopMeta = workflowMeta.getWorkflowHop(i);

                    Map<String, Object> hopPars = new HashMap<>();
                    hopPars.put("fromAction", hopMeta.getFromAction().getName());
                    hopPars.put("toAction", hopMeta.getToAction().getName());
                    hopPars.put("workflowName", workflowMeta.getName());

                    StringBuilder hopCypher = new StringBuilder();
                    hopCypher.append(
                        "MATCH (f:Action { workflowName : $workflowName, name : $fromAction}) ");
                    hopCypher.append(
                        "MATCH (t:Action { workflowName : $workflowName, name : $toAction}) ");
                    hopCypher.append("MERGE (f)-[rel:PRECEDES]->(t) ");
                    transaction.run(hopCypher.toString(), hopPars);
                  }

                  transaction.commit();
                } catch (Exception e) {
                  transaction.rollback();
                  log.logError("Error logging workflow metadata", e);
                }
                return null;
              });
    }
  }

  private void logStartOfWorkflow(
      final ILogChannel log,
      final Session session,
      final NeoConnection connection,
      final IWorkflowEngine<WorkflowMeta> workflow)
      throws HopException {
    log.logDetailed(
        "Logging execution start of workflow to Neo4j connection : " + connection.getName());

    final WorkflowMeta workflowMeta = workflow.getWorkflowMeta();

    synchronized (session) {
      session.writeTransaction(
          new TransactionWork<Void>() {
            @Override
            public Void execute(Transaction transaction) {
              try {
                // Create a new node for each log channel and it's owner
                // Start with the workflow
                //
                ILogChannel channel = workflow.getLogChannel();
                Date startDate = (Date) workflow.getExtensionDataMap().get(WORKFLOW_START_DATE);

                Map<String, Object> workflowPars = new HashMap<>();
                workflowPars.put("workflowName", workflowMeta.getName());
                workflowPars.put("id", channel.getLogChannelId());
                workflowPars.put("type", EXECUTION_TYPE_WORKFLOW);
                workflowPars.put(
                    "executionStart",
                    new SimpleDateFormat("yyyy/MM/dd'T'HH:mm:ss").format(startDate));

                StringBuilder workflowCypher = new StringBuilder();
                workflowCypher.append("MATCH (w:Workflow { name : $workflowName} ) ");
                workflowCypher.append(
                    "MERGE (e:Execution { name : $workflowName, type : $type, id : $id} ) ");
                workflowCypher.append("SET ");
                workflowCypher.append(" e.executionStart = $executionStart ");
                workflowCypher.append("MERGE (e)-[r:EXECUTION_OF_WORKFLOW]->(w) ");

                transaction.run(workflowCypher.toString(), workflowPars);

                transaction.commit();
              } catch (Exception e) {
                transaction.rollback();
                log.logError("Error logging workflow start", e);
              }

              return null;
            }
          });
    }
  }

  private void logEndOfWorkflow(
      final ILogChannel log,
      final Session session,
      final NeoConnection connection,
      final IWorkflowEngine<WorkflowMeta> workflow)
      throws HopException {
    log.logDetailed(
        "Logging execution end of workflow to Neo4j connection : " + connection.getName());

    final WorkflowMeta workflowMeta = workflow.getWorkflowMeta();

    synchronized (session) {
      session.writeTransaction(
          (TransactionWork<Void>)
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
                  workflowPars.put("workflowName", workflowMeta.getName());
                  workflowPars.put("type", EXECUTION_TYPE_WORKFLOW);
                  workflowPars.put("id", channel.getLogChannelId());
                  workflowPars.put(
                      "executionEnd",
                      new SimpleDateFormat("yyyy/MM/dd'T'HH:mm:ss").format(endDate));
                  workflowPars.put("durationMs", endDate.getTime() - startDate.getTime());
                  workflowPars.put("errors", workflowResult.getNrErrors());
                  workflowPars.put("linesInput", workflowResult.getNrLinesInput());
                  workflowPars.put("linesOutput", workflowResult.getNrLinesOutput());
                  workflowPars.put("linesRead", workflowResult.getNrLinesRead());
                  workflowPars.put("linesWritten", workflowResult.getNrLinesWritten());
                  workflowPars.put("linesRejected", workflowResult.getNrLinesRejected());
                  workflowPars.put("loggingText", workflowLoggingText);
                  workflowPars.put("result", workflowResult.getResult());
                  workflowPars.put("nrResultRows", workflowResult.getRows().size());
                  workflowPars.put("nrResultFiles", workflowResult.getResultFilesList().size());

                  StringBuilder execCypher = new StringBuilder();
                  execCypher.append(
                      "MERGE (e:Execution { name : $workflowName, type : $type, id : $id } ) ");
                  execCypher.append("SET ");
                  execCypher.append("  e.executionEnd = $executionEnd ");
                  execCypher.append(", e.durationMs = $durationMs ");
                  execCypher.append(", e.errors = $errors ");
                  execCypher.append(", e.linesInput = $linesInput ");
                  execCypher.append(", e.linesOutput = $linesOutput ");
                  execCypher.append(", e.linesRead = $linesRead ");
                  execCypher.append(", e.linesWritten = $linesWritten ");
                  execCypher.append(", e.linesRejected = $linesRejected ");
                  execCypher.append(", e.loggingText = $loggingText ");
                  execCypher.append(", e.result = $result ");
                  execCypher.append(", e.nrResultRows = $nrResultRows ");
                  execCypher.append(", e.nrResultFiles = $nrResultFiles ");
                  transaction.run(execCypher.toString(), workflowPars);

                  StringBuilder relCypher = new StringBuilder();
                  relCypher.append("MATCH (w:Workflow { name : $workflowName } ) ");
                  relCypher.append(
                      "MATCH (e:Execution { name : $workflowName, type : $type, id : $id } ) ");
                  relCypher.append("MERGE (e)-[r:EXECUTION_OF_WORKFLOW]->(w) ");
                  transaction.run(relCypher.toString(), workflowPars);

                  // Also log every workflow action execution results.
                  //
                  List<ActionResult> actionResults = workflow.getActionResults();
                  for (ActionResult actionResult : actionResults) {
                    String actionLogChannelId = actionResult.getLogChannelId();
                    String transformLoggingText =
                        HopLogStore.getAppender().getBuffer(actionLogChannelId, true).toString();
                    Result result = actionResult.getResult();
                    Map<String, Object> actionPars = new HashMap<>();
                    actionPars.put("workflowName", workflowMeta.getName());
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

                    StringBuilder actionExecCypher = new StringBuilder();
                    actionExecCypher.append(
                        "MERGE (e:Execution { name : $name, type : $type, id : $id } ) ");
                    actionExecCypher.append("SET ");
                    actionExecCypher.append("  e.workflowId = $workflowId ");
                    actionExecCypher.append(", e.loggingText = $loggingText ");
                    actionExecCypher.append(", e.comment = $comment ");
                    actionExecCypher.append(", e.reason = $reason ");
                    actionExecCypher.append(", e.linesRead = $linesRead ");
                    actionExecCypher.append(", e.linesWritten = $linesWritten ");
                    actionExecCypher.append(", e.linesInput = $linesInput ");
                    actionExecCypher.append(", e.linesOutput = $linesOutput ");
                    actionExecCypher.append(", e.linesRejected = $linesRejected ");
                    transaction.run(actionExecCypher.toString(), actionPars);

                    StringBuilder actionRelCypher = new StringBuilder();
                    actionRelCypher.append(
                        "MATCH (a:Action { workflowName : $workflowName, name : $name } ) ");
                    actionRelCypher.append(
                        "MATCH (e:Execution { name : $name, type : $type, id : $id } ) ");
                    actionRelCypher.append("MERGE (e)-[r:EXECUTION_OF_ACTION]->(a) ");
                    transaction.run(actionRelCypher.toString(), actionPars);
                  }

                  transaction.commit();
                } catch (Exception e) {
                  log.logError("Error logging workflow end", e);
                  transaction.rollback();
                }
                return null;
              });
    }
  }

  private void logHierarchy(
      final ILogChannel log,
      final Session session,
      final NeoConnection connection,
      final List<LoggingHierarchy> hierarchies,
      String rootLogChannelId) {

    synchronized (session) {
      session.writeTransaction(
          new TransactionWork<Void>() {
            @Override
            public Void execute(Transaction transaction) {
              // Update create the Execution relationships
              //
              LoggingCore.writeHierarchies(
                  log, connection, transaction, hierarchies, rootLogChannelId);
              return null;
            }
          });
    }
  }
}
