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
import java.util.Set;
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
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaDataCombi;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.TransactionCallback;

@ExtensionPoint(
    id = "PipelineLoggingExtensionPoint",
    extensionPointId = "PipelineStartThreads",
    description = "Handle logging to Neo4j for a pipeline")
public class PipelineLoggingExtensionPoint
    implements IExtensionPoint<IPipelineEngine<PipelineMeta>> {

  public static final String PIPELINE_START_DATE = "PIPELINE_START_DATE";
  public static final String PIPELINE_END_DATE = "PIPELINE_END_DATE";

  public static final String EXECUTION_TYPE_PIPELINE = LoggingObjectType.PIPELINE.name();
  public static final String EXECUTION_TYPE_TRANSFORM = LoggingObjectType.TRANSFORM.name();
  public static final String CONST_STATUS = "status";
  public static final String CONST_PIPELINE_NAME = "pipelineName";

  @Override
  public void callExtensionPoint(
      ILogChannel log, IVariables variables, IPipelineEngine<PipelineMeta> pipeline)
      throws HopException {

    // See if logging is enabled
    //
    if (!LoggingCore.isEnabled(pipeline)) {
      return;
    }

    // Keep the start date
    //
    pipeline.getExtensionDataMap().put(PIPELINE_START_DATE, new Date());

    String connectionName = pipeline.getVariable(Defaults.NEO4J_LOGGING_CONNECTION);

    try {

      final NeoConnection connection =
          LoggingCore.getConnection(pipeline.getMetadataProvider(), pipeline);
      if (connection == null) {
        log.logBasic("Warning! Unable to find Neo4j connection to log to : " + connectionName);
        return;
      }
      log.logDetailed("Logging pipeline information to Neo4j connection : " + connection.getName());

      final Driver driver = connection.getDriver(log, variables);
      final Session session = connection.getSession(log, driver, variables);

      logPipelineMetadata(log, session, connection, pipeline);
      logStartOfPipeline(log, session, connection, pipeline);

      pipeline.addExecutionFinishedListener(
          pipelineEngine -> {
            logEndOfPipeline(log, session, connection, pipelineEngine);

            // If there are no other parents, we now have the complete log channel hierarchy
            //
            if (pipelineEngine.getParentWorkflow() == null
                && pipelineEngine.getParentPipeline() == null) {
              String logChannelId = pipelineEngine.getLogChannelId();
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
          });
    } catch (Exception e) {
      // Let's not kill the pipeline just yet, just log the error
      // otherwise: throw new HopException(...)
      //
      log.logError("Error logging to Neo4j:", e);
    }
  }

  private void logPipelineMetadata(
      final ILogChannel log,
      final Session session,
      final NeoConnection connection,
      final IPipelineEngine<PipelineMeta> pipeline) {
    log.logDetailed("Logging pipeline metadata to Neo4j connection : " + connection.getName());

    final PipelineMeta pipelineMeta = pipeline.getPipelineMeta();

    synchronized (session) {
      session.executeWrite(
          (TransactionCallback<Void>)
              transaction -> {
                try {

                  Map<String, Object> transPars = new HashMap<>();
                  transPars.put(CONST_PIPELINE_NAME, pipelineMeta.getName());
                  transPars.put("description", pipelineMeta.getDescription());
                  transPars.put("filename", pipelineMeta.getFilename());
                  StringBuilder transCypher = new StringBuilder();
                  transCypher.append("MERGE (pipeline:Pipeline { name : $pipelineName } ) ");
                  transCypher.append(
                      "SET pipeline.filename = $filename, pipeline.description = $description ");
                  transaction.run(transCypher.toString(), transPars);

                  log.logDetailed("Pipeline cypher : " + transCypher);

                  for (TransformMeta transformMeta : pipelineMeta.getTransforms()) {

                    Map<String, Object> transformPars = new HashMap<>();
                    transformPars.put(CONST_PIPELINE_NAME, pipelineMeta.getName());
                    transformPars.put("transformName", transformMeta.getName());
                    transformPars.put("description", transformMeta.getDescription());
                    transformPars.put("pluginId", transformMeta.getPluginId());
                    transformPars.put("copies", transformMeta.getCopies(pipeline));
                    transformPars.put("locationX", transformMeta.getLocation().x);
                    transformPars.put("locationY", transformMeta.getLocation().y);

                    StringBuilder transformCypher = new StringBuilder();
                    transformCypher.append("MATCH (pipeline:Pipeline { name : $pipelineName } ) ");
                    transformCypher.append(
                        "MERGE (transform:Transform { pipelineName : $pipelineName, name : $transformName } ) ");
                    transformCypher.append("SET ");
                    transformCypher.append("  transform.description = $description ");
                    transformCypher.append(", transform.pluginId = $pluginId ");
                    transformCypher.append(", transform.copies = $copies ");
                    transformCypher.append(", transform.locationX = $locationX ");
                    transformCypher.append(", transform.locationY = $locationY ");

                    // Also MERGE the relationship
                    //
                    transformCypher.append(
                        "MERGE (transform)-[rel:TRANSFORM_OF_PIPELINE]->(pipeline) ");

                    log.logDetailed(
                        "Transform '" + transformMeta.getName() + "' cypher : " + transformCypher);

                    // run it
                    //
                    transaction.run(transformCypher.toString(), transformPars);
                  }

                  // Save hops
                  //
                  for (int i = 0; i < pipelineMeta.nrPipelineHops(); i++) {
                    PipelineHopMeta hopMeta = pipelineMeta.getPipelineHop(i);

                    Map<String, Object> hopPars = new HashMap<>();
                    hopPars.put("fromTransform", hopMeta.getFromTransform().getName());
                    hopPars.put("toTransform", hopMeta.getToTransform().getName());
                    hopPars.put(CONST_PIPELINE_NAME, pipelineMeta.getName());

                    String hopCypher =
                        "MATCH (from:Transform { pipelineName : $pipelineName, name : $fromTransform }) "
                            + "MATCH (to:Transform { pipelineName : $pipelineName, name : $toTransform }) "
                            + "MERGE (from)-[rel:PRECEDES]->(to) ";
                    transaction.run(hopCypher, hopPars);
                  }

                  // Transaction is automatically committed by executeWrite
                } catch (Exception e) {
                  // Transaction is automatically rolled back by executeWrite on exception
                  log.logError("Error logging pipeline metadata", e);
                }
                return null;
              });
    }
  }

  private void logStartOfPipeline(
      final ILogChannel log,
      final Session session,
      final NeoConnection connection,
      final IPipelineEngine<PipelineMeta> pipeline) {
    log.logDetailed(
        "Logging execution start of pipeline to Neo4j connection : " + connection.getName());

    final PipelineMeta pipelineMeta = pipeline.getPipelineMeta();

    synchronized (session) {
      session.executeWrite(
          (TransactionCallback<Void>)
              transaction -> {
                try {
                  // Create a new node for each log channel and its owner.
                  // Start with the pipeline.
                  //
                  ILogChannel channel = pipeline.getLogChannel();
                  Date startDate = (Date) pipeline.getExtensionDataMap().get(PIPELINE_START_DATE);

                  Map<String, Object> pipelinePars = new HashMap<>();
                  pipelinePars.put(CONST_PIPELINE_NAME, pipelineMeta.getName());
                  pipelinePars.put("id", channel.getLogChannelId());
                  pipelinePars.put("type", EXECUTION_TYPE_PIPELINE);
                  pipelinePars.put("containerId", pipeline.getContainerId());

                  pipelinePars.put(
                      "executionStart",
                      new SimpleDateFormat("yyyy/MM/dd'T'HH:mm:ss").format(startDate));
                  pipelinePars.put(CONST_STATUS, pipeline.getStatusDescription());

                  String pipelineCypher =
                      "MATCH (pipeline:Pipeline { name : $pipelineName } ) "
                          + "MERGE (exec:Execution { name : $pipelineName, type : $type, id : $id } ) "
                          + "SET "
                          + "  exec.executionStart = $executionStart "
                          + ", exec.status = $status "
                          + ", exec.containerId = $containerId "
                          + "MERGE (exec)-[r:EXECUTION_OF_PIPELINE]->(pipeline) ";

                  transaction.run(pipelineCypher, pipelinePars);

                  // Transaction is automatically committed by executeWrite
                } catch (Exception e) {
                  // Transaction is automatically rolled back by executeWrite on exception
                  log.logError("Error logging pipeline start", e);
                }

                return null;
              });
    }
  }

  private void logEndOfPipeline(
      final ILogChannel log,
      final Session session,
      final NeoConnection connection,
      final IPipelineEngine<PipelineMeta> pipeline) {
    log.logDetailed(
        "Logging execution end of pipeline to Neo4j connection : " + connection.getName());

    final PipelineMeta pipelineMeta = pipeline.getPipelineMeta();

    synchronized (session) {
      session.executeWrite(
          (TransactionCallback<Void>)
              transaction -> {
                try {

                  // Create a new node for each log channel and it's owner
                  // Start with the pipeline
                  //
                  ILogChannel channel = pipeline.getLogChannel();
                  Result result = pipeline.getResult();
                  String transLogChannelId = pipeline.getLogChannelId();
                  String transLoggingText =
                      HopLogStore.getAppender().getBuffer(transLogChannelId, false).toString();
                  Date endDate = new Date();
                  pipeline.getExtensionDataMap().put(PIPELINE_END_DATE, endDate);
                  Date startDate = (Date) pipeline.getExtensionDataMap().get(PIPELINE_START_DATE);

                  Map<String, Object> pipelinePars = new HashMap<>();
                  pipelinePars.put(CONST_PIPELINE_NAME, pipelineMeta.getName());
                  pipelinePars.put("type", EXECUTION_TYPE_PIPELINE);
                  pipelinePars.put("id", channel.getLogChannelId());
                  pipelinePars.put(
                      "executionEnd",
                      new SimpleDateFormat("yyyy/MM/dd'T'HH:mm:ss").format(endDate));
                  pipelinePars.put("durationMs", endDate.getTime() - startDate.getTime());
                  pipelinePars.put("errors", result.getNrErrors());
                  pipelinePars.put("linesInput", result.getNrLinesInput());
                  pipelinePars.put("linesOutput", result.getNrLinesOutput());
                  pipelinePars.put("linesRead", result.getNrLinesRead());
                  pipelinePars.put("linesWritten", result.getNrLinesWritten());
                  pipelinePars.put("linesRejected", result.getNrLinesRejected());
                  pipelinePars.put("loggingText", transLoggingText);
                  pipelinePars.put(CONST_STATUS, pipeline.getStatusDescription());

                  String pipelineCypher =
                      "MATCH (pipeline:Pipeline { name : $pipelineName } ) "
                          + "MERGE (exec:Execution { name : $pipelineName, type : $type, id : $id } ) "
                          + "SET "
                          + "  exec.executionEnd = $executionEnd "
                          + ", exec.durationMs = $durationMs "
                          + ", exec.status = $status "
                          + ", exec.errors = $errors "
                          + ", exec.linesInput = $linesInput "
                          + ", exec.linesOutput = $linesOutput "
                          + ", exec.linesRead = $linesRead "
                          + ", exec.linesWritten = $linesWritten "
                          + ", exec.linesRejected = $linesRejected "
                          + ", exec.loggingText = $loggingText "
                          + "MERGE (exec)-[r:EXECUTION_OF_PIPELINE]->(pipeline) ";

                  transaction.run(pipelineCypher, pipelinePars);

                  // Also log every transform copy
                  //
                  List<TransformMetaDataCombi> combis = ((Pipeline) pipeline).getTransforms();
                  for (TransformMetaDataCombi combi : combis) {
                    String transformLogChannelId =
                        combi.transform.getLogChannel().getLogChannelId();
                    String transformLoggingText =
                        HopLogStore.getAppender()
                            .getBuffer(transformLogChannelId, false)
                            .toString();
                    Map<String, Object> transformPars = new HashMap<>();
                    transformPars.put(CONST_PIPELINE_NAME, pipelineMeta.getName());
                    transformPars.put("name", combi.transformName);
                    transformPars.put("type", EXECUTION_TYPE_TRANSFORM);
                    transformPars.put("id", transformLogChannelId);
                    transformPars.put("transId", transLogChannelId);
                    transformPars.put("copy", (long) combi.copy);
                    transformPars.put(CONST_STATUS, combi.transform.getStatus().getDescription());
                    transformPars.put("loggingText", transformLoggingText);
                    transformPars.put("errors", combi.transform.getErrors());
                    transformPars.put("linesRead", combi.transform.getLinesRead());
                    transformPars.put("linesWritten", combi.transform.getLinesWritten());
                    transformPars.put("linesInput", combi.transform.getLinesInput());
                    transformPars.put("linesOutput", combi.transform.getLinesOutput());
                    transformPars.put("linesRejected", combi.transform.getLinesRejected());

                    String transformCypher =
                        "MATCH (transform:Transform { pipelineName : $pipelineName, name : $name } ) "
                            + "MERGE (exec:Execution { name : $name, type : $type, id : $id } ) "
                            + "SET "
                            + "  exec.transId = $transId "
                            + ", exec.copy = $copy "
                            + ", exec.status = $status "
                            + ", exec.loggingText = $loggingText "
                            + ", exec.errors = $errors "
                            + ", exec.linesRead = $linesRead "
                            + ", exec.linesWritten = $linesWritten "
                            + ", exec.linesInput = $linesInput "
                            + ", exec.linesOutput = $linesOutput "
                            + ", exec.linesRejected = $linesRejected "
                            + "MERGE (exec)-[r:EXECUTION_OF_TRANSFORM]->(transform) ";

                    transaction.run(transformCypher, transformPars);

                    // Log graph usage as well
                    // This Map is left by the Neo4j transform plugins : Neo4j Output and Neo4j
                    // Graph
                    // Output
                    //
                    Map<String, Map<String, Set<String>>> usageMap =
                        (Map<String, Map<String, Set<String>>>)
                            pipeline.getExtensionDataMap().get(Defaults.TRANS_NODE_UPDATES_GROUP);
                    if (usageMap != null) {
                      for (String graphUsage : usageMap.keySet()) {
                        Map<String, Set<String>> transformsMap = usageMap.get(graphUsage);

                        Set<String> labels = transformsMap.get(combi.transformName);
                        if (labels != null) {
                          for (String label : labels) {
                            // Save relationship to GraphUsage node
                            //
                            Map<String, Object> usagePars = new HashMap<>();
                            usagePars.put("transform", combi.transformName);
                            usagePars.put("type", "TRANSFORM");
                            usagePars.put("id", transformLogChannelId);
                            usagePars.put("label", label);
                            usagePars.put("usage", graphUsage);

                            String usageCypher =
                                "MATCH (transform:Execution { name : $transform, type : $type, id : $id } ) "
                                    + "MERGE (usage:Usage { usage : $usage, label : $label } ) "
                                    + "MERGE (transform)-[r:PERFORMS_"
                                    + graphUsage
                                    + "]->(usage)";

                            transaction.run(usageCypher, usagePars);
                          }
                        }
                      }
                    }
                  }

                  // Transaction is automatically committed by executeWrite
                } catch (Exception e) {
                  // Transaction is automatically rolled back by executeWrite on exception
                  log.logError("Error logging pipeline end", e);
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
      session.executeWrite(
          (TransactionCallback<Void>)
              transaction -> {
                // Update create the Execution relationships
                //
                LoggingCore.writeHierarchies(
                    log, connection, transaction, hierarchies, rootLogChannelId);
                return null;
              });
    }
  }
}
