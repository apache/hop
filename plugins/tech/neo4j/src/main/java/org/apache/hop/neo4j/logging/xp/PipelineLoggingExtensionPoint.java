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
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaDataCombi;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionWork;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    String connectionName = pipeline.getVariable(Defaults.VARIABLE_NEO4J_LOGGING_CONNECTION);

    try {

      final NeoConnection connection =
          LoggingCore.getConnection(pipeline.getMetadataProvider(), pipeline);
      if (connection == null) {
        log.logBasic("Warning! Unable to find Neo4j connection to log to : " + connectionName);
        return;
      }
      log.logDetailed("Logging pipeline information to Neo4j connection : " + connection.getName());

      Session session = connection.getSession(log, variables);

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
          });

    } catch (Exception e) {
      // Let's not kill the pipeline just yet, just log the error
      // otherwise: throw new HopException(...);
      //
      log.logError("Error logging to Neo4j:", e);
    }
  }

  private void logPipelineMetadata(
      final ILogChannel log,
      final Session session,
      final NeoConnection connection,
      final IPipelineEngine<PipelineMeta> pipeline)
      throws HopException {
    log.logDetailed("Logging pipeline metadata to Neo4j connection : " + connection.getName());

    final PipelineMeta pipelineMeta = pipeline.getPipelineMeta();

    synchronized (session) {
      session.writeTransaction(
          (TransactionWork<Void>)
              transaction -> {
                try {

                  Map<String, Object> transPars = new HashMap<>();
                  transPars.put("pipelineName", pipelineMeta.getName());
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
                    transformPars.put("pipelineName", pipelineMeta.getName());
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
                    hopPars.put("pipelineName", pipelineMeta.getName());

                    StringBuilder hopCypher = new StringBuilder();
                    hopCypher.append(
                        "MATCH (from:Transform { pipelineName : $pipelineName, name : $fromTransform }) ");
                    hopCypher.append(
                        "MATCH (to:Transform { pipelineName : $pipelineName, name : $toTransform }) ");
                    hopCypher.append("MERGE (from)-[rel:PRECEDES]->(to) ");
                    transaction.run(hopCypher.toString(), hopPars);
                  }

                  transaction.commit();
                } catch (Exception e) {
                  transaction.rollback();
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
      final IPipelineEngine<PipelineMeta> pipeline)
      throws HopException {
    log.logDetailed(
        "Logging execution start of pipeline to Neo4j connection : " + connection.getName());

    final PipelineMeta pipelineMeta = pipeline.getPipelineMeta();

    synchronized (session) {
      session.writeTransaction(
          new TransactionWork<Void>() {
            @Override
            public Void execute(Transaction transaction) {
              try {
                // Create a new node for each log channel and it's owner
                // Start with the pipeline
                //
                ILogChannel channel = pipeline.getLogChannel();
                Date startDate = (Date) pipeline.getExtensionDataMap().get(PIPELINE_START_DATE);

                Map<String, Object> transPars = new HashMap<>();
                transPars.put("pipelineName", pipelineMeta.getName());
                transPars.put("id", channel.getLogChannelId());
                transPars.put("type", EXECUTION_TYPE_PIPELINE);
                transPars.put(
                    "executionStart",
                    new SimpleDateFormat("yyyy/MM/dd'T'HH:mm:ss").format(startDate));
                transPars.put("status", pipeline.getStatusDescription());

                StringBuilder transCypher = new StringBuilder();
                transCypher.append("MATCH (pipeline:Pipeline { name : $pipelineName } ) ");
                transCypher.append(
                    "MERGE (exec:Execution { name : $pipelineName, type : $type, id : $id } ) ");
                transCypher.append("SET ");
                transCypher.append("  exec.executionStart = $executionStart ");
                transCypher.append(", exec.status = $status ");
                transCypher.append("MERGE (exec)-[r:EXECUTION_OF_PIPELINE]->(pipeline) ");

                transaction.run(transCypher.toString(), transPars);

                transaction.commit();
              } catch (Exception e) {
                transaction.rollback();
                log.logError("Error logging pipeline start", e);
              }

              return null;
            }
          });
    }
  }

  private void logEndOfPipeline(
      final ILogChannel log,
      final Session session,
      final NeoConnection connection,
      final IPipelineEngine<PipelineMeta> pipeline)
      throws HopException {
    log.logDetailed(
        "Logging execution end of pipeline to Neo4j connection : " + connection.getName());

    final PipelineMeta pipelineMeta = pipeline.getPipelineMeta();

    synchronized (session) {
      session.writeTransaction(
          new TransactionWork<Void>() {
            @Override
            public Void execute(Transaction transaction) {
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

                Map<String, Object> transPars = new HashMap<>();
                transPars.put("pipelineName", pipelineMeta.getName());
                transPars.put("type", EXECUTION_TYPE_PIPELINE);
                transPars.put("id", channel.getLogChannelId());
                transPars.put(
                    "executionEnd", new SimpleDateFormat("yyyy/MM/dd'T'HH:mm:ss").format(endDate));
                transPars.put("durationMs", endDate.getTime() - startDate.getTime());
                transPars.put("errors", result.getNrErrors());
                transPars.put("linesInput", result.getNrLinesInput());
                transPars.put("linesOutput", result.getNrLinesOutput());
                transPars.put("linesRead", result.getNrLinesRead());
                transPars.put("linesWritten", result.getNrLinesWritten());
                transPars.put("linesRejected", result.getNrLinesRejected());
                transPars.put("loggingText", transLoggingText);
                transPars.put("status", pipeline.getStatusDescription());

                StringBuilder transCypher = new StringBuilder();
                transCypher.append("MATCH (pipeline:Pipeline { name : $pipelineName } ) ");
                transCypher.append(
                    "MERGE (exec:Execution { name : $pipelineName, type : $type, id : $id } ) ");
                transCypher.append("SET ");
                transCypher.append("  exec.executionEnd = $executionEnd ");
                transCypher.append(", exec.durationMs = $durationMs ");
                transCypher.append(", exec.status = $status ");
                transCypher.append(", exec.errors = $errors ");
                transCypher.append(", exec.linesInput = $linesInput ");
                transCypher.append(", exec.linesOutput = $linesOutput ");
                transCypher.append(", exec.linesRead = $linesRead ");
                transCypher.append(", exec.linesWritten = $linesWritten ");
                transCypher.append(", exec.linesRejected = $linesRejected ");
                transCypher.append(", exec.loggingText = $loggingText ");
                transCypher.append("MERGE (exec)-[r:EXECUTION_OF_PIPELINE]->(pipeline) ");

                transaction.run(transCypher.toString(), transPars);

                // Also log every transform copy
                //
                List<TransformMetaDataCombi<ITransform, ITransformMeta, ITransformData>> combis =
                    ((Pipeline) pipeline).getTransforms();
                for (TransformMetaDataCombi combi : combis) {
                  String transformLogChannelId = combi.transform.getLogChannel().getLogChannelId();
                  String transformLoggingText =
                      HopLogStore.getAppender().getBuffer(transformLogChannelId, false).toString();
                  Map<String, Object> transformPars = new HashMap<>();
                  transformPars.put("pipelineName", pipelineMeta.getName());
                  transformPars.put("name", combi.transformName);
                  transformPars.put("type", EXECUTION_TYPE_TRANSFORM);
                  transformPars.put("id", transformLogChannelId);
                  transformPars.put("transId", transLogChannelId);
                  transformPars.put("copy", Long.valueOf(combi.copy));
                  transformPars.put("status", combi.transform.getStatus().getDescription());
                  transformPars.put("loggingText", transformLoggingText);
                  transformPars.put("errors", combi.transform.getErrors());
                  transformPars.put("linesRead", combi.transform.getLinesRead());
                  transformPars.put("linesWritten", combi.transform.getLinesWritten());
                  transformPars.put("linesInput", combi.transform.getLinesInput());
                  transformPars.put("linesOutput", combi.transform.getLinesOutput());
                  transformPars.put("linesRejected", combi.transform.getLinesRejected());

                  StringBuilder transformCypher = new StringBuilder();
                  transformCypher.append(
                      "MATCH (transform:Transform { pipelineName : $pipelineName, name : $name } ) ");
                  transformCypher.append(
                      "MERGE (exec:Execution { name : $name, type : $type, id : $id } ) ");
                  transformCypher.append("SET ");
                  transformCypher.append("  exec.transId = $transId ");
                  transformCypher.append(", exec.copy = $copy ");
                  transformCypher.append(", exec.status = $status ");
                  transformCypher.append(", exec.loggingText = $loggingText ");
                  transformCypher.append(", exec.errors = $errors ");
                  transformCypher.append(", exec.linesRead = $linesRead ");
                  transformCypher.append(", exec.linesWritten = $linesWritten ");
                  transformCypher.append(", exec.linesInput = $linesInput ");
                  transformCypher.append(", exec.linesOutput = $linesOutput ");
                  transformCypher.append(", exec.linesRejected = $linesRejected ");
                  transformCypher.append("MERGE (exec)-[r:EXECUTION_OF_TRANSFORM]->(transform) ");

                  transaction.run(transformCypher.toString(), transformPars);

                  // Log graph usage as well
                  // This Map is left by the Neo4j transform plugins : Neo4j Output and Neo4j Graph
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

                          StringBuilder usageCypher = new StringBuilder();
                          usageCypher.append(
                              "MATCH (transform:Execution { name : $transform, type : $type, id : $id } ) ");
                          usageCypher.append(
                              "MERGE (usage:Usage { usage : $usage, label : $label } ) ");
                          usageCypher.append(
                              "MERGE (transform)-[r:PERFORMS_" + graphUsage + "]->(usage)");

                          transaction.run(usageCypher.toString(), usagePars);
                        }
                      }
                    }
                  }
                }

                transaction.commit();
              } catch (Exception e) {
                transaction.rollback();
                log.logError("Error logging pipeline end", e);
              }
              return null;
            }
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
          (TransactionWork<Void>)
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
