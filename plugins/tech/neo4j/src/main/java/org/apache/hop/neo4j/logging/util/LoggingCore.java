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

package org.apache.hop.neo4j.logging.util;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.logging.LoggingHierarchy;
import org.apache.hop.core.logging.LoggingRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.neo4j.logging.Defaults;
import org.apache.hop.neo4j.shared.NeoConnection;
import org.eclipse.swt.graphics.Rectangle;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.Value;
import org.neo4j.driver.types.Node;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class LoggingCore {

  public static final boolean isEnabled(IVariables space) {
    String connectionName = space.getVariable(Defaults.VARIABLE_NEO4J_LOGGING_CONNECTION);
    return StringUtils.isNotEmpty(connectionName)
        && !Defaults.VARIABLE_NEO4J_LOGGING_CONNECTION_DISABLED.equals(connectionName);
  }

  public static final NeoConnection getConnection(
      IHopMetadataProvider metadataProvider, IVariables space) throws HopException {
    String connectionName = space.getVariable(Defaults.VARIABLE_NEO4J_LOGGING_CONNECTION);
    if (StringUtils.isEmpty(connectionName)) {
      return null;
    }
    IHopMetadataSerializer<NeoConnection> serializer =
        metadataProvider.getSerializer(NeoConnection.class);
    NeoConnection connection = serializer.load(connectionName);
    return connection;
  }

  public static final void writeHierarchies(
      ILogChannel log,
      NeoConnection connection,
      Transaction transaction,
      List<LoggingHierarchy> hierarchies,
      String rootLogChannelId) {

    try {
      // First create the Execution nodes
      //
      for (LoggingHierarchy hierarchy : hierarchies) {
        ILoggingObject loggingObject = hierarchy.getLoggingObject();
        LogLevel logLevel = loggingObject.getLogLevel();
        Map<String, Object> execPars = new HashMap<>();
        execPars.put("name", loggingObject.getObjectName());
        execPars.put("type", loggingObject.getObjectType().name());
        execPars.put("copy", loggingObject.getObjectCopy());
        execPars.put("id", loggingObject.getLogChannelId());
        execPars.put("containerId", loggingObject.getContainerId());
        execPars.put("logLevel", logLevel != null ? logLevel.getCode() : null);
        execPars.put("root", loggingObject.getLogChannelId().equals(rootLogChannelId));
        execPars.put(
            "registrationDate",
            new SimpleDateFormat("yyyy/MM/dd'T'HH:mm:ss")
                .format(loggingObject.getRegistrationDate()));

        StringBuilder execCypher = new StringBuilder();
        execCypher.append("MERGE (e:Execution { name : $name, type : $type, id : $id } ) ");
        execCypher.append("SET ");
        execCypher.append("  e.containerId = $containerId ");
        execCypher.append(", e.logLevel = $logLevel ");
        execCypher.append(", e.copy = $copy ");
        execCypher.append(", e.registrationDate = $registrationDate ");
        execCypher.append(", e.root = $root ");

        Result run = transaction.run(execCypher.toString(), execPars);
        run.consume();
      }

      // Now create the relationships between them
      //
      for (LoggingHierarchy hierarchy : hierarchies) {
        ILoggingObject loggingObject = hierarchy.getLoggingObject();
        ILoggingObject parentObject = loggingObject.getParent();
        if (parentObject != null) {
          Map<String, Object> execPars = new HashMap<>();
          execPars.put("name", loggingObject.getObjectName());
          execPars.put("type", loggingObject.getObjectType().name());
          execPars.put("id", loggingObject.getLogChannelId());
          execPars.put("parentName", parentObject.getObjectName());
          execPars.put("parentType", parentObject.getObjectType().name());
          execPars.put("parentId", parentObject.getLogChannelId());

          StringBuilder execCypher = new StringBuilder();
          execCypher.append("MATCH (child:Execution { name : $name, type : $type, id : $id } ) ");
          execCypher.append(
              "MATCH (parent:Execution { name : $parentName, type : $parentType, id : $parentId } ) ");
          execCypher.append("MERGE (parent)-[rel:EXECUTES]->(child) ");
          transaction.run(execCypher.toString(), execPars);
        }
      }
      transaction.commit();
    } catch (Exception e) {
      log.logError("Error logging hierarchies", e);
      transaction.rollback();
    }
  }

  public static <T> T executeCypher(
      ILogChannel log,
      IVariables variables,
      NeoConnection connection,
      String cypher,
      Map<String, Object> parameters,
      WorkLambda<T> lambda)
      throws Exception {

    Session session = null;
    try {
      session = connection.getSession(log, variables);

      return session.readTransaction(
          tx -> {
            Result result = tx.run(cypher, parameters);
            return lambda.getResultValue(result);
          });
    } finally {
      if (session != null) {
        session.close();
      }
    }
  }

  public static String getStringValue(Record record, int i) {
    if (i >= record.size()) {
      return null;
    }
    Value value = record.get(i);
    if (value == null || value.isNull()) {
      return null;
    }
    return value.asString();
  }

  public static Long getLongValue(Record record, int i) {
    if (i >= record.size()) {
      return null;
    }
    Value value = record.get(i);
    if (value == null || value.isNull()) {
      return null;
    }
    return value.asLong();
  }

  public static Date getDateValue(Record record, int i) {
    if (i >= record.size()) {
      return null;
    }
    Value value = record.get(i);
    if (value == null || value.isNull()) {
      return null;
    }
    LocalDateTime localDateTime = value.asLocalDateTime();
    if (localDateTime == null) {
      return null;
    }
    return Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant());
  }

  public static Boolean getBooleanValue(Record record, int i) {
    if (i >= record.size()) {
      return null;
    }
    Value value = record.get(i);
    if (value == null || value.isNull()) {
      return null;
    }
    return value.asBoolean();
  }

  public static String getStringValue(Node node, String name) {
    Value value = node.get(name);
    if (value == null || value.isNull()) {
      return null;
    }
    return value.asString();
  }

  public static Long getLongValue(Node node, String name) {
    Value value = node.get(name);
    if (value == null || value.isNull()) {
      return null;
    }
    return value.asLong();
  }

  public static Boolean getBooleanValue(Node node, String name) {
    Value value = node.get(name);
    if (value == null || value.isNull()) {
      return null;
    }
    return value.asBoolean();
  }

  public static Date getDateValue(Node node, String name) {
    Value value = node.get(name);
    if (value == null || value.isNull()) {
      return null;
    }
    LocalDateTime localDateTime = value.asLocalDateTime();
    if (localDateTime == null) {
      return null;
    }
    return Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant());
  }

  public static double calculateRadius(Rectangle bounds) {
    double radius =
        (double) (Math.min(bounds.width, bounds.height)) * 0.8 / 2; // 20% margin around circle
    return radius;
  }

  public static double calculateOptDistance(Rectangle bounds, int nrNodes) {

    if (nrNodes == 0) {
      return -1;
    }
    // Layout around a circle in essense.
    // So get a spot for every node on the circle.
    //

    // The radius is at most the smallest of width or height
    //
    double radius = calculateRadius(bounds);

    // Circumference
    //
    double circleLength = Math.PI * 2 * radius;

    // Optimal distance estimate is line segment on circle circumference
    // 25% margin between segments
    // Only put half of the nodes on the circle, the rest not.
    //
    double optDistance = 0.75 * circleLength / (nrNodes * 2);

    return optDistance;
  }

  /**
   * Extract the logging hierarchy for the given log channel ID
   *
   * @param logChannelId The root of the hierarchy to examine
   * @return The list of logging hierarchy objects
   */
  public static final List<LoggingHierarchy> getLoggingHierarchy(String logChannelId) {
    List<LoggingHierarchy> hierarchy = new ArrayList<>();
    List<String> childIds = LoggingRegistry.getInstance().getLogChannelChildren(logChannelId);
    for (String childId : childIds) {
      ILoggingObject loggingObject = LoggingRegistry.getInstance().getLoggingObject(childId);
      if (loggingObject != null) {
        hierarchy.add(new LoggingHierarchy(logChannelId, loggingObject));
      }
    }

    return hierarchy;
  }

  public static final String getFancyDurationFromMs(Long durationMs) {
    if (durationMs == null) {
      return "";
    }
    double seconds = ((double) durationMs) / 1000;
    return getFancyDurationFromSeconds(seconds);
  }

  public static final String getFancyDurationFromSeconds(double seconds) {
    int day = (int) TimeUnit.SECONDS.toDays((long) seconds);
    long hours = TimeUnit.SECONDS.toHours((long) seconds) - (day * 24);
    long minute =
        TimeUnit.SECONDS.toMinutes((long) seconds)
            - (TimeUnit.SECONDS.toHours((long) seconds) * 60);
    long second =
        TimeUnit.SECONDS.toSeconds((long) seconds)
            - (TimeUnit.SECONDS.toMinutes((long) seconds) * 60);
    long ms = (long) ((seconds - ((long) seconds)) * 1000);

    StringBuilder hms = new StringBuilder();
    if (day > 0) {
      hms.append(day + "d ");
    }
    if (day > 0 || hours > 0) {
      hms.append(hours + "h ");
    }
    if (day > 0 || hours > 0 || minute > 0) {
      hms.append(String.format("%02d", minute) + "' ");
    }
    hms.append(String.format("%02d", second) + ".");
    hms.append(String.format("%03d", ms) + "\"");

    return hms.toString();
  }
}
