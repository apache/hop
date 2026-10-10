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

package org.apache.hop.neo4j.logging.util;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.graph.GraphNodeValue;
import org.apache.hop.core.graph.IGraphConnection;
import org.apache.hop.core.graph.IGraphTransaction;
import org.apache.hop.core.graph.IGraphTransactionWork;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.logging.LoggingHierarchy;
import org.apache.hop.core.logging.LoggingRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.neo4j.logging.Defaults;
import org.apache.hop.neo4j.shared.NamedGraphConnection;
import org.apache.hop.neo4j.shared.NeoConnectionUtils;
import org.eclipse.swt.graphics.Rectangle;

public class LoggingCore {

  /** The format Neo4j logging stores the registration date of an execution in. */
  public static final String REGISTRATION_DATE_FORMAT = "yyyy/MM/dd'T'HH:mm:ss";

  /**
   * The name of the connection to log to: {@link Defaults#HOP_GRAPH_LOGGING_CONNECTION} or else
   * {@link Defaults#NEO4J_LOGGING_CONNECTION}. Null if logging is disabled.
   */
  public static String getConnectionName(IVariables variables) {
    String connectionName = variables.getVariable(Defaults.HOP_GRAPH_LOGGING_CONNECTION);
    if (StringUtils.isEmpty(connectionName)) {
      connectionName = variables.getVariable(Defaults.NEO4J_LOGGING_CONNECTION);
    }
    if (StringUtils.isEmpty(connectionName)
        || Defaults.VARIABLE_NEO4J_LOGGING_CONNECTION_DISABLED.equals(connectionName)) {
      return null;
    }
    return connectionName;
  }

  public static final boolean isEnabled(IVariables space) {
    return getConnectionName(space) != null;
  }

  /**
   * The connection to log to: a Neo4j connection or a graph database connection of a Cypher
   * database.
   *
   * @return The connection or null if logging is disabled
   * @throws HopException if the connection doesn't exist or its database doesn't speak Cypher
   */
  public static final NamedGraphConnection getConnection(
      IHopMetadataProvider metadataProvider, IVariables space) throws HopException {
    String connectionName = getConnectionName(space);
    if (connectionName == null) {
      return null;
    }
    NamedGraphConnection connection =
        NeoConnectionUtils.findGraphConnection(metadataProvider, connectionName);
    if (connection == null) {
      throw new HopException("Unable to find graph database connection '" + connectionName + "'");
    }
    if (!connection.getDialect().isCypher()) {
      throw new HopException(
          "Execution logging needs a Cypher graph database, connection '"
              + connectionName
              + "' isn't one");
    }
    return connection;
  }

  /**
   * The connection to log to, like {@link #getConnection} but never failing: a connection which
   * doesn't exist or doesn't speak Cypher gets a warning, as the execution logging always did.
   *
   * @return The connection or null if logging is disabled or not possible
   */
  public static NamedGraphConnection findConnection(
      ILogChannel log, IHopMetadataProvider metadataProvider, IVariables variables) {
    try {
      return getConnection(metadataProvider, variables);
    } catch (HopException e) {
      log.logBasic("Warning! No execution logging: " + e.getMessage());
      return null;
    }
  }

  /** Run work in a write transaction. Errors are logged: logging never fails an execution. */
  public static void write(
      ILogChannel log, IGraphConnection connection, IGraphTransactionWork<Object> work) {
    synchronized (connection) {
      try {
        connection.executeWrite(work);
      } catch (Exception e) {
        log.logError("Error writing execution information to the graph database", e);
      }
    }
  }

  /** Close the connection, logging errors. */
  public static void close(ILogChannel log, IGraphConnection connection) {
    try {
      connection.close();
    } catch (Exception e) {
      log.logError("Error closing the graph database logging connection", e);
    }
  }

  /** Run a read statement on the logging connection, the result rows as maps. */
  public static List<Map<String, Object>> query(
      ILogChannel log,
      IVariables variables,
      NamedGraphConnection connection,
      String cypher,
      Map<String, Object> parameters)
      throws HopException {
    try (IGraphConnection graphConnection = connection.connect(log, variables)) {
      return graphConnection.executeRead(transaction -> transaction.execute(cypher, parameters));
    }
  }

  public static final void writeHierarchies(
      ILogChannel log,
      IGraphTransaction transaction,
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
            new SimpleDateFormat(REGISTRATION_DATE_FORMAT)
                .format(loggingObject.getRegistrationDate()));

        StringBuilder execCypher = new StringBuilder();
        execCypher.append("MERGE (e:Execution { name : $name, type : $type, id : $id } ) ");
        execCypher.append("SET ");
        execCypher.append("  e.containerId = $containerId ");
        execCypher.append(", e.logLevel = $logLevel ");
        execCypher.append(", e.copy = $copy ");
        execCypher.append(", e.registrationDate = $registrationDate ");
        execCypher.append(", e.root = $root ");

        transaction.execute(execCypher.toString(), execPars);
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
          transaction.execute(execCypher.toString(), execPars);
        }
      }
      // Transaction is automatically committed by executeWrite
    } catch (Exception e) {
      log.logError("Error logging hierarchies", e);
    }
  }

  public static String getStringValue(Map<String, Object> row, String name) {
    Object value = row.get(name);
    return value == null ? null : value.toString();
  }

  public static Long getLongValue(Map<String, Object> row, String name) {
    Object value = row.get(name);
    if (value == null) {
      return null;
    }
    return value instanceof Number number ? number.longValue() : Long.valueOf(value.toString());
  }

  public static Integer getIntegerValue(Map<String, Object> row, String name) {
    Long value = getLongValue(row, name);
    return value == null ? null : value.intValue();
  }

  @Nullable
  public static Boolean getBooleanValue(Map<String, Object> row, String name) {
    Object value = row.get(name);
    if (value == null) {
      return null;
    }
    return value instanceof Boolean bool ? bool : Boolean.valueOf(value.toString());
  }

  /** The properties of a node in a result row, empty if it isn't a node. */
  public static Map<String, Object> getNodeProperties(Map<String, Object> row, String name) {
    Object value = row.get(name);
    return value instanceof GraphNodeValue node ? node.properties() : Map.of();
  }

  /**
   * A date property of an Execution node. The execution information location stores dates as a date
   * and time: a local date time on Neo4j and Memgraph, an ISO string on FalkorDB and Apache AGE.
   * Neo4j logging stores the registration date as a string in {@link #REGISTRATION_DATE_FORMAT}.
   * Both write Execution nodes, so either can show up here (issue #8704).
   *
   * @return the date, or null if the property is missing or isn't a date
   */
  public static Date getDateValue(Map<String, Object> properties, String name) {
    Object value = properties.get(name);
    if (value == null) {
      return null;
    }
    if (value instanceof Date date) {
      return date;
    }
    if (value instanceof LocalDateTime localDateTime) {
      return toDate(localDateTime);
    }
    if (value instanceof ZonedDateTime zonedDateTime) {
      return Date.from(zonedDateTime.toInstant());
    }
    if (value instanceof OffsetDateTime offsetDateTime) {
      return Date.from(offsetDateTime.toInstant());
    }
    if (value instanceof LocalDate localDate) {
      return toDate(localDate.atStartOfDay());
    }
    if (value instanceof String string) {
      return parseDate(string);
    }
    return null;
  }

  private static Date parseDate(String string) {
    for (DateTimeFormatter formatter :
        List.of(
            DateTimeFormatter.ofPattern(REGISTRATION_DATE_FORMAT),
            DateTimeFormatter.ISO_LOCAL_DATE_TIME)) {
      try {
        return toDate(LocalDateTime.parse(string, formatter));
      } catch (DateTimeParseException e) {
        // Try the next format
      }
    }
    return null;
  }

  private static Date toDate(LocalDateTime localDateTime) {
    return Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant());
  }

  public static double calculateRadius(Rectangle bounds) {
    // 20% margin around circle
    return (double) (Math.min(bounds.width, bounds.height)) * 0.8 / 2;
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
    return 0.75 * circleLength / (nrNodes * 2);
  }

  /**
   * Extract the logging hierarchy for the given log channel ID
   *
   * @param logChannelId The root of the hierarchy to examine
   * @return The list of logging hierarchy objects
   */
  @SuppressWarnings("javabugs:S2259") // the log channel id is never null here
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
