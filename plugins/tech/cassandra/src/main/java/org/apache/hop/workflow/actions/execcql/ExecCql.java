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

package org.apache.hop.workflow.actions.execcql;

import com.datastax.driver.core.Session;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.databases.cassandra.metadata.CassandraConnection;
import org.apache.hop.databases.cassandra.spi.Connection;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.w3c.dom.Node;

@Action(
    id = "CASSANDRA_EXEC_CQL",
    name = "Cassandra Execute CQL",
    description = "Execute CQL statements against a Cassandra cluster",
    image = "Cassandra_logo.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Scripting",
    keywords = "i18n::ExecCql.keyword",
    documentationUrl = "/workflow/actions/cassandra-exec-cql.html")
public class ExecCql extends ActionBase implements IAction {

  private String connectionName;

  private String script;

  private boolean replacingVariables;

  public ExecCql() {
    this("", "");
  }

  public ExecCql(String name) {
    this(name, "");
  }

  public ExecCql(String name, String description) {
    super(name, description);
  }

  @Override
  public String getXml() {
    StringBuilder xml = new StringBuilder();
    // Add action name, type, ...
    //
    xml.append(super.getXml());

    xml.append(XmlHandler.addTagValue("connection", connectionName));
    xml.append(XmlHandler.addTagValue("script", script));
    xml.append(XmlHandler.addTagValue("replace_variables", replacingVariables ? "Y" : "N"));

    return xml.toString();
  }

  @Override
  public void loadXml(Node node, IHopMetadataProvider iHopMetadataProvider, IVariables iVariables)
      throws HopXmlException {
    super.loadXml(node);

    connectionName = XmlHandler.getTagValue(node, "connection");
    script = XmlHandler.getTagValue(node, "script");
    replacingVariables = "Y".equalsIgnoreCase(XmlHandler.getTagValue(node, "replace_variables"));
  }

  @Override
  public Result execute(Result result, int nr) throws HopException {

    IHopMetadataSerializer<CassandraConnection> serializer =
        getMetadataProvider().getSerializer(CassandraConnection.class);

    // Replace variables & parameters
    //
    CassandraConnection cassandraConnection;
    String realConnectionName = resolve(connectionName);
    try {
      if (StringUtils.isEmpty(realConnectionName)) {
        throw new HopException("A Cassandra cassandraConnection name is not defined");
      }

      cassandraConnection = serializer.load(realConnectionName);
      if (cassandraConnection == null) {
        throw new HopException(
            "Unable to find Cassandra cassandraConnection with name '" + realConnectionName + "'");
      }
    } catch (Exception e) {
      result.setResult(false);
      result.increaseErrors(1L);
      throw new HopException(
          "Unable to load or find a Cassandra cassandraConnection with name '"
              + realConnectionName
              + "'",
          e);
    }

    String cqlStatements;
    if (replacingVariables) {
      cqlStatements = resolve(script);
    } else {
      cqlStatements = script;
    }

    int nrExecuted = executeCqlStatements(this, log, result, cassandraConnection, cqlStatements);

    if (result.getNrErrors() == 0) {
      logBasic("Cassandra executed " + nrExecuted + " CQL commands without error");
    } else {
      logBasic("Cassandra Exec CQL: some command(s) executed with error(s)");
    }

    return result;
  }

  public static int executeCqlStatements(
      IVariables variables,
      ILogChannel log,
      Result result,
      CassandraConnection cassandraConnection,
      String cqlStatements)
      throws HopException {
    int nrExecuted = 0;

    // Connect to the database
    //
    Connection connection = null;
    try {
      connection = cassandraConnection.createConnection(variables, true);
      Session session = connection.openConnection();

      try {
        // Split the script into parts : semi-colon at the start of a separate line
        //
        String[] commands = cqlStatements.split("\\r?\\n;");
        for (String command : commands) {
          // Cleanup command: replace leading and trailing whitespaces and newlines
          //
          String cql = command.replaceFirst("^\\s+", "").replaceFirst("\\s+$", "");

          // Only execute if the statement is not empty
          //
          if (StringUtils.isNotEmpty(cql)) {
            session.execute(cql);
            nrExecuted++;
            log.logDetailed("Executed cql statement: " + cql);
          }
        }
      } catch (Exception e) {
        log.logError("Error executing CQL statements...", e);
        result.increaseErrors(1L);
        result.setResult(false);
      }

    } catch (Exception e) {
      throw new HopException(
          "Error executing CQL on Cassandra connection " + cassandraConnection.getName(), e);
    } finally {
      if (connection != null) {
        try {
          connection.closeConnection();
        } catch (Exception e) {
          throw new HopException(
              "Error closing Cassandra connection " + cassandraConnection.getName(), e);
        }
      }
    }
    return nrExecuted;
  }

  @Override
  public String getDialogClassName() {
    return super.getDialogClassName();
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }

  @Override
  public boolean isUnconditional() {
    return false;
  }

  /**
   * Gets connectionName
   *
   * @return value of connectionName
   */
  public String getConnectionName() {
    return connectionName;
  }

  /** @param connectionName The connectionName to set */
  public void setConnectionName(String connectionName) {
    this.connectionName = connectionName;
  }

  /**
   * Gets script
   *
   * @return value of script
   */
  public String getScript() {
    return script;
  }

  /** @param script The script to set */
  public void setScript(String script) {
    this.script = script;
  }

  /**
   * Gets replacingVariables
   *
   * @return value of replacingVariables
   */
  public boolean isReplacingVariables() {
    return replacingVariables;
  }

  /** @param replacingVariables The replacingVariables to set */
  public void setReplacingVariables(boolean replacingVariables) {
    this.replacingVariables = replacingVariables;
  }
}
