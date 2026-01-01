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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataType;
import java.util.Iterator;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.databases.cassandra.datastax.DriverConnection;
import org.apache.hop.databases.cassandra.datastax.DriverCqlRowHandler;
import org.apache.hop.databases.cassandra.datastax.TableMetaData;
import org.apache.hop.databases.cassandra.metadata.CassandraConnection;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;

@Action(
    id = "CASSANDRA_EXEC_CQL",
    name = "Cassandra Execute CQL",
    description = "Execute CQL statements against a Cassandra cluster",
    image = "Cassandra_logo.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Scripting",
    keywords = "i18n::ExecCql.keyword",
    documentationUrl = "/workflow/actions/cassandra-exec-cql.html")
@Getter
@Setter
public class ExecCql extends ActionBase implements IAction {

  @HopMetadataProperty(key = "connection")
  private String connectionName;

  @HopMetadataProperty(key = "script")
  private String script;

  @HopMetadataProperty(key = "replace_variables")
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

    int nrExecuted =
        executeCqlStatements(this, getLogChannel(), result, cassandraConnection, cqlStatements);

    if (result.getNrErrors() == 0) {
      if (isBasic()) {
        logBasic("Cassandra executed " + nrExecuted + " CQL commands without error");
      }
    } else {
      if (isBasic()) {
        logBasic("Cassandra Exec CQL: some command(s) executed with error(s)");
      }
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
    try (DriverConnection connection = cassandraConnection.createConnection(variables, true)) {
      try (CqlSession session = connection.open()) {
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
              ResultSet resultSet = session.execute(cql);

              // Consume the result set rows
              //
              Iterator<Row> iterator = resultSet.iterator();
              IRowMeta resultRowMeta = null;
              while (iterator.hasNext()) {
                Row row = iterator.next();
                if (resultRowMeta == null) {
                  resultRowMeta = getRowMeta(row.getColumnDefinitions());
                }
                Object[] resultRowData = DriverCqlRowHandler.readRow(resultRowMeta, row);
                result.getRows().add(new RowMetaAndData(resultRowMeta, resultRowData));
              }

              // Wait until the CQL is completely executed.
              //
              while (!resultSet.wasApplied()) {
                Thread.sleep(50);
              }
              nrExecuted++;
              if (log.isDetailed()) {
                log.logDetailed("Executed cql statement: " + cql);
              }
            }
          }
        } catch (Exception e) {
          log.logError("Error executing CQL statements...", e);
          result.increaseErrors(1L);
          result.setResult(false);
        }
      }
    } catch (Exception e) {
      throw new HopException(
          "Error executing CQL on Cassandra connection " + cassandraConnection.getName(), e);
    }
    return nrExecuted;
  }

  public static IRowMeta getRowMeta(ColumnDefinitions columnDefinitions) {
    IRowMeta rowMeta = new RowMeta();
    for (int i = 0; i < columnDefinitions.size(); i++) {
      ColumnDefinition columnDefinition = columnDefinitions.get(i);
      DataType dataType = columnDefinition.getType();
      String name = columnDefinition.getName().asCql(false);
      if (name.startsWith("\"")) {
        name = name.substring(1);
      }
      if (name.endsWith("\"")) {
        name = name.substring(0, name.length() - 1);
      }
      IValueMeta valueMeta = TableMetaData.toValueMeta(name, dataType);
      rowMeta.addValueMeta(valueMeta);
    }
    return rowMeta;
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
}
