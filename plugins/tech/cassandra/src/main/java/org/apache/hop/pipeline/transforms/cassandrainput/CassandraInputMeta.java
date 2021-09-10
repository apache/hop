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
package org.apache.hop.pipeline.transforms.cassandrainput;

import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.databases.cassandra.metadata.CassandraConnection;
import org.apache.hop.databases.cassandra.spi.Connection;
import org.apache.hop.databases.cassandra.spi.ITableMetaData;
import org.apache.hop.databases.cassandra.spi.Keyspace;
import org.apache.hop.databases.cassandra.util.CqlUtils;
import org.apache.hop.databases.cassandra.util.Selector;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.*;
import org.eclipse.swt.widgets.Shell;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;

/** Class providing an input transform for reading data from an Cassandra table */
@Transform(
    id = "CassandraInput",
    image = "Cassandrain.svg",
    name = "Cassandra input",
    description = "Reads data from a Cassandra table",
    documentationUrl =
        "https://hop.apache.org/manual/latest/pipeline/transforms/cassandra-input.html",
    categoryDescription = "Cassandra")
@InjectionSupported(localizationPrefix = "CassandraInput.Injection.")
public class CassandraInputMeta extends BaseTransformMeta<CassandraInput, CassandraInputData> {

  protected static final Class<?> PKG = CassandraInputMeta.class;

  /** The connection to use (metadata) */
  @Injection(name = "CONNECTION")
  protected String connectionName;

  /** The select query to execute */
  @Injection(name = "CQL_QUERY")
  protected String cqlSelectQuery = "SELECT <fields> FROM <table> WHERE <condition>;";

  /** Whether to execute the query for each incoming row */
  @Injection(name = "EXECUTE_FOR_EACH_ROW")
  protected boolean executeForEachIncomingRow;

  /** Max size of the object can be transported - blank means use default (16384000) */
  @Injection(name = "TRANSPORT_MAX_LENGTH")
  protected String maxLength = "";

  // set based on parsed CQL
  /**
   * True if a select * is being done - this is important to know because rows from select * queries
   * contain the key as the first column. Key is also available separately in the API (and we use
   * this for retrieving the key). The column that contains the key in this case is not necessarily
   * convertible using the default column validator because there is a separate key validator. So we
   * need to be able to recognize the key when it appears as a column and skip it. Can't rely on
   * it's name (KEY) since this is only easily detectable when the column names are strings.
   */
  protected boolean isSelectStarQuery = false;

  // these are set based on the parsed CQL when executing tuple mode using thrift
  protected int rowLimit = -1; // no limit - otherwise we look for LIMIT in
  // CQL
  protected int colLimit = -1; // no limit - otherwise we look for FIRST N in
  // CQL

  // maximum number of rows or columns to pull over at one time via thrift
  protected int rowBatchSize = 100;
  protected int colBatchSize = 100;
  protected List<String> specificCols;

  private boolean useDriver = true;

  @Override
  public String getXml() {
    StringBuffer xml = new StringBuffer();

    xml.append(XmlHandler.addTagValue("connection", connectionName));
    xml.append(XmlHandler.addTagValue("cql_select_query", cqlSelectQuery));
    xml.append(XmlHandler.addTagValue("max_length", maxLength));
    xml.append(XmlHandler.addTagValue("execute_for_each_row", executeForEachIncomingRow));

    return xml.toString();
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {

    connectionName = XmlHandler.getTagValue(transformNode, "connection");
    cqlSelectQuery = XmlHandler.getTagValue(transformNode, "cql_select_query");
    executeForEachIncomingRow =
        "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "execute_for_each_row"));
    maxLength = XmlHandler.getTagValue(transformNode, "max_length");
  }

  @Override
  public void setDefault() {
    cqlSelectQuery = "SELECT <fields> FROM <table> WHERE <condition>;";
    maxLength = "";
  }

  @Override
  public void getFields(
      IRowMeta rowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {

    specificCols = null;
    rowLimit = -1;
    colLimit = -1;

    rowMeta.clear(); // start afresh - eats the input

    if (Utils.isEmpty(connectionName)) {
      throw new HopTransformException("Please specify a Cassandra connection name to use");
    }

    String tableName = null;
    if (!Utils.isEmpty(cqlSelectQuery)) {
      String subQ = variables.resolve(cqlSelectQuery);

      if (!subQ.toLowerCase().startsWith("select")) {
        // not a select statement!
        throw new HopTransformException(
            BaseMessages.getString(PKG, "CassandraInput.Error.NoSelectInQuery"));
      }

      if (subQ.indexOf(';') < 0) {
        // query must end with a ';' or it will wait for more!
        throw new HopTransformException(
            BaseMessages.getString(PKG, "CassandraInput.Error.QueryTermination"));
      }

      // is there a LIMIT clause?
      if (subQ.toLowerCase().indexOf("limit") > 0) {
        String limitS =
            subQ.toLowerCase()
                .substring(subQ.toLowerCase().indexOf("limit") + 5, subQ.length())
                .trim();
        limitS = limitS.replaceAll(";", "");
        try {
          rowLimit = Integer.parseInt(limitS);
        } catch (NumberFormatException ex) {
          throw new HopTransformException(
              BaseMessages.getString(
                  PKG, "CassandraInput.Error.UnableToParseLimitClause", cqlSelectQuery));
        }
      }

      // strip off where clause (if any)
      if (subQ.toLowerCase().lastIndexOf("where") > 0) {
        subQ = subQ.substring(0, subQ.toLowerCase().lastIndexOf("where"));
      }

      // first determine the source table
      // look for a FROM that is surrounded by variables
      int fromIndex = subQ.toLowerCase().indexOf("from");
      String tempS = subQ.toLowerCase();
      int offset = fromIndex;
      while (fromIndex > 0
          && tempS.charAt(fromIndex - 1) != ' '
          && (fromIndex + 4 < tempS.length())
          && tempS.charAt(fromIndex + 4) != ' ') {
        tempS = tempS.substring(fromIndex + 4, tempS.length());
        fromIndex = tempS.indexOf("from");
        offset += (4 + fromIndex);
      }

      fromIndex = offset;
      if (fromIndex < 0) {
        throw new HopTransformException(
            BaseMessages.getString(PKG, "CassandraInput.Error.MustSpecifyATable"));
      }

      tableName = subQ.substring(fromIndex + 4, subQ.length()).trim();
      if (tableName.indexOf(' ') > 0) {
        tableName = tableName.substring(0, tableName.indexOf(' '));
      } else {
        tableName = tableName.replace(";", "");
      }

      if (tableName.length() == 0) {
        throw new HopTransformException(
            BaseMessages.getString(PKG, "CassandraInput.Error.MustSpecifyATable"));
      }

      // Remove leading/trailing spaces and newlines in the table name
      tableName = tableName.replaceFirst("^\\s+", "").replaceFirst("\\s+$", "");

      // is there a FIRST clause?
      if (subQ.toLowerCase().indexOf("first ") > 0) {
        String firstS =
            subQ.substring(subQ.toLowerCase().indexOf("first") + 5, subQ.length()).trim();

        // Strip FIRST part from query
        subQ = firstS.substring(firstS.indexOf(' ') + 1, firstS.length());

        firstS = firstS.substring(0, firstS.indexOf(' '));
        try {
          colLimit = Integer.parseInt(firstS);
        } catch (NumberFormatException ex) {
          throw new HopTransformException(
              BaseMessages.getString(
                  PKG, "CassandraInput.Error.UnableToParseFirstClause", cqlSelectQuery));
        }
      } else {
        subQ = subQ.substring(subQ.toLowerCase().indexOf("select") + 6, subQ.length());
      }

      // Reset FROM index
      fromIndex = subQ.toLowerCase().indexOf("from");

      // now determine if its a select */FIRST or specific set of columns
      Selector[] cols = null;
      if (subQ.contains("*") && !subQ.toLowerCase().contains("count(*)")) {
        // nothing special to do here
        isSelectStarQuery = true;
      } else {
        isSelectStarQuery = false;
        // String colsS = subQ.substring(subQ.indexOf('\''), fromIndex);
        String colsS = subQ.substring(0, fromIndex);
        // Parse select expression to get selectors: columns and functions
        cols = CqlUtils.getColumnsInSelect(colsS, true);
      }

      // try and connect to get meta data
      //
      Connection conn = null;
      Keyspace kSpace;

      try {
        CassandraConnection cassandraConnection =
            metadataProvider
                .getSerializer(CassandraConnection.class)
                .load(variables.resolve(connectionName));
        conn = cassandraConnection.createConnection(variables, false);
        kSpace = cassandraConnection.lookupKeyspace(conn, variables);
      } catch (Exception e) {
        throw new HopTransformException(
            "Unable to connect to Cassandra with '" + connectionName + "' or look up the keyspace",
            e);
      }

      try {
        /*
         * CassandraColumnMetaData colMeta = new CassandraColumnMetaData(conn, tableName);
         */
        ITableMetaData colMeta = kSpace.getTableMetaData(tableName);

        if (cols == null) {
          // select * - use all the columns that are defined in the schema
          List<IValueMeta> vms = colMeta.getValueMetasForSchema();
          for (IValueMeta vm : vms) {
            rowMeta.addValueMeta(vm);
          }
        } else {
          specificCols = new ArrayList<>();
          for (Selector col : cols) {
            if (!col.isFunction() && !colMeta.columnExistsInSchema(col.getColumnName())) {
              // this one isn't known about in about in the schema - we can
              // output it
              // as long as its values satisfy the default validator...
              logBasic(
                  BaseMessages.getString(PKG, "CassandraInput.Info.DefaultColumnValidator", col));
            }
            IValueMeta vm = colMeta.getValueMeta(col);
            rowMeta.addValueMeta(vm);
          }
        }
      } catch (Exception ex) {
        logBasic(
            BaseMessages.getString(
                PKG, "CassandraInput.Info.UnableToRetrieveColumnMetaData", tableName),
            ex);
      } finally {
        try {
          conn.closeConnection();
        } catch (Exception e) {
          throw new HopTransformException(e);
        }
      }
    }
  }

  public boolean isUseDriver() {
    return useDriver;
  }

  /**
   * Get the UI for this transform.
   *
   * @param shell a <code>Shell</code> value
   * @param meta a <code>ITransformMeta</code> value
   * @param pipelineMeta a <code>PipelineMeta</code> value
   * @param name a <code>String</code> value
   * @return a <code>ITransformDialog</code> value
   */
  public ITransformDialog getDialog(
      Shell shell,
      IVariables variables,
      ITransformMeta meta,
      PipelineMeta pipelineMeta,
      String name) {

    return new CassandraInputDialog(shell, variables, meta, pipelineMeta, name);
  }

  /**
   * Gets cqlSelectQuery
   *
   * @return value of cqlSelectQuery
   */
  public String getCqlSelectQuery() {
    return cqlSelectQuery;
  }

  /** @param cqlSelectQuery The cqlSelectQuery to set */
  public void setCqlSelectQuery(String cqlSelectQuery) {
    this.cqlSelectQuery = cqlSelectQuery;
  }

  /**
   * Gets executeForEachIncomingRow
   *
   * @return value of executeForEachIncomingRow
   */
  public boolean isExecuteForEachIncomingRow() {
    return executeForEachIncomingRow;
  }

  /** @param executeForEachIncomingRow The executeForEachIncomingRow to set */
  public void setExecuteForEachIncomingRow(boolean executeForEachIncomingRow) {
    this.executeForEachIncomingRow = executeForEachIncomingRow;
  }

  /**
   * Gets maxLength
   *
   * @return value of maxLength
   */
  public String getMaxLength() {
    return maxLength;
  }

  /** @param maxLength The maxLength to set */
  public void setMaxLength(String maxLength) {
    this.maxLength = maxLength;
  }

  /**
   * Gets isSelectStarQuery
   *
   * @return value of isSelectStarQuery
   */
  public boolean isSelectStarQuery() {
    return isSelectStarQuery;
  }

  /** @param selectStarQuery The isSelectStarQuery to set */
  public void setSelectStarQuery(boolean selectStarQuery) {
    isSelectStarQuery = selectStarQuery;
  }

  /**
   * Gets rowLimit
   *
   * @return value of rowLimit
   */
  public int getRowLimit() {
    return rowLimit;
  }

  /** @param rowLimit The rowLimit to set */
  public void setRowLimit(int rowLimit) {
    this.rowLimit = rowLimit;
  }

  /**
   * Gets colLimit
   *
   * @return value of colLimit
   */
  public int getColLimit() {
    return colLimit;
  }

  /** @param colLimit The colLimit to set */
  public void setColLimit(int colLimit) {
    this.colLimit = colLimit;
  }

  /**
   * Gets rowBatchSize
   *
   * @return value of rowBatchSize
   */
  public int getRowBatchSize() {
    return rowBatchSize;
  }

  /** @param rowBatchSize The rowBatchSize to set */
  public void setRowBatchSize(int rowBatchSize) {
    this.rowBatchSize = rowBatchSize;
  }

  /**
   * Gets colBatchSize
   *
   * @return value of colBatchSize
   */
  public int getColBatchSize() {
    return colBatchSize;
  }

  /** @param colBatchSize The colBatchSize to set */
  public void setColBatchSize(int colBatchSize) {
    this.colBatchSize = colBatchSize;
  }

  /**
   * Gets specificCols
   *
   * @return value of specificCols
   */
  public List<String> getSpecificCols() {
    return specificCols;
  }

  /** @param specificCols The specificCols to set */
  public void setSpecificCols(List<String> specificCols) {
    this.specificCols = specificCols;
  }

  /** @param useDriver The useDriver to set */
  public void setUseDriver(boolean useDriver) {
    this.useDriver = useDriver;
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
}
