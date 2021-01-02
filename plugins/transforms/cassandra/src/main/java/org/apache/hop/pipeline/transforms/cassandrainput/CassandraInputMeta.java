/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/
package org.apache.hop.pipeline.transforms.cassandrainput;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.databases.cassandra.ConnectionFactory;
import org.apache.hop.databases.cassandra.spi.Connection;
import org.apache.hop.databases.cassandra.spi.ITableMetaData;
import org.apache.hop.databases.cassandra.spi.Keyspace;
import org.apache.hop.databases.cassandra.util.CQLUtils;
import org.apache.hop.databases.cassandra.util.CassandraUtils;
import org.apache.hop.databases.cassandra.util.Selector;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.eclipse.swt.widgets.Shell;
import org.w3c.dom.Node;

/** Class providing an input step for reading data from an Cassandra table */
@Transform(
    id = "CassandraInput",
    image = "Cassandrain.svg",
    name = "Cassandra input",
    description = "Reads data from a Cassandra table",
    documentationUrl = "Products/Cassandra_Input",
    categoryDescription = "Big Data")
@InjectionSupported(localizationPrefix = "CassandraInput.Injection.")
public class CassandraInputMeta extends BaseTransformMeta
    implements ITransformMeta<CassandraInput, CassandraInputData> {

  protected static final Class<?> PKG = CassandraInputMeta.class;

  /** The host to contact */
  @Injection(name = "CASSANDRA_HOST")
  protected String m_cassandraHost = "localhost"; // $NON-NLS-1$

  /** The port that cassandra is listening on */
  @Injection(name = "CASSANDRA_PORT")
  protected String m_cassandraPort = "9042"; // $NON-NLS-1$

  /** Username for authentication */
  @Injection(name = "USER_NAME")
  protected String m_username;

  /** Password for authentication */
  @Injection(name = "PASSWORD")
  protected String m_password;

  /** The keyspace (database) to use */
  @Injection(name = "CASSANDRA_KEYSPACE")
  protected String m_cassandraKeyspace;

  /** Whether to use GZIP compression of CQL queries */
  @Injection(name = "USE_QUERY_COMPRESSION")
  protected boolean m_useCompression;

  /** The select query to execute */
  @Injection(name = "CQL_QUERY")
  protected String m_cqlSelectQuery =
      "SELECT <fields> FROM <table> WHERE <condition>;"; //$NON-NLS-1$

  /** Whether to execute the query for each incoming row */
  @Injection(name = "EXECUTE_FOR_EACH_ROW")
  protected boolean m_executeForEachIncomingRow;

  /** Timeout (milliseconds) to use for socket connections - blank means use cluster default */
  @Injection(name = "SOCKET_TIMEOUT")
  protected String m_socketTimeout = ""; // $NON-NLS-1$

  /** Max size of the object can be transported - blank means use default (16384000) */
  @Injection(name = "TRANSPORT_MAX_LENGTH")
  protected String m_maxLength = ""; // $NON-NLS-1$

  // set based on parsed CQL
  /**
   * True if a select * is being done - this is important to know because rows from select * queries
   * contain the key as the first column. Key is also available separately in the API (and we use
   * this for retrieving the key). The column that contains the key in this case is not necessarily
   * convertible using the default column validator because there is a separate key validator. So we
   * need to be able to recognize the key when it appears as a column and skip it. Can't rely on
   * it's name (KEY) since this is only easily detectable when the column names are strings.
   */
  protected boolean m_isSelectStarQuery = false;

  // these are set based on the parsed CQL when executing tuple mode using thrift
  protected int m_rowLimit = -1; // no limit - otherwise we look for LIMIT in
  // CQL
  protected int m_colLimit = -1; // no limit - otherwise we look for FIRST N in
  // CQL

  // maximum number of rows or columns to pull over at one time via thrift
  protected int m_rowBatchSize = 100;
  protected int m_colBatchSize = 100;
  protected List<String> m_specificCols;

  private boolean useDriver = true;

  /**
   * Get the max length (bytes) to use for transport
   *
   * @return the max transport length to use in bytes
   */
  public String getMaxLength() {
    return m_maxLength;
  }

  /**
   * Set the max length (bytes) to use for transport
   *
   * @param maxLength the max transport length to use in bytes
   */
  public void setMaxLength(String maxLength) {
    this.m_maxLength = maxLength;
  }

  /**
   * Set the timeout (milliseconds) to use for socket comms
   *
   * @param t the timeout to use in milliseconds
   */
  public void setSocketTimeout(String t) {
    m_socketTimeout = t;
  }

  /**
   * Get the timeout (milliseconds) to use for socket comms
   *
   * @return the timeout to use in milliseconds
   */
  public String getSocketTimeout() {
    return m_socketTimeout;
  }

  /**
   * Set the cassandra node hostname to connect to
   *
   * @param host the host to connect to
   */
  public void setCassandraHost(String host) {
    m_cassandraHost = host;
  }

  /**
   * Get the name of the cassandra node to connect to
   *
   * @return the name of the cassandra node to connect to
   */
  public String getCassandraHost() {
    return m_cassandraHost;
  }

  /**
   * Set the port that cassandra is listening on
   *
   * @param port the port that cassandra is listening on
   */
  public void setCassandraPort(String port) {
    m_cassandraPort = port;
  }

  /**
   * Get the port that cassandra is listening on
   *
   * @return the port that cassandra is listening on
   */
  public String getCassandraPort() {
    return m_cassandraPort;
  }

  /**
   * Set the keyspace (db) to use
   *
   * @param keyspace the keyspace to use
   */
  public void setCassandraKeyspace(String keyspace) {
    m_cassandraKeyspace = keyspace;
  }

  /**
   * Get the keyspace (db) to use
   *
   * @return the keyspace (db) to use
   */
  public String getCassandraKeyspace() {
    return m_cassandraKeyspace;
  }

  /**
   * Set whether to compress (GZIP) CQL queries when transmitting them to the server
   *
   * @param c true if CQL queries are to be compressed
   */
  public void setUseCompression(boolean c) {
    m_useCompression = c;
  }

  /**
   * Get whether CQL queries will be compressed (GZIP) or not
   *
   * @return true if CQL queries will be compressed when sending to the server
   */
  public boolean getUseCompression() {
    return m_useCompression;
  }

  /**
   * Set the CQL SELECT query to execute.
   *
   * @param query the query to execute
   */
  public void setCQLSelectQuery(String query) {
    m_cqlSelectQuery = query;
  }

  /**
   * Get the CQL SELECT query to execute
   *
   * @return the query to execute
   */
  public String getCQLSelectQuery() {
    return m_cqlSelectQuery;
  }

  /**
   * Set the username to authenticate with
   *
   * @param un the username to authenticate with
   */
  public void setUsername(String un) {
    m_username = un;
  }

  /**
   * Get the username to authenticate with
   *
   * @return the username to authenticate with
   */
  public String getUsername() {
    return m_username;
  }

  /**
   * Set the password to authenticate with
   *
   * @param pass the password to authenticate with
   */
  public void setPassword(String pass) {
    m_password = pass;
  }

  /**
   * Get the password to authenticate with
   *
   * @return the password to authenticate with
   */
  public String getPassword() {
    return m_password;
  }

  /**
   * Set whether the query should be executed for each incoming row
   *
   * @param e true if the query should be executed for each incoming row
   */
  public void setExecuteForEachIncomingRow(boolean e) {
    m_executeForEachIncomingRow = e;
  }

  /**
   * Get whether the query should be executed for each incoming row
   *
   * @return true if the query should be executed for each incoming row
   */
  public boolean getExecuteForEachIncomingRow() {
    return m_executeForEachIncomingRow;
  }

  @Override
  public String getXml() {
    StringBuffer retval = new StringBuffer();

    if (!Utils.isEmpty(m_cassandraHost)) {
      retval
          .append("\n    ")
          .append(
              XmlHandler.addTagValue(
                  "cassandra_host", m_cassandraHost)); // $NON-NLS-1$ //$NON-NLS-2$
    }

    if (!Utils.isEmpty(m_cassandraPort)) {
      retval
          .append("\n    ")
          .append(
              XmlHandler.addTagValue(
                  "cassandra_port", m_cassandraPort)); // $NON-NLS-1$ //$NON-NLS-2$
    }

    if (!Utils.isEmpty(m_username)) {
      retval
          .append("\n    ")
          .append(XmlHandler.addTagValue("username", m_username)); // $NON-NLS-1$ //$NON-NLS-2$
    }

    if (!Utils.isEmpty(m_password)) {
      retval
          .append("\n    ")
          .append( //$NON-NLS-1$
              XmlHandler.addTagValue(
                  "password", Encr.encryptPasswordIfNotUsingVariables(m_password))); // $NON-NLS-1$
    }

    if (!Utils.isEmpty(m_cassandraKeyspace)) {
      retval
          .append("\n    ")
          .append(
              XmlHandler.addTagValue(
                  "cassandra_keyspace", m_cassandraKeyspace)); // $NON-NLS-1$ //$NON-NLS-2$
    }

    retval
        .append("\n    ")
        .append(
            XmlHandler.addTagValue(
                "use_compression", m_useCompression)); // $NON-NLS-1$ //$NON-NLS-2$

    if (!Utils.isEmpty(m_cqlSelectQuery)) {
      retval
          .append("\n    ")
          .append(
              XmlHandler.addTagValue(
                  "cql_select_query", m_cqlSelectQuery)); // $NON-NLS-1$ //$NON-NLS-2$
    }

    if (!Utils.isEmpty(m_socketTimeout)) {
      retval
          .append("\n    ")
          .append(
              XmlHandler.addTagValue(
                  "socket_timeout", m_socketTimeout)); // $NON-NLS-1$ //$NON-NLS-2$
    }

    if (!Utils.isEmpty(m_maxLength)) {
      retval
          .append("\n    ")
          .append(XmlHandler.addTagValue("max_length", m_maxLength)); // $NON-NLS-1$ //$NON-NLS-2$
    }

    retval
        .append("    ")
        .append( //$NON-NLS-1$
            XmlHandler.addTagValue(
                "execute_for_each_row", m_executeForEachIncomingRow)); // $NON-NLS-1$

    return retval.toString();
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {

    m_cassandraHost = XmlHandler.getTagValue(transformNode, "cassandra_host"); // $NON-NLS-1$
    m_cassandraPort = XmlHandler.getTagValue(transformNode, "cassandra_port"); // $NON-NLS-1$
    m_username = XmlHandler.getTagValue(transformNode, "username"); // $NON-NLS-1$
    m_password = XmlHandler.getTagValue(transformNode, "password"); // $NON-NLS-1$
    if (!Utils.isEmpty(m_password)) {
      m_password = Encr.decryptPasswordOptionallyEncrypted(m_password);
    }
    m_cassandraKeyspace =
        XmlHandler.getTagValue(transformNode, "cassandra_keyspace"); // $NON-NLS-1$
    m_cqlSelectQuery = XmlHandler.getTagValue(transformNode, "cql_select_query"); // $NON-NLS-1$
    m_useCompression =
        XmlHandler.getTagValue(transformNode, "use_compression")
            .equalsIgnoreCase("Y"); // $NON-NLS-1$ //$NON-NLS-2$

    String executeForEachR =
        XmlHandler.getTagValue(transformNode, "execute_for_each_row"); // $NON-NLS-1$
    if (!Utils.isEmpty(executeForEachR)) {
      m_executeForEachIncomingRow = executeForEachR.equalsIgnoreCase("Y"); // $NON-NLS-1$
    }

    m_socketTimeout = XmlHandler.getTagValue(transformNode, "socket_timeout"); // $NON-NLS-1$
    m_maxLength = XmlHandler.getTagValue(transformNode, "max_length"); // $NON-NLS-1$
  }

  @Override
  public void setDefault() {
    m_cassandraHost = "localhost"; // $NON-NLS-1$
    m_cassandraPort = "9042"; // $NON-NLS-1$
    m_cqlSelectQuery = "SELECT <fields> FROM <table> WHERE <condition>;"; // $NON-NLS-1$
    m_useCompression = false;
    m_socketTimeout = ""; // $NON-NLS-1$
    m_maxLength = ""; // $NON-NLS-1$
  }

  @Override
  public void getFields(
      IRowMeta rowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables space,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {

    m_specificCols = null;
    m_rowLimit = -1;
    m_colLimit = -1;

    rowMeta.clear(); // start afresh - eats the input

    if (Utils.isEmpty(m_cassandraKeyspace)) {
      // no keyspace!
      return;
    }

    String tableName = null;
    if (!Utils.isEmpty(m_cqlSelectQuery)) {
      String subQ = space.environmentSubstitute(m_cqlSelectQuery);

      if (!subQ.toLowerCase().startsWith("select")) { // $NON-NLS-1$
        // not a select statement!
        logError(
            BaseMessages.getString(PKG, "CassandraInput.Error.NoSelectInQuery")); // $NON-NLS-1$
        return;
      }

      if (subQ.indexOf(';') < 0) {
        // query must end with a ';' or it will wait for more!
        logError(
            BaseMessages.getString(PKG, "CassandraInput.Error.QueryTermination")); // $NON-NLS-1$
        return;
      }

      // is there a LIMIT clause?
      if (subQ.toLowerCase().indexOf("limit") > 0) { // $NON-NLS-1$
        String limitS =
            subQ.toLowerCase()
                .substring(subQ.toLowerCase().indexOf("limit") + 5, subQ.length())
                .trim(); //$NON-NLS-1$
        limitS = limitS.replaceAll(";", ""); // $NON-NLS-1$ //$NON-NLS-2$
        try {
          m_rowLimit = Integer.parseInt(limitS);
        } catch (NumberFormatException ex) {
          logError(
              BaseMessages.getString(
                  PKG,
                  "CassandraInput.Error.UnableToParseLimitClause",
                  m_cqlSelectQuery)); //$NON-NLS-1$
          m_rowLimit = 10000;
        }
      }

      // strip off where clause (if any)
      if (subQ.toLowerCase().lastIndexOf("where") > 0) { // $NON-NLS-1$
        subQ = subQ.substring(0, subQ.toLowerCase().lastIndexOf("where")); // $NON-NLS-1$
      }

      // first determine the source table
      // look for a FROM that is surrounded by space
      int fromIndex = subQ.toLowerCase().indexOf("from"); // $NON-NLS-1$
      String tempS = subQ.toLowerCase();
      int offset = fromIndex;
      while (fromIndex > 0
          && tempS.charAt(fromIndex - 1) != ' '
          && (fromIndex + 4 < tempS.length())
          && tempS.charAt(fromIndex + 4) != ' ') {
        tempS = tempS.substring(fromIndex + 4, tempS.length());
        fromIndex = tempS.indexOf("from"); // $NON-NLS-1$
        offset += (4 + fromIndex);
      }

      fromIndex = offset;
      if (fromIndex < 0) {
        logError(
            BaseMessages.getString(PKG, "CassandraInput.Error.MustSpecifyATable")); // $NON-NLS-1$
        return; // no from clause
      }

      tableName = subQ.substring(fromIndex + 4, subQ.length()).trim();
      if (tableName.indexOf(' ') > 0) {
        tableName = tableName.substring(0, tableName.indexOf(' '));
      } else {
        tableName = tableName.replace(";", ""); // $NON-NLS-1$ //$NON-NLS-2$
      }

      if (tableName.length() == 0) {
        return; // no table specified
      }

      // is there a FIRST clause?
      if (subQ.toLowerCase().indexOf("first ") > 0) { // $NON-NLS-1$
        String firstS =
            subQ.substring(subQ.toLowerCase().indexOf("first") + 5, subQ.length())
                .trim(); //$NON-NLS-1$

        // Strip FIRST part from query
        subQ = firstS.substring(firstS.indexOf(' ') + 1, firstS.length());

        firstS = firstS.substring(0, firstS.indexOf(' '));
        try {
          m_colLimit = Integer.parseInt(firstS);
        } catch (NumberFormatException ex) {
          logError(
              BaseMessages.getString(
                  PKG,
                  "CassandraInput.Error.UnableToParseFirstClause",
                  m_cqlSelectQuery)); //$NON-NLS-1$
          return;
        }
      } else {
        subQ =
            subQ.substring(subQ.toLowerCase().indexOf("select") + 6, subQ.length()); // $NON-NLS-1$
      }

      // Reset FROM index
      fromIndex = subQ.toLowerCase().indexOf("from"); // $NON-NLS-1$

      // now determine if its a select */FIRST or specific set of columns
      Selector[] cols = null;
      if (subQ.indexOf("*") >= 0 && subQ.toLowerCase().indexOf("count(*)") == -1) { // $NON-NLS-1$
        // nothing special to do here
        m_isSelectStarQuery = true;
      } else {
        m_isSelectStarQuery = false;
        // String colsS = subQ.substring(subQ.indexOf('\''), fromIndex);
        String colsS = subQ.substring(0, fromIndex);
        // Parse select expression to get selectors: columns and functions
        cols = CQLUtils.getColumnsInSelect(colsS, true); // $NON-NLS-1$
      }

      // try and connect to get meta data
      String hostS = space.environmentSubstitute(m_cassandraHost);
      String portS = space.environmentSubstitute(m_cassandraPort);
      String userS = m_username;
      String passS = m_password;
      if (!Utils.isEmpty(userS) && !Utils.isEmpty(passS)) {
        userS = space.environmentSubstitute(m_username);
        passS = space.environmentSubstitute(m_password);
      }
      String keyspaceS = space.environmentSubstitute(m_cassandraKeyspace);
      Connection conn = null;
      Keyspace kSpace;
      try {

        Map<String, String> opts = new HashMap<String, String>();
        opts.put(
            CassandraUtils.CQLOptions.DATASTAX_DRIVER_VERSION,
            CassandraUtils.CQLOptions.CQL3_STRING);

        conn =
            CassandraUtils.getCassandraConnection(
                hostS,
                Integer.parseInt(portS),
                userS,
                passS,
                ConnectionFactory.Driver.BINARY_CQL3_PROTOCOL,
                opts);

        /*
         * conn = CassandraInputData.getCassandraConnection(hostS, Integer.parseInt(portS), userS, passS);
         * conn.setKeyspace(keyspaceS);
         */
        kSpace = conn.getKeyspace(keyspaceS);
      } catch (Exception ex) {
        ex.printStackTrace();
        logError(ex.getMessage(), ex);
        return;
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
          m_specificCols = new ArrayList<String>();
          for (Selector col : cols) {
            if (!col.isFunction() && !colMeta.columnExistsInSchema(col.getColumnName())) {
              // this one isn't known about in about in the schema - we can
              // output it
              // as long as its values satisfy the default validator...
              logBasic(
                  BaseMessages.getString(
                      PKG, "CassandraInput.Info.DefaultColumnValidator", col)); // $NON-NLS-1$
            }
            IValueMeta vm = colMeta.getValueMeta(col);
            rowMeta.addValueMeta(vm);
          }
        }
      } catch (Exception ex) {
        logBasic(
            BaseMessages.getString(
                PKG, "CassandraInput.Info.UnableToRetrieveColumnMetaData", tableName),
            ex); //$NON-NLS-1$
        return;
      } finally {
        if (conn != null) {
          try {
            conn.closeConnection();
          } catch (Exception e) {
            throw new HopTransformException(e);
          }
        }
      }
    }
  }

  public boolean isUseDriver() {
    return useDriver;
  }

  /**
   * Get the UI for this step.
   *
   * @param shell a <code>Shell</code> value
   * @param meta a <code>ITransformMeta</code> value
   * @param transMeta a <code>PipelineMeta</code> value
   * @param name a <code>String</code> value
   * @return a <code>ITransformDialog</code> value
   */
  public ITransformDialog getDialog(
      Shell shell, ITransformMeta meta, PipelineMeta transMeta, String name) {

    return new CassandraInputDialog(shell, meta, transMeta, name);
  }

  @Override
  public ITransform createTransform(
      TransformMeta arg0, CassandraInputData arg1, int arg2, PipelineMeta arg3, Pipeline arg4) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public CassandraInputData getTransformData() {
    return new CassandraInputData();
  }
}
