/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.di.trans.steps.cassandraoutput;

import java.util.List;
import java.util.Map;

import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Counter;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.w3c.dom.Node;

/**
 * Class providing an output step for writing data to a cassandra table. Can create the specified
 * table (if it doesn't already exist) and can update table meta data.
 */
@Step( id = "CassandraOutput", image = "Cassandraout.svg", name = "Cassandra output",
    description = "Writes to a Cassandra table",
    documentationUrl = "Products/Cassandra_Output",
    categoryDescription = "Big Data" )
@InjectionSupported( localizationPrefix = "CassandraOutput.Injection." )
public class CassandraOutputMeta extends BaseStepMeta implements StepMetaInterface {

  public static final Class<?> PKG = CassandraOutputMeta.class;

  /** The host to contact */
  @Injection( name = "CASSANDRA_HOST" )
  protected String m_cassandraHost = "localhost"; //$NON-NLS-1$

  /** The port that cassandra is listening on */
  @Injection( name = "CASSANDRA_PORT" )
  protected String m_cassandraPort = "9042"; //$NON-NLS-1$

  /** The username to use for authentication */
  @Injection( name = "USER_NAME" )
  protected String m_username;

  /** The password to use for authentication */
  @Injection( name = "PASSWORD" )
  protected String m_password;

  /** The keyspace (database) to use */
  @Injection( name = "CASSANDRA_KEYSPACE" )
  protected String m_cassandraKeyspace;

  /** The cassandra node to put schema updates through */
  @Injection( name = "SCHEMA_HOST" )
  protected String m_schemaHost;

  /** The port of the cassandra node for schema updates */
  @Injection( name = "SCHEMA_PORT" )
  protected String m_schemaPort;

  /** The table to write to */
  @Injection( name = "TABLE" )
  protected String m_table = ""; //$NON-NLS-1$

  /** The consistency level to use - null or empty string result in the default */
  @Injection( name = "CONSISTENCY_LEVEL" )
  protected String m_consistency = ""; //$NON-NLS-1$

  /**
   * The batch size - i.e. how many rows to collect before inserting them via a batch CQL statement
   */
  @Injection( name = "BATCH_SIZE" )
  protected String m_batchSize = "100"; //$NON-NLS-1$

  /** True if unlogged (i.e. non atomic) batch writes are to be used. CQL 3 only */
  @Injection( name = "USE_UNLOGGED_BATCH" )
  protected boolean m_unloggedBatch = false;

  /** Whether to use GZIP compression of CQL queries */
  @Injection( name = "USE_QUERY_COMPRESSION" )
  protected boolean m_useCompression = false;

  /** Whether to create the specified table if it doesn't exist */
  @Injection( name = "CREATE_TABLE" )
  protected boolean m_createTable = true;

  /** Anything to include in the WITH clause at table creation time? */
  @Injection( name = "CREATE_TABLE_WITH_CLAUSE" )
  protected String m_createTableWithClause;

  /** The field in the incoming data to use as the key for inserts */
  @Injection( name = "KEY_FIELD" )
  protected String m_keyField = ""; //$NON-NLS-1$

  /**
   * Timeout (milliseconds) to use for socket connections - blank means use cluster default
   */
  @Injection( name = "SOCKET_TIMEOUT" )
  protected String m_socketTimeout = ""; //$NON-NLS-1$

  /**
   * Timeout (milliseconds) to use for CQL batch inserts. If blank, no timeout is used. Otherwise, whent the timeout
   * occurs the step will try to kill the insert and re-try after splitting the batch according to the batch split
   * factor
   */
  @Injection( name = "BATCH_TIMEOUT" )
  protected String m_cqlBatchTimeout = ""; //$NON-NLS-1$

  /**
   * Default batch split size - only comes into play if cql batch timeout has been specified. Specifies the size of the
   * sub-batches to split the batch into if a timeout occurs.
   */
  @Injection( name = "SUB_BATCH_SIZE" )
  protected String m_cqlSubBatchSize = "10"; //$NON-NLS-1$

  /**
   * Whether or not to insert incoming fields that are not in the cassandra table's meta data. Has no affect if the user
   * has opted to update the meta data for unknown incoming fields
   */
  @Injection( name = "INSERT_FIELDS_NOT_IN_META" )
  protected boolean m_insertFieldsNotInMeta = false;

  /**
   * Whether or not to initially update the table meta data with any unknown incoming fields
   */
  @Injection( name = "UPDATE_CASSANDRA_META" )
  protected boolean m_updateCassandraMeta = false;

  /** Whether to truncate the table before inserting */
  @Injection( name = "TRUNCATE_TABLE" )
  protected boolean m_truncateTable = false;

  /**
   * Any CQL statements to execute before inserting the first row. Can be used, for example, to create secondary indexes
   * on columns in a table.
   */
  @Injection( name = "APRIORI_CQL" )
  protected String m_aprioriCQL = ""; //$NON-NLS-1$

  /**
   * Whether or not an exception generated when executing apriori CQL statements should stop the step
   */
  @Injection( name = "DONT_COMPLAIN_IF_APRIORI_CQL_FAILS" )
  protected boolean m_dontComplainAboutAprioriCQLFailing;

  /** Time to live (TTL) for inserts (affects all fields inserted) */
  @Injection( name = "TTL" )
  protected String m_ttl = ""; //$NON-NLS-1$

  @Injection( name = "TTL_UNIT" )
  protected String m_ttlUnit = TTLUnits.NONE.toString();

  public enum TTLUnits {
    NONE( BaseMessages.getString( PKG, "CassandraOutput.TTLUnit.None" ) ) { //$NON-NLS-1$
      @Override
      int convertToSeconds( int value ) {
        return -1;
      }
    },
    SECONDS( BaseMessages.getString( PKG, "CassandraOutput.TTLUnit.Seconds" ) ) { //$NON-NLS-1$
      @Override
      int convertToSeconds( int value ) {
        return value;
      }
    },
    MINUTES( BaseMessages.getString( PKG, "CassandraOutput.TTLUnit.Minutes" ) ) { //$NON-NLS-1$
      @Override
      int convertToSeconds( int value ) {
        return value * 60;
      }
    },
    HOURS( BaseMessages.getString( PKG, "CassandraOutput.TTLUnit.Hours" ) ) { //$NON-NLS-1$
      @Override
      int convertToSeconds( int value ) {
        return value * 60 * 60;
      }
    },
    DAYS( BaseMessages.getString( PKG, "CassandraOutput.TTLUnit.Days" ) ) { //$NON-NLS-1$
      @Override
      int convertToSeconds( int value ) {
        return value * 60 * 60 * 24;
      }
    };

    private final String m_stringVal;

    TTLUnits( String name ) {
      m_stringVal = name;
    }

    @Override
    public String toString() {
      return m_stringVal;
    }

    abstract int convertToSeconds( int value );
  }

  /**
   * Set the host for sending schema updates to
   *
   * @param s
   *          the host for sending schema updates to
   */
  public void setSchemaHost( String s ) {
    m_schemaHost = s;
  }

  /**
   * Set the host for sending schema updates to
   *
   * @return the host for sending schema updates to
   */
  public String getSchemaHost() {
    return m_schemaHost;
  }

  /**
   * Set the port for the schema update host
   *
   * @param p
   *          port for the schema update host
   */
  public void setSchemaPort( String p ) {
    m_schemaPort = p;
  }

  /**
   * Get the port for the schema update host
   *
   * @return port for the schema update host
   */
  public String getSchemaPort() {
    return m_schemaPort;
  }

  /**
   * Set how many sub-batches a batch should be split into when an insert times out.
   *
   * @param f
   *          the number of sub-batches to create when an insert times out.
   */
  public void setCQLSubBatchSize( String f ) {
    m_cqlSubBatchSize = f;
  }

  /**
   * Get how many sub-batches a batch should be split into when an insert times out.
   *
   * @return the number of sub-batches to create when an insert times out.
   */
  public String getCQLSubBatchSize() {
    return m_cqlSubBatchSize;
  }

  /**
   * Set the timeout for failing a batch insert attempt.
   *
   * @param t
   *          the time (milliseconds) to wait for a batch insert to succeed.
   */
  public void setCQLBatchInsertTimeout( String t ) {
    m_cqlBatchTimeout = t;
  }

  /**
   * Get the timeout for failing a batch insert attempt.
   *
   * @return the time (milliseconds) to wait for a batch insert to succeed.
   */
  public String getCQLBatchInsertTimeout() {
    return m_cqlBatchTimeout;
  }

  /**
   * Set the timeout (milliseconds) to use for socket comms
   *
   * @param t
   *          the timeout to use in milliseconds
   */
  public void setSocketTimeout( String t ) {
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
   * @param host
   *          the host to connect to
   */
  public void setCassandraHost( String host ) {
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
   * @param port
   *          the port that cassandra is listening on
   */
  public void setCassandraPort( String port ) {
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
   * Set the username to authenticate with
   *
   * @param un
   *          the username to authenticate with
   */
  public void setUsername( String un ) {
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
   * @param pass
   *          the password to authenticate with
   */
  public void setPassword( String pass ) {
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
   * Set the keyspace (db) to use
   *
   * @param keyspace
   *          the keyspace to use
   */
  public void setCassandraKeyspace( String keyspace ) {
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
   * Set the table to write to
   *
   * @param table
   *          the name of the table to write to
   */
  public void setTableName( String table ) {
    m_table = table;
  }

  /**
   * Get the name of the table to write to
   *
   * @return the name of the table to write to
   */
  public String getTableName() {
    return m_table;
  }

  /**
   * Set whether to create the specified table if it doesn't already exist
   *
   * @param create
   *          true if the specified table is to be created if it doesn't already exist
   */
  public void setCreateTable( boolean create ) {
    m_createTable = create;
  }

  /**
   * Get whether to create the specified table if it doesn't already exist
   *
   * @return true if the specified table is to be created if it doesn't already exist
   */
  public boolean getCreateTable() {
    return m_createTable;
  }

  public void setCreateTableClause( String w ) {
    m_createTableWithClause = w;
  }

  public String getCreateTableWithClause() {
    return m_createTableWithClause;
  }

  /**
   * Set the consistency to use (e.g. ONE, QUORUM etc).
   *
   * @param consistency
   *          the consistency to use
   */
  public void setConsistency( String consistency ) {
    m_consistency = consistency;
  }

  /**
   * Get the consistency to use
   *
   * @return the consistency
   */
  public String getConsistency() {
    return m_consistency;
  }

  /**
   * Set the batch size to use (i.e. max rows to send via a CQL batch insert statement)
   *
   * @param batchSize
   *          the max number of rows to send in each CQL batch insert
   */
  public void setBatchSize( String batchSize ) {
    m_batchSize = batchSize;
  }

  /**
   * Get the batch size to use (i.e. max rows to send via a CQL batch insert statement)
   *
   * @return the batch size.
   */
  public String getBatchSize() {
    return m_batchSize;
  }

  /**
   * Set whether unlogged batch writes (non-atomic) are to be used
   *
   * @param u
   *          true if unlogged batch operations are to be used
   */
  public void setUseUnloggedBatches( boolean u ) {
    m_unloggedBatch = u;
  }

  /**
   * Get whether unlogged batch writes (non-atomic) are to be used
   *
   * @return true if unlogged batch operations are to be used
   */
  public boolean getUseUnloggedBatch() {
    return m_unloggedBatch;
  }

  /**
   * Set whether to compress (GZIP) CQL queries when transmitting them to the server
   *
   * @param c
   *          true if CQL queries are to be compressed
   */
  public void setUseCompression( boolean c ) {
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
   * Set whether or not to insert any incoming fields that are not in the Cassandra table's column meta data. This has
   * no affect if the user has opted to first update the meta data with any unknown columns.
   *
   * @param insert
   *          true if incoming fields not found in the table's meta data are to be inserted (and validated according to
   *          the default validator for the table)
   */
  public void setInsertFieldsNotInMeta( boolean insert ) {
    m_insertFieldsNotInMeta = insert;
  }

  /**
   * Get whether or not to insert any incoming fields that are not in the Cassandra table's column meta data. This has
   * no affect if the user has opted to first update the meta data with any unknown columns.
   *
   * @return true if incoming fields not found in the table's meta data are to be inserted (and validated according to
   *         the default validator for the table)
   */
  public boolean getInsertFieldsNotInMeta() {
    return m_insertFieldsNotInMeta;
  }

  /**
   * Set the incoming field to use as the key for inserts
   *
   * @param keyField
   *          the name of the incoming field to use as the key
   */
  public void setKeyField( String keyField ) {
    m_keyField = keyField;
  }

  /**
   * Get the name of the incoming field to use as the key for inserts
   *
   * @return the name of the incoming field to use as the key for inserts
   */
  public String getKeyField() {
    return m_keyField;
  }

  /**
   * Set whether to update the table meta data with any unknown incoming columns
   *
   * @param u
   *          true if the meta data is to be updated with any unknown incoming columns
   */
  public void setUpdateCassandraMeta( boolean u ) {
    m_updateCassandraMeta = u;
  }

  /**
   * Get whether to update the table meta data with any unknown incoming columns
   *
   * @return true if the meta data is to be updated with any unknown incoming columns
   */
  public boolean getUpdateCassandraMeta() {
    return m_updateCassandraMeta;
  }

  /**
   * Set whether to first truncate (remove all data) the table before inserting.
   *
   * @param t
   *          true if the table is to be initially truncated.
   */
  public void setTruncateTable( boolean t ) {
    m_truncateTable = t;
  }

  /**
   * Get whether to first truncate (remove all data) the table before inserting.
   *
   * @return true if the table is to be initially truncated.
   */
  public boolean getTruncateTable() {
    return m_truncateTable;
  }

  /**
   * Set any cql statements (separated by ;'s) to execute before inserting the first row into the table. Can be
   * used to do tasks like creating secondary indexes on columns in the table.
   *
   * @param cql
   *          cql statements (separated by ;'s) to execute
   */
  public void setAprioriCQL( String cql ) {
    m_aprioriCQL = cql;
  }

  /**
   * Get any cql statements (separated by ;'s) to execute before inserting the first row into the table. Can be
   * used to do tasks like creating secondary indexes on columns in the table.
   *
   * @return cql statements (separated by ;'s) to execute
   */
  public String getAprioriCQL() {
    return m_aprioriCQL;
  }

  /**
   * Set whether to complain or not if any apriori CQL statements fail
   *
   * @param c
   *          true if failures should be ignored (but logged)
   */
  public void setDontComplainAboutAprioriCQLFailing( boolean c ) {
    m_dontComplainAboutAprioriCQLFailing = c;
  }

  /**
   * Get whether to complain or not if any apriori CQL statements fail
   *
   * @return true if failures should be ignored (but logged)
   */
  public boolean getDontComplainAboutAprioriCQLFailing() {
    return m_dontComplainAboutAprioriCQLFailing;
  }

  /**
   * Set the time to live for fields inserted. Null or empty indicates no TTL (i.e. fields don't expire).
   *
   * @param ttl
   *          the time to live to use
   */
  public void setTTL( String ttl ) {
    m_ttl = ttl;
  }

  /**
   * Get the time to live for fields inserted. Null or empty indicates no TTL (i.e. fields don't expire).
   *
   * @return the time to live to use
   */
  public String getTTL() {
    return m_ttl;
  }

  /**
   * Set the unit for the ttl
   *
   * @param unit
   *          the unit for the ttl
   */
  public void setTTLUnit( String unit ) {
    m_ttlUnit = unit;
  }

  /**
   * Get the unit for the ttl
   *
   * @return the unit for the ttl
   */
  public String getTTLUnit() {
    return m_ttlUnit;
  }

  @Override
  public String getXML() {
    StringBuffer retval = new StringBuffer();

    if ( !Utils.isEmpty( m_cassandraHost ) ) {
      retval.append( "\n    " ).append( //$NON-NLS-1$
          XMLHandler.addTagValue( "cassandra_host", m_cassandraHost ) ); //$NON-NLS-1$
    }

    if ( !Utils.isEmpty( m_cassandraPort ) ) {
      retval.append( "\n    " ).append( //$NON-NLS-1$
          XMLHandler.addTagValue( "cassandra_port", m_cassandraPort ) ); //$NON-NLS-1$
    }

    if ( !Utils.isEmpty( m_schemaHost ) ) {
      retval.append( "\n    " ).append( //$NON-NLS-1$
          XMLHandler.addTagValue( "schema_host", m_schemaHost ) ); //$NON-NLS-1$
    }

    if ( !Utils.isEmpty( m_schemaPort ) ) {
      retval.append( "\n    " ).append( //$NON-NLS-1$
          XMLHandler.addTagValue( "schema_port", m_schemaPort ) ); //$NON-NLS-1$
    }

    if ( !Utils.isEmpty( m_socketTimeout ) ) {
      retval.append( "\n    " ).append( //$NON-NLS-1$
          XMLHandler.addTagValue( "socket_timeout", m_socketTimeout ) ); //$NON-NLS-1$
    }

    if ( !Utils.isEmpty( m_password ) ) {
      retval.append( "\n    " ).append( //$NON-NLS-1$
          XMLHandler.addTagValue( "password", //$NON-NLS-1$
              Encr.encryptPasswordIfNotUsingVariables( m_password ) ) );
    }

    if ( !Utils.isEmpty( m_username ) ) {
      retval.append( "\n    " ).append( //$NON-NLS-1$
          XMLHandler.addTagValue( "username", m_username ) ); //$NON-NLS-1$
    }

    if ( !Utils.isEmpty( m_cassandraKeyspace ) ) {
      retval.append( "\n    " ).append( //$NON-NLS-1$
          XMLHandler.addTagValue( "cassandra_keyspace", m_cassandraKeyspace ) ); //$NON-NLS-1$
    }

    if ( !Utils.isEmpty( m_cassandraKeyspace ) ) {
      retval.append( "\n    " ).append( //$NON-NLS-1$
          XMLHandler.addTagValue( "cassandra_keyspace", m_cassandraKeyspace ) ); //$NON-NLS-1$
    }

    if ( !Utils.isEmpty( m_table ) ) {
      retval.append( "\n    " ).append( //$NON-NLS-1$
          XMLHandler.addTagValue( "table", m_table ) ); //$NON-NLS-1$
    }

    if ( !Utils.isEmpty( m_keyField ) ) {
      retval.append( "\n    " ).append( //$NON-NLS-1$
          XMLHandler.addTagValue( "key_field", m_keyField ) ); //$NON-NLS-1$
    }

    if ( !Utils.isEmpty( m_consistency ) ) {
      retval.append( "\n    " ).append( //$NON-NLS-1$
          XMLHandler.addTagValue( "consistency", m_consistency ) ); //$NON-NLS-1$
    }

    if ( !Utils.isEmpty( m_batchSize ) ) {
      retval.append( "\n    " ).append( //$NON-NLS-1$
          XMLHandler.addTagValue( "batch_size", m_batchSize ) ); //$NON-NLS-1$
    }

    if ( !Utils.isEmpty( m_cqlBatchTimeout ) ) {
      retval.append( "\n    " ).append( //$NON-NLS-1$
          XMLHandler.addTagValue( "cql_batch_timeout", m_cqlBatchTimeout ) ); //$NON-NLS-1$
    }

    if ( !Utils.isEmpty( m_cqlSubBatchSize ) ) {
      retval.append( "\n    " ).append( //$NON-NLS-1$
          XMLHandler.addTagValue( "cql_sub_batch_size", m_cqlSubBatchSize ) ); //$NON-NLS-1$
    }

    retval.append( "\n    " ).append( //$NON-NLS-1$
        XMLHandler.addTagValue( "create_table", m_createTable ) ); //$NON-NLS-1$

    retval.append( "\n    " ).append( //$NON-NLS-1$
        XMLHandler.addTagValue( "use_compression", m_useCompression ) ); //$NON-NLS-1$

    retval.append( "\n    " ).append( //$NON-NLS-1$
        XMLHandler.addTagValue( "insert_fields_not_in_meta", //$NON-NLS-1$
            m_insertFieldsNotInMeta ) );

    retval.append( "\n    " ).append( //$NON-NLS-1$
        XMLHandler.addTagValue( "update_cassandra_meta", m_updateCassandraMeta ) ); //$NON-NLS-1$

    retval.append( "\n    " ).append( //$NON-NLS-1$
        XMLHandler.addTagValue( "truncate_table", m_truncateTable ) ); //$NON-NLS-1$

    retval.append( "\n    " ).append( //$NON-NLS-1$
        XMLHandler.addTagValue( "unlogged_batch", m_unloggedBatch ) ); //$NON-NLS-1$

    retval.append( "\n    " ).append( //$NON-NLS-1$
        XMLHandler.addTagValue( "dont_complain_apriori_cql", //$NON-NLS-1$
            m_dontComplainAboutAprioriCQLFailing ) );

    if ( !Utils.isEmpty( m_aprioriCQL ) ) {
      retval.append( "\n    " ).append( //$NON-NLS-1$
          XMLHandler.addTagValue( "apriori_cql", m_aprioriCQL ) ); //$NON-NLS-1$
    }

    if ( !Utils.isEmpty( m_createTableWithClause ) ) {
      retval.append( "\n    " ).append( //$NON-NLS-1$
          XMLHandler.addTagValue( "create_table_with_clause", //$NON-NLS-1$
              m_createTableWithClause ) );
    }

    if ( !Utils.isEmpty( m_ttl ) ) {
      retval.append( "\n    " ).append( XMLHandler.addTagValue( "ttl", m_ttl ) ); //$NON-NLS-1$ //$NON-NLS-2$
    }

    retval.append( "\n    " ).append( //$NON-NLS-1$
        XMLHandler.addTagValue( "ttl_unit", m_ttlUnit ) ); //$NON-NLS-1$

    return retval.toString();
  }

  @Override
  public void loadXML( Node stepnode, List<DatabaseMeta> databases, Map<String, Counter> counters )
    throws KettleXMLException {
    m_cassandraHost = XMLHandler.getTagValue( stepnode, "cassandra_host" ); //$NON-NLS-1$
    m_cassandraPort = XMLHandler.getTagValue( stepnode, "cassandra_port" ); //$NON-NLS-1$
    m_schemaHost = XMLHandler.getTagValue( stepnode, "schema_host" ); //$NON-NLS-1$
    m_schemaPort = XMLHandler.getTagValue( stepnode, "schema_port" ); //$NON-NLS-1$
    m_socketTimeout = XMLHandler.getTagValue( stepnode, "socket_timeout" ); //$NON-NLS-1$
    m_username = XMLHandler.getTagValue( stepnode, "username" ); //$NON-NLS-1$
    m_password = XMLHandler.getTagValue( stepnode, "password" ); //$NON-NLS-1$
    if ( !Utils.isEmpty( m_password ) ) {
      m_password = Encr.decryptPasswordOptionallyEncrypted( m_password );
    }
    m_cassandraKeyspace = XMLHandler.getTagValue( stepnode, "cassandra_keyspace" ); //$NON-NLS-1$
    m_table = XMLHandler.getTagValue( stepnode, "table" ); //$NON-NLS-1$
    m_keyField = XMLHandler.getTagValue( stepnode, "key_field" ); //$NON-NLS-1$
    m_consistency = XMLHandler.getTagValue( stepnode, "consistency" ); //$NON-NLS-1$
    m_batchSize = XMLHandler.getTagValue( stepnode, "batch_size" ); //$NON-NLS-1$
    m_cqlBatchTimeout = XMLHandler.getTagValue( stepnode, "cql_batch_timeout" ); //$NON-NLS-1$
    m_cqlSubBatchSize = XMLHandler.getTagValue( stepnode, "cql_sub_batch_size" ); //$NON-NLS-1$

    // check legacy column family tag first
    String createColFamStr = XMLHandler.getTagValue( stepnode, "create_column_family" ); //$NON-NLS-1$ //$NON-NLS-2$
    m_createTable = !Utils.isEmpty( createColFamStr ) ? createColFamStr.equalsIgnoreCase( "Y" )
        : XMLHandler.getTagValue( stepnode, "create_table" ).equalsIgnoreCase( "Y" );

    m_useCompression = XMLHandler.getTagValue( stepnode, "use_compression" ) //$NON-NLS-1$
        .equalsIgnoreCase( "Y" ); //$NON-NLS-1$
    m_insertFieldsNotInMeta = XMLHandler.getTagValue( stepnode, "insert_fields_not_in_meta" ).equalsIgnoreCase( "Y" ); //$NON-NLS-1$ //$NON-NLS-2$
    m_updateCassandraMeta = XMLHandler.getTagValue( stepnode, "update_cassandra_meta" ).equalsIgnoreCase( "Y" ); //$NON-NLS-1$ //$NON-NLS-2$

    String truncateColFamStr = XMLHandler.getTagValue( stepnode, "truncate_column_family" ); //$NON-NLS-1$ //$NON-NLS-2$
    m_truncateTable = !Utils.isEmpty( truncateColFamStr ) ? truncateColFamStr.equalsIgnoreCase( "Y" )
        : XMLHandler.getTagValue( stepnode, "truncate_table" ).equalsIgnoreCase( "Y" ); //$NON-NLS-1$ //$NON-NLS-2$

    m_aprioriCQL = XMLHandler.getTagValue( stepnode, "apriori_cql" ); //$NON-NLS-1$

    m_createTableWithClause = XMLHandler.getTagValue( stepnode, "create_table_with_clause" ); //$NON-NLS-1$

    String unloggedBatch = XMLHandler.getTagValue( stepnode, "unlogged_batch" ); //$NON-NLS-1$
    if ( !Utils.isEmpty( unloggedBatch ) ) {
      m_unloggedBatch = unloggedBatch.equalsIgnoreCase( "Y" ); //$NON-NLS-1$
    }

    String dontComplain = XMLHandler.getTagValue( stepnode, "dont_complain_apriori_cql" ); //$NON-NLS-1$
    if ( !Utils.isEmpty( dontComplain ) ) {
      m_dontComplainAboutAprioriCQLFailing = dontComplain.equalsIgnoreCase( "Y" ); //$NON-NLS-1$
    }

    m_ttl = XMLHandler.getTagValue( stepnode, "ttl" ); //$NON-NLS-1$
    m_ttlUnit = XMLHandler.getTagValue( stepnode, "ttl_unit" ); //$NON-NLS-1$

    if ( Utils.isEmpty( m_ttlUnit ) ) {
      m_ttlUnit = TTLUnits.NONE.toString();
    }
  }

  @Override
  public void readRep( Repository rep, ObjectId id_step, List<DatabaseMeta> databases, Map<String, Counter> counters )
    throws KettleException {
    m_cassandraHost = rep.getStepAttributeString( id_step, 0, "cassandra_host" ); //$NON-NLS-1$
    m_cassandraPort = rep.getStepAttributeString( id_step, 0, "cassandra_port" ); //$NON-NLS-1$
    m_schemaHost = rep.getStepAttributeString( id_step, 0, "schema_host" ); //$NON-NLS-1$
    m_schemaPort = rep.getStepAttributeString( id_step, 0, "schema_port" ); //$NON-NLS-1$
    m_socketTimeout = rep.getStepAttributeString( id_step, 0, "socket_timeout" ); //$NON-NLS-1$
    m_username = rep.getStepAttributeString( id_step, 0, "username" ); //$NON-NLS-1$
    m_password = rep.getStepAttributeString( id_step, 0, "password" ); //$NON-NLS-1$
    if ( !Utils.isEmpty( m_password ) ) {
      m_password = Encr.decryptPasswordOptionallyEncrypted( m_password );
    }
    m_cassandraKeyspace = rep.getStepAttributeString( id_step, 0, "cassandra_keyspace" ); //$NON-NLS-1$
    m_table = rep.getStepAttributeString( id_step, 0, "table" ); //$NON-NLS-1$
    // try legacy identifier if table does not work
    if ( Utils.isEmpty( m_table ) ) {
      m_table = rep.getStepAttributeString( id_step, 0, "column_family" ); //$NON-NLS-1$
    }
    m_keyField = rep.getStepAttributeString( id_step, 0, "key_field" ); //$NON-NLS-1$
    m_consistency = rep.getStepAttributeString( id_step, 0, "consistency" ); //$NON-NLS-1$
    m_batchSize = rep.getStepAttributeString( id_step, 0, "batch_size" ); //$NON-NLS-1$
    m_cqlBatchTimeout = rep.getStepAttributeString( id_step, 0, "cql_batch_timeout" ); //$NON-NLS-1$
    m_cqlSubBatchSize = rep.getStepAttributeString( id_step, 0, "cql_sub_batch_size" ); //$NON-NLS-1$

    try {
      m_createTable = rep.getStepAttributeBoolean( id_step, 0, "create_table" );
    } catch ( Exception e ) {
      // try legacy identifier
      m_createTable = rep.getStepAttributeBoolean( id_step, 0, "create_column_family" ); //$NON-NLS-1$
    }

    m_useCompression = rep.getStepAttributeBoolean( id_step, 0, "use_compression" ); //$NON-NLS-1$
    m_insertFieldsNotInMeta = rep.getStepAttributeBoolean( id_step, 0, "insert_fields_not_in_meta" ); //$NON-NLS-1$
    m_updateCassandraMeta = rep.getStepAttributeBoolean( id_step, 0, "update_cassandra_meta" ); //$NON-NLS-1$

    try {
      m_truncateTable = rep.getStepAttributeBoolean( id_step, 0, "truncate_table" ); //$NON-NLS-1$
    } catch ( Exception e ) {
      // try legacy identifier
      m_truncateTable = rep.getStepAttributeBoolean( id_step, 0, "truncate_column_family" ); //$NON-NLS-1$
    }

    m_unloggedBatch = rep.getStepAttributeBoolean( id_step, 0, "unlogged_batch" ); //$NON-NLS-1$

    m_aprioriCQL = rep.getStepAttributeString( id_step, 0, "apriori_cql" ); //$NON-NLS-1$

    m_createTableWithClause = rep.getStepAttributeString( id_step, 0, "create_table_with_clause" ); //$NON-NLS-1$

    m_dontComplainAboutAprioriCQLFailing = rep.getStepAttributeBoolean( id_step, 0, "dont_complain_aprior_cql" ); //$NON-NLS-1$

    m_ttl = rep.getStepAttributeString( id_step, 0, "ttl" ); //$NON-NLS-1$
    m_ttlUnit = rep.getStepAttributeString( id_step, 0, "ttl_unit" ); //$NON-NLS-1$

    if ( Utils.isEmpty( m_ttlUnit ) ) {
      m_ttlUnit = TTLUnits.NONE.toString();
    }
  }

  @Override
  public void saveRep( Repository rep, ObjectId id_transformation, ObjectId id_step ) throws KettleException {
    if ( !Utils.isEmpty( m_cassandraHost ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "cassandra_host", //$NON-NLS-1$
          m_cassandraHost );
    }

    if ( !Utils.isEmpty( m_cassandraPort ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "cassandra_port", //$NON-NLS-1$
          m_cassandraPort );
    }

    if ( !Utils.isEmpty( m_schemaHost ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "schema_host", //$NON-NLS-1$
          m_schemaHost );
    }

    if ( !Utils.isEmpty( m_schemaPort ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "schema_port", //$NON-NLS-1$
          m_schemaPort );
    }

    if ( !Utils.isEmpty( m_socketTimeout ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "socket_timeout", //$NON-NLS-1$
          m_socketTimeout );
    }

    if ( !Utils.isEmpty( m_username ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "username", //$NON-NLS-1$
          m_username );
    }

    if ( !Utils.isEmpty( m_password ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "password", //$NON-NLS-1$
          Encr.encryptPasswordIfNotUsingVariables( m_password ) );
    }

    if ( !Utils.isEmpty( m_cassandraKeyspace ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "cassandra_keyspace", m_cassandraKeyspace ); //$NON-NLS-1$
    }

    if ( !Utils.isEmpty( m_table ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "table", //$NON-NLS-1$
          m_table );
    }

    if ( !Utils.isEmpty( m_keyField ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "key_field", //$NON-NLS-1$
          m_keyField );
    }

    if ( !Utils.isEmpty( m_consistency ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "consistency", //$NON-NLS-1$
          m_consistency );
    }

    if ( !Utils.isEmpty( m_batchSize ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "batch_size", //$NON-NLS-1$
          m_batchSize );
    }

    if ( !Utils.isEmpty( m_cqlBatchTimeout ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "cql_batch_timeout", //$NON-NLS-1$
          m_cqlBatchTimeout );
    }

    if ( !Utils.isEmpty( m_cqlSubBatchSize ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "cql_sub_batch_size", m_cqlSubBatchSize ); //$NON-NLS-1$
    }

    rep.saveStepAttribute( id_transformation, id_step, 0, "create_table", m_createTable ); //$NON-NLS-1$
    rep.saveStepAttribute( id_transformation, id_step, 0, "use_compression", //$NON-NLS-1$
        m_useCompression );
    rep.saveStepAttribute( id_transformation, id_step, 0, "insert_fields_not_in_meta", m_insertFieldsNotInMeta ); //$NON-NLS-1$
    rep.saveStepAttribute( id_transformation, id_step, 0, "update_cassandra_meta", m_updateCassandraMeta ); //$NON-NLS-1$
    rep.saveStepAttribute( id_transformation, id_step, 0, "truncate_table", m_truncateTable ); //$NON-NLS-1$
    rep.saveStepAttribute( id_transformation, id_step, 0, "unlogged_batch", //$NON-NLS-1$
        m_unloggedBatch );

    if ( !Utils.isEmpty( m_aprioriCQL ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "apriori_cql", //$NON-NLS-1$
          m_aprioriCQL );
    }

    if ( !Utils.isEmpty( m_createTableWithClause ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "create_table_with_clause", m_aprioriCQL ); //$NON-NLS-1$
    }

    if ( !Utils.isEmpty( m_ttl ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "ttl", m_ttl ); //$NON-NLS-1$
    }

    rep.saveStepAttribute( id_transformation, id_step, 0, "ttl_unit", m_ttlUnit ); //$NON-NLS-1$

    rep.saveStepAttribute( id_transformation, id_step, 0,
        "dont_complain_apriori_cql", m_dontComplainAboutAprioriCQLFailing ); //$NON-NLS-1$
  }

  @Override
  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev,
      String[] input, String[] output, RowMetaInterface info ) {

    CheckResult cr;

    if ( ( prev == null ) || ( prev.size() == 0 ) ) {
      cr = new CheckResult( CheckResult.TYPE_RESULT_WARNING, "Not receiving any fields from previous steps!", stepMeta ); //$NON-NLS-1$
      remarks.add( cr );
    } else {
      cr = new CheckResult( CheckResult.TYPE_RESULT_OK, "Step is connected to previous one, receiving " + prev.size() //$NON-NLS-1$
          + " fields", stepMeta ); //$NON-NLS-1$
      remarks.add( cr );
    }

    // See if we have input streams leading to this step!
    if ( input.length > 0 ) {
      cr = new CheckResult( CheckResult.TYPE_RESULT_OK, "Step is receiving info from other steps.", stepMeta ); //$NON-NLS-1$
      remarks.add( cr );
    } else {
      cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, "No input received from other steps!", stepMeta ); //$NON-NLS-1$
      remarks.add( cr );
    }
  }

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
      TransMeta transMeta, Trans trans ) {

    return new CassandraOutput( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override
  public StepDataInterface getStepData() {
    return new CassandraOutputData();
  }

  @Override
  public void setDefault() {
    m_cassandraHost = "localhost"; //$NON-NLS-1$
    m_cassandraPort = "9042"; //$NON-NLS-1$
    m_schemaHost = "localhost"; //$NON-NLS-1$
    m_schemaPort = "9042"; //$NON-NLS-1$
    m_table = ""; //$NON-NLS-1$
    m_batchSize = "100"; //$NON-NLS-1$
    m_useCompression = false;
    m_insertFieldsNotInMeta = false;
    m_updateCassandraMeta = false;
    m_truncateTable = false;
    m_aprioriCQL = ""; //$NON-NLS-1$
  }

  /*
   * (non-Javadoc)
   *
   * @see org.pentaho.di.trans.step.BaseStepMeta#getDialogClassName()
   */
  @Override
  public String getDialogClassName() {
    return "org.pentaho.di.trans.steps.cassandraoutput.CassandraOutputDialog"; //$NON-NLS-1$
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }
}
