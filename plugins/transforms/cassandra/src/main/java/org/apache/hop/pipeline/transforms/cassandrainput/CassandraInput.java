/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

import java.util.HashMap;
import java.util.Map;

import org.pentaho.cassandra.util.CassandraUtils;
import org.pentaho.cassandra.ConnectionFactory;
import org.pentaho.cassandra.spi.CQLRowHandler;
import org.pentaho.cassandra.spi.ITableMetaData;
import org.pentaho.cassandra.spi.Connection;
import org.pentaho.cassandra.spi.Keyspace;
import org.pentaho.cassandra.util.Compression;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

/**
 * Class providing an input step for reading data from a table in Cassandra. Accesses the schema
 * information stored in Cassandra for type information.
 * 
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 * @version $Revision$
 */
public class CassandraInput extends BaseStep implements StepInterface {

  protected CassandraInputMeta m_meta;
  protected CassandraInputData m_data;

  public CassandraInput( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
      Trans trans ) {

    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  /** Connection to cassandra */
  protected Connection m_connection;

  /** Keyspace */
  protected Keyspace m_keyspace;

  /** Column meta data and schema information */
  protected ITableMetaData m_cassandraMeta;

  /** Handler for CQL-based row fetching */
  protected CQLRowHandler m_cqlHandler;

  /**
   * map of indexes into the output field structure (key is special - it's always the first field in the output row meta
   */
  protected Map<String, Integer> m_outputFormatMap = new HashMap<String, Integer>();

  /** Current input row being processed (if executing for each row) */
  protected Object[] m_currentInputRowDrivingQuery = null;

  /** Column family name */
  protected String m_tableName;

  @Override
  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {

    if ( !isStopped() ) {

      if ( m_meta.getExecuteForEachIncomingRow() && m_currentInputRowDrivingQuery == null ) {
        m_currentInputRowDrivingQuery = getRow();

        if ( m_currentInputRowDrivingQuery == null ) {
          // no more input, no more queries to make
          setOutputDone();
          return false;
        }

        if ( !first ) {
          initQuery();
        }
      }

      if ( first ) {
        first = false;

        // Get the connection to Cassandra
        String hostS = environmentSubstitute( m_meta.getCassandraHost() );
        String portS = environmentSubstitute( m_meta.getCassandraPort() );
        String timeoutS = environmentSubstitute( m_meta.getSocketTimeout() );
        String maxLength = environmentSubstitute( m_meta.getMaxLength() );
        String userS = m_meta.getUsername();
        String passS = m_meta.getPassword();
        if ( !Utils.isEmpty( userS ) && !Utils.isEmpty( passS ) ) {
          userS = environmentSubstitute( userS );
          passS = environmentSubstitute( passS );
        }
        String keyspaceS = environmentSubstitute( m_meta.getCassandraKeyspace() );

        if ( Utils.isEmpty( hostS ) || Utils.isEmpty( portS ) || Utils.isEmpty( keyspaceS ) ) {
          throw new KettleException( "Some connection details are missing!!" ); //$NON-NLS-1$
        }

        logBasic( BaseMessages.getString( CassandraInputMeta.PKG,
            "CassandraInput.Info.Connecting", hostS, portS, keyspaceS ) ); //$NON-NLS-1$

        Map<String, String> opts = new HashMap<String, String>();

        if ( !Utils.isEmpty( timeoutS ) ) {
          opts.put( CassandraUtils.ConnectionOptions.SOCKET_TIMEOUT, timeoutS );
        }

        if ( !Utils.isEmpty( maxLength ) ) {
          opts.put( CassandraUtils.ConnectionOptions.MAX_LENGTH, maxLength );
        }

        opts.put( CassandraUtils.CQLOptions.DATASTAX_DRIVER_VERSION, CassandraUtils.CQLOptions.CQL3_STRING );

        if ( m_meta.getUseCompression() ) {
          opts.put( CassandraUtils.ConnectionOptions.COMPRESSION, Boolean.TRUE.toString() );
        }

        if ( opts.size() > 0 ) {
          logBasic( BaseMessages.getString( CassandraInputMeta.PKG, "CassandraInput.Info.UsingConnectionOptions", //$NON-NLS-1$
              CassandraUtils.optionsToString( opts ) ) );
        }

        try {
          m_connection =
              CassandraUtils.getCassandraConnection( hostS, Integer.parseInt( portS ), userS, passS,
                  ConnectionFactory.Driver.BINARY_CQL3_PROTOCOL, opts );

          m_keyspace = m_connection.getKeyspace( keyspaceS );
        } catch ( Exception ex ) {
          closeConnection();
          throw new KettleException( ex.getMessage(), ex );
        }

        // check the source table first
        m_tableName =
            CassandraUtils.getTableNameFromCQLSelectQuery( environmentSubstitute( m_meta.getCQLSelectQuery() ) );

        if ( Utils.isEmpty( m_tableName ) ) {
          throw new KettleException( BaseMessages.getString( CassandraInputMeta.PKG,
              "CassandraInput.Error.NonExistentTable" ) ); //$NON-NLS-1$
        }

        try {
          if ( !m_keyspace.tableExists( m_tableName ) ) {
            throw new KettleException(
                BaseMessages
                    .getString(
                        CassandraInputMeta.PKG,
                        "CassandraInput.Error.NonExistentTable", CassandraUtils.removeQuotes( m_tableName ), //$NON-NLS-1$
                        keyspaceS ) );
          }
        } catch ( Exception ex ) {
          closeConnection();

          throw new KettleException( ex.getMessage(), ex );
        }

        // set up the output row meta
        m_data.setOutputRowMeta( new RowMeta() );
        m_meta.getFields( m_data.getOutputRowMeta(), getStepname(), null, null, this );

        // check that there are some outgoing fields!
        if ( m_data.getOutputRowMeta().size() == 0 ) {
          throw new KettleException( BaseMessages.getString( CassandraInputMeta.PKG,
              "CassandraInput.Error.QueryWontProduceOutputFields" ) ); //$NON-NLS-1$
        }

        // set up the lookup map
        for ( int i = 0; i < m_data.getOutputRowMeta().size(); i++ ) {
          String fieldName = m_data.getOutputRowMeta().getValueMeta( i ).getName();
          m_outputFormatMap.put( fieldName, i );
        }

        // table name (key) is the first field output
        try {
          logBasic( BaseMessages
              .getString( CassandraInputMeta.PKG, "CassandraInput.Info.GettingMetaData", m_tableName ) ); //$NON-NLS-1$

          m_cassandraMeta = m_keyspace.getTableMetaData( m_tableName );
        } catch ( Exception e ) {
          closeConnection();
          throw new KettleException( e.getMessage(), e );
        }

        initQuery();
      }

      Object[][] outRowData = new Object[1][];
      try {
        outRowData = m_cqlHandler.getNextOutputRow( m_data.getOutputRowMeta(), m_outputFormatMap );
      } catch ( Exception e ) {
        throw new KettleException( e.getMessage(), e );
      }

      if ( outRowData != null ) {
        for ( Object[] r : outRowData ) {
          putRow( m_data.getOutputRowMeta(), r );
        }

        if ( log.isRowLevel() ) {
          log.logRowlevel( toString(), "Outputted row #" + getProcessed() //$NON-NLS-1$
              + " : " + outRowData ); //$NON-NLS-1$
        }

        if ( checkFeedback( getProcessed() ) ) {
          logBasic( "Read " + getProcessed() + " rows from Cassandra" ); //$NON-NLS-1$ //$NON-NLS-2$
        }
      }

      if ( outRowData == null ) {
        if ( !m_meta.getExecuteForEachIncomingRow() ) {
          // we're done now
          closeConnection();
          setOutputDone();
          return false;
        } else {
          m_currentInputRowDrivingQuery = null; // finished with this row
        }
      }
    } else {
      closeConnection();
      return false;
    }

    return true;
  }

  @Override
  public boolean init( StepMetaInterface stepMeta, StepDataInterface stepData ) {
    if ( super.init( stepMeta, stepData ) ) {
      m_data = (CassandraInputData) stepData;
      m_meta = (CassandraInputMeta) stepMeta;
    }

    return true;
  }

  protected void initQuery() throws KettleException {
    String queryS = environmentSubstitute( m_meta.getCQLSelectQuery() );
    if ( m_meta.getExecuteForEachIncomingRow() ) {
      queryS = fieldSubstitute( queryS, getInputRowMeta(), m_currentInputRowDrivingQuery );
    }
    Compression compression = m_meta.getUseCompression() ? Compression.GZIP : Compression.NONE;
    try {
      logBasic( BaseMessages.getString( CassandraInputMeta.PKG, "CassandraInput.Info.ExecutingQuery", //$NON-NLS-1$
          queryS, ( m_meta.getUseCompression() ? BaseMessages.getString( CassandraInputMeta.PKG,
              "CassandraInput.Info.UsingGZIPCompression" ) : "" ) ) ); //$NON-NLS-!$ //$NON-NLS-2$
      if ( m_cqlHandler == null ) {
        m_cqlHandler = m_keyspace.getCQLRowHandler();
      }
      m_cqlHandler.newRowQuery( this, m_tableName, queryS, compression.name(), "", log );
    } catch ( Exception e ) {
      closeConnection();

      throw new KettleException( e.getMessage(), e );
    }
  }

  @Override
  public void setStopped( boolean stopped ) {
    if ( isStopped() && stopped == true ) {
      return;
    }
    super.setStopped( stopped );
  }

  @Override
  public void dispose( StepMetaInterface smi, StepDataInterface sdi ) {
    try {
      closeConnection();
    } catch ( KettleException e ) {
      e.printStackTrace();
    }
  }

  protected void closeConnection() throws KettleException {
    if ( m_connection != null ) {
      logBasic( BaseMessages.getString( CassandraInputMeta.PKG, "CassandraInput.Info.ClosingConnection" ) ); //$NON-NLS-1$
      try {
        m_connection.closeConnection();
        m_connection = null;
      } catch ( Exception e ) {
        throw new KettleException( e.getMessage(), e );
      }
    }
  }
}
