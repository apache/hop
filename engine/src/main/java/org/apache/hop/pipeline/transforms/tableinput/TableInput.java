/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.tableinput;

import org.apache.hop.core.Const;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Reads information from a database table by using freehand SQL
 *
 * @author Matt
 * @since 8-apr-2003
 */
public class TableInput extends BaseTransform<TableInputMeta, TableInputData> implements ITransform<TableInputMeta, TableInputData> {

  private static final Class<?> PKG = TableInputMeta.class; // For Translator

  public TableInput( TransformMeta transformMeta, TableInputMeta meta, TableInputData data, int copyNr, PipelineMeta pipelineMeta,
                     Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  private RowMetaAndData readStartDate() throws HopException {
    if ( log.isDetailed() ) {
      logDetailed( "Reading from transform [" + data.infoStream.getTransformName() + "]" );
    }

    IRowMeta parametersMeta = new RowMeta();
    Object[] parametersData = new Object[] {};

    IRowSet rowSet = findInputRowSet( data.infoStream.getTransformName() );
    if ( rowSet != null ) {
      Object[] rowData = getRowFrom( rowSet ); // rows are originating from "lookup_from"
      while ( rowData != null ) {
        parametersData = RowDataUtil.addRowData( parametersData, parametersMeta.size(), rowData );
        parametersMeta.addRowMeta( rowSet.getRowMeta() );

        rowData = getRowFrom( rowSet ); // take all input rows if needed!
      }

      if ( parametersMeta.size() == 0 ) {
        throw new HopException( "Expected to read parameters from transform ["
          + data.infoStream.getTransformName() + "] but none were found." );
      }
    } else {
      throw new HopException( "Unable to find rowset to read from, perhaps transform ["
        + data.infoStream.getTransformName() + "] doesn't exist. (or perhaps you are trying a preview?)" );
    }

    RowMetaAndData parameters = new RowMetaAndData( parametersMeta, parametersData );

    return parameters;
  }

  public boolean processRow() throws HopException {
    if ( first ) { // we just got started

      Object[] parameters;
      IRowMeta parametersMeta;
      first = false;

      // Make sure we read data from source transforms...
      if ( data.infoStream.getTransformMeta() != null ) {
        if ( meta.isExecuteEachInputRow() ) {
          if ( log.isDetailed() ) {
            logDetailed( "Reading single row from stream [" + data.infoStream.getTransformName() + "]" );
          }
          data.rowSet = findInputRowSet( data.infoStream.getTransformName() );
          if ( data.rowSet == null ) {
            throw new HopException( "Unable to find rowset to read from, perhaps transform ["
              + data.infoStream.getTransformName() + "] doesn't exist. (or perhaps you are trying a preview?)" );
          }
          parameters = getRowFrom( data.rowSet );
          parametersMeta = data.rowSet.getRowMeta();
        } else {
          if ( log.isDetailed() ) {
            logDetailed( "Reading query parameters from stream [" + data.infoStream.getTransformName() + "]" );
          }
          RowMetaAndData rmad = readStartDate(); // Read values in lookup table (look)
          parameters = rmad.getData();
          parametersMeta = rmad.getRowMeta();
        }
        if ( parameters != null ) {
          if ( log.isDetailed() ) {
            logDetailed( "Query parameters found = " + parametersMeta.getString( parameters ) );
          }
        }
      } else {
        parameters = new Object[] {};
        parametersMeta = new RowMeta();
      }

      if ( meta.isExecuteEachInputRow() && ( parameters == null || parametersMeta.size() == 0 ) ) {
        setOutputDone(); // signal end to receiver(s)
        return false; // stop immediately, nothing to do here.
      }

      boolean success = doQuery( parametersMeta, parameters );
      if ( !success ) {
        return false;
      }
    } else {
      if ( data.thisrow != null ) { // We can expect more rows

        try {
          data.nextrow = data.db.getRow( data.rs, false );
        } catch ( HopDatabaseException e ) {
          if ( e.getCause() instanceof SQLException && isStopped() ) {
            //This exception indicates we tried reading a row after the statment for this transform was cancelled
            //this is expected and ok so do not pass the exception up
            logDebug( e.getMessage() );
            return false;
          } else {
            throw e;
          }
        }
        if ( data.nextrow != null ) {
          incrementLinesInput();
        }
      }
    }

    if ( data.thisrow == null ) { // Finished reading?

      boolean done = false;
      if ( meta.isExecuteEachInputRow() ) { // Try to get another row from the input stream
        Object[] nextRow = getRowFrom( data.rowSet );
        if ( nextRow == null ) { // Nothing more to get!

          done = true;
        } else {
          // First close the previous query, otherwise we run out of cursors!
          closePreviousQuery();

          boolean success = doQuery( data.rowSet.getRowMeta(), nextRow ); // OK, perform a new query
          if ( !success ) {
            return false;
          }

          if ( data.thisrow != null ) {
            putRow( data.rowMeta, data.thisrow ); // fill the rowset(s). (wait for empty)
            data.thisrow = data.nextrow;

            if ( checkFeedback( getLinesInput() ) ) {
              if ( log.isBasic() ) {
                logBasic( "linenr " + getLinesInput() );
              }
            }
          }
        }
      } else {
        done = true;
      }

      if ( done ) {
        setOutputDone(); // signal end to receiver(s)
        return false; // end of data or error.
      }
    } else {
      putRow( data.rowMeta, data.thisrow ); // fill the rowset(s). (wait for empty)
      data.thisrow = data.nextrow;

      if ( checkFeedback( getLinesInput() ) ) {
        if ( log.isBasic() ) {
          logBasic( "linenr " + getLinesInput() );
        }
      }
    }

    return true;
  }

  private void closePreviousQuery() throws HopDatabaseException {
    if ( data.db != null ) {
      data.db.closeQuery( data.rs );
    }
  }

  private boolean doQuery( IRowMeta parametersMeta, Object[] parameters ) throws HopDatabaseException {
    boolean success = true;

    // Open the query with the optional parameters received from the source transforms.
    String sql = null;
    if ( meta.isVariableReplacementActive() ) {
      sql = resolve( meta.getSql() );
    } else {
      sql = meta.getSql();
    }

    if ( log.isDetailed() ) {
      logDetailed( "SQL query : " + sql );
    }
    if ( parametersMeta.isEmpty() ) {
      data.rs = data.db.openQuery( sql, null, null, ResultSet.FETCH_FORWARD, false );
    } else {
      data.rs = data.db.openQuery( sql, parametersMeta, parameters, ResultSet.FETCH_FORWARD, false );
    }
    if ( data.rs == null ) {
      logError( "Couldn't open Query [" + sql + "]" );
      setErrors( 1 );
      stopAll();
      success = false;
    } else {
      // Keep the metadata
      data.rowMeta = data.db.getReturnRowMeta();

      // Set the origin on the row metadata...
      if ( data.rowMeta != null ) {
        for ( IValueMeta valueMeta : data.rowMeta.getValueMetaList() ) {
          valueMeta.setOrigin( getTransformName() );
        }
      }

      // Get the first row...
      data.thisrow = data.db.getRow( data.rs );
      if ( data.thisrow != null ) {
        incrementLinesInput();
        data.nextrow = data.db.getRow( data.rs );
        if ( data.nextrow != null ) {
          incrementLinesInput();
        }
      }
    }
    return success;
  }

  public void dispose() {
    if ( log.isBasic() ) {
      logBasic( "Finished reading query, closing connection." );
    }
    try {
      closePreviousQuery();
    } catch ( HopException e ) {
      logError( "Unexpected error closing query : " + e.toString() );
      setErrors( 1 );
      stopAll();
    } finally {
      if ( data.db != null ) {
        data.db.disconnect();
      }
    }

    super.dispose();
  }

  /**
   * Stop the running query
   */
  public synchronized void stopRunning()throws HopException {
    if ( this.isStopped() || data.isDisposed() ) {
      return;
    }

    setStopped( true );

    if ( data.db != null && data.db.getConnection() != null && !data.isCanceled ) {
      data.db.cancelQuery();
      data.isCanceled = true;
    }
  }

  @Override
  public boolean init() {

    if ( super.init() ) {
      // Verify some basic things first...
      //
      boolean passed = true;
      if ( Utils.isEmpty( meta.getSql() ) ) {
        logError( BaseMessages.getString( PKG, "TableInput.Exception.SQLIsNeeded" ) );
        passed = false;
      }

      if ( meta.getDatabaseMeta() == null ) {
        logError( BaseMessages.getString( PKG, "TableInput.Exception.DatabaseConnectionsIsNeeded" ) );
        passed = false;
      }
      if ( !passed ) {
        return false;
      }

      data.infoStream = meta.getTransformIOMeta().getInfoStreams().get( 0 );
      if ( meta.getDatabaseMeta() == null ) {
        logError( BaseMessages.getString( PKG, "TableInput.Init.ConnectionMissing", getTransformName() ) );
        return false;
      }
      data.db = new Database( this, this, meta.getDatabaseMeta() );
      data.db.setQueryLimit( Const.toInt( resolve( meta.getRowLimit() ), 0 ) );

      try {
        data.db.connect( getPartitionId() );

        if ( meta.getDatabaseMeta().isRequiringTransactionsOnQueries() ) {
          data.db.setCommit( 100 ); // needed for PGSQL it seems...
        }
        if ( log.isDetailed() ) {
          logDetailed( "Connected to database..." );
        }

        return true;
      } catch ( HopException e ) {
        logError( "An error occurred, processing will be stopped: " + e.getMessage() );
        setErrors( 1 );
        stopAll();
      }
    }

    return false;
  }

  public boolean isWaitingForData() {
    return true;
  }

}
