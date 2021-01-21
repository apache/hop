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

package org.apache.hop.pipeline.transforms.databasejoin;

import org.apache.hop.core.database.Database;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.sql.ResultSet;

/**
 * Use values from input streams to joins with values in a database. Freehand SQL can be used to do this.
 *
 * @author Matt
 * @since 26-apr-2003
 */
public class DatabaseJoin extends BaseTransform<DatabaseJoinMeta, DatabaseJoinData> implements ITransform<DatabaseJoinMeta, DatabaseJoinData> {

  private static final Class<?> PKG = DatabaseJoinMeta.class; // For Translator

  public DatabaseJoin( TransformMeta transformMeta, DatabaseJoinMeta meta, DatabaseJoinData data, int copyNr, PipelineMeta pipelineMeta, Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  private synchronized void lookupValues( IRowMeta rowMeta, Object[] rowData ) throws HopException {
    if ( first ) {
      first = false;

      data.outputRowMeta = rowMeta.clone();
      meta.getFields(
        data.outputRowMeta, getTransformName(), new IRowMeta[] { meta.getTableFields(this), }, null, this, metadataProvider );

      data.lookupRowMeta = new RowMeta();

      if ( log.isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "DatabaseJoin.Log.CheckingRow" ) + rowMeta.getString( rowData ) );
      }

      data.keynrs = new int[ meta.getParameterField().length ];

      for ( int i = 0; i < meta.getParameterField().length; i++ ) {
        data.keynrs[ i ] = rowMeta.indexOfValue( meta.getParameterField()[ i ] );
        if ( data.keynrs[ i ] < 0 ) {
          throw new HopTransformException( BaseMessages.getString( PKG, "DatabaseJoin.Exception.FieldNotFound", meta
            .getParameterField()[ i ] ) );
        }

        data.lookupRowMeta.addValueMeta( rowMeta.getValueMeta( data.keynrs[ i ] ).clone() );
      }
    }

    // Construct the parameters row...
    Object[] lookupRowData = new Object[ data.lookupRowMeta.size() ];
    for ( int i = 0; i < data.keynrs.length; i++ ) {
      lookupRowData[ i ] = rowData[ data.keynrs[ i ] ];
    }

    // Set the values on the prepared statement (for faster exec.)
    ResultSet rs = data.db.openQuery( data.pstmt, data.lookupRowMeta, lookupRowData );

    // Get a row from the database...
    //
    Object[] add = data.db.getRow( rs );
    IRowMeta addMeta = data.db.getReturnRowMeta();

    incrementLinesInput();

    int counter = 0;
    while ( add != null && ( meta.getRowLimit() == 0 || counter < meta.getRowLimit() ) ) {
      counter++;

      Object[] newRow = RowDataUtil.resizeArray( rowData, data.outputRowMeta.size() );
      int newIndex = rowMeta.size();
      for ( int i = 0; i < addMeta.size(); i++ ) {
        newRow[ newIndex++ ] = add[ i ];
      }
      // we have to clone, otherwise we only get the last new value
      putRow( data.outputRowMeta, data.outputRowMeta.cloneRow( newRow ) );

      if ( log.isRowLevel() ) {
        logRowlevel( BaseMessages.getString( PKG, "DatabaseJoin.Log.PutoutRow" )
          + data.outputRowMeta.getString( newRow ) );
      }

      // Get a new row
      if ( meta.getRowLimit() == 0 || counter < meta.getRowLimit() ) {
        add = data.db.getRow( rs );
        incrementLinesInput();
      }
    }

    // Nothing found? Perhaps we have to put something out after all?
    if ( counter == 0 && meta.isOuterJoin() ) {
      if ( data.notfound == null ) {
        // Just return null values for all values...
        //
        data.notfound = new Object[ data.db.getReturnRowMeta().size() ];
      }
      Object[] newRow = RowDataUtil.resizeArray( rowData, data.outputRowMeta.size() );
      int newIndex = rowMeta.size();
      for ( int i = 0; i < data.notfound.length; i++ ) {
        newRow[ newIndex++ ] = data.notfound[ i ];
      }
      putRow( data.outputRowMeta, newRow );
    }

    data.db.closeQuery( rs );
  }

  public boolean processRow() throws HopException {

    boolean sendToErrorRow = false;
    String errorMessage = null;

    Object[] r = getRow(); // Get row from input rowset & set row busy!
    if ( r == null ) { // no more input to be expected...
      setOutputDone();
      return false;
    }

    try {
      lookupValues( getInputRowMeta(), r ); // add new values to the row in rowset[0].

      if ( checkFeedback( getLinesRead() ) ) {
        if ( log.isBasic() ) {
          logBasic( BaseMessages.getString( PKG, "DatabaseJoin.Log.LineNumber" ) + getLinesRead() );
        }
      }
    } catch ( HopException e ) {

      if ( getTransformMeta().isDoingErrorHandling() ) {
        sendToErrorRow = true;
        errorMessage = e.toString();
      } else {

        logError( BaseMessages.getString( PKG, "DatabaseJoin.Log.ErrorInTransformRunning" ) + e.getMessage(), e );
        setErrors( 1 );
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }
      if ( sendToErrorRow ) {
        // Simply add this row to the error row
        putError( getInputRowMeta(), r, 1, errorMessage, null, "DBJOIN001" );
      }
    }

    return true;
  }

  /**
   * Stop the running query
   * [PDI-17820] - In the Database Join transform data.isCancelled is checked before synchronization and set after synchronization is completed.
   * <p>
   * To cancel a prepared statement we need a valid database connection which we do not have if disposed has already been called
   */
  public synchronized void stopRunning() throws HopException {
    if ( this.isStopped() || data.isDisposed() ) {
      return;
    }

    if ( data.db != null && data.db.getConnection() != null && !data.isCanceled ) {
      data.db.cancelStatement( data.pstmt );
      setStopped( true );
      data.isCanceled = true;
    }
  }

  public boolean init(){
    if ( super.init() ) {
      if ( meta.getDatabaseMeta() == null ) {
        logError( BaseMessages.getString( PKG, "DatabaseJoin.Init.ConnectionMissing", getTransformName() ) );
        return false;
      }
      data.db = new Database( this, this, meta.getDatabaseMeta() );

      try {
        data.db.connect( getPartitionId() );

        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "DatabaseJoin.Log.ConnectedToDB" ) );
        }

        String sql = meta.getSql();
        if ( meta.isVariableReplace() ) {
          sql = resolve( sql );
        }
        // Prepare the SQL statement
        data.pstmt = data.db.prepareSql( sql );
        if ( log.isDebug() ) {
          logDebug( BaseMessages.getString( PKG, "DatabaseJoin.Log.SQLStatement", sql ) );
        }
        data.db.setQueryLimit( meta.getRowLimit() );

        return true;
      } catch ( HopException e ) {
        logError( BaseMessages.getString( PKG, "DatabaseJoin.Log.DatabaseError" ) + e.getMessage(), e );
        if ( data.db != null ) {
          data.db.disconnect();
        }
      }
    }

    return false;
  }

  public void dispose(){
    if ( data.db != null ) {
      data.db.disconnect();
    }

    super.dispose();
  }
}
