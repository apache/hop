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

package org.apache.hop.pipeline.transforms.dbproc;

import org.apache.hop.core.Const;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.sql.SQLException;
import java.util.List;

/**
 * Retrieves values from a database by calling database stored procedures or functions
 *
 * @author Matt
 * @since 26-apr-2003
 */
public class DBProc extends BaseTransform<DBProcMeta, DBProcData> implements ITransform<DBProcMeta, DBProcData> {

  private static final Class<?> PKG = DBProcMeta.class; // For Translator

  public DBProc( TransformMeta transformMeta, DBProcMeta meta, DBProcData data, int copyNr, PipelineMeta pipelineMeta,
                 Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  private Object[] runProc( IRowMeta rowMeta, Object[] rowData ) throws HopException {
    if ( first ) {
      first = false;

      // get the RowMeta for the output
      //
      data.outputMeta = data.inputRowMeta.clone();
      meta.getFields( data.outputMeta, getTransformName(), null, null, this, metadataProvider );

      data.argnrs = new int[ meta.getArgument().length ];
      for ( int i = 0; i < meta.getArgument().length; i++ ) {
        if ( !meta.getArgumentDirection()[ i ].equalsIgnoreCase( "OUT" ) ) { // IN or INOUT
          data.argnrs[ i ] = rowMeta.indexOfValue( meta.getArgument()[ i ] );
          if ( data.argnrs[ i ] < 0 ) {
            logError( BaseMessages.getString( PKG, "DBProc.Log.ErrorFindingField" ) + meta.getArgument()[ i ] + "]" );
            throw new HopTransformException( BaseMessages.getString( PKG, "DBProc.Exception.CouldnotFindField", meta
              .getArgument()[ i ] ) );
          }
        } else {
          data.argnrs[ i ] = -1;
        }
      }

      data.db.setProcLookup( resolve( meta.getProcedure() ), meta.getArgument(), meta
        .getArgumentDirection(), meta.getArgumentType(), meta.getResultName(), meta.getResultType() );
    }

    Object[] outputRowData = RowDataUtil.resizeArray( rowData, data.outputMeta.size() );
    int outputIndex = rowMeta.size();

    data.db.setProcValues( rowMeta, rowData, data.argnrs, meta.getArgumentDirection(), !Utils.isEmpty( meta
      .getResultName() ) );

    RowMetaAndData add =
      data.db.callProcedure( meta.getArgument(), meta.getArgumentDirection(), meta.getArgumentType(), meta
        .getResultName(), meta.getResultType() );
    int addIndex = 0;

    // Function return?
    if ( !Utils.isEmpty( meta.getResultName() ) ) {
      outputRowData[ outputIndex++ ] = add.getData()[ addIndex++ ]; // first is the function return
    }

    // We are only expecting the OUT and INOUT arguments here.
    // The INOUT values need to replace the value with the same name in the row.
    //
    for ( int i = 0; i < data.argnrs.length; i++ ) {
      if ( meta.getArgumentDirection()[ i ].equalsIgnoreCase( "OUT" ) ) {
        // add
        outputRowData[ outputIndex++ ] = add.getData()[ addIndex++ ];
      } else if ( meta.getArgumentDirection()[ i ].equalsIgnoreCase( "INOUT" ) ) {
        // replace
        outputRowData[ data.argnrs[ i ] ] = add.getData()[ addIndex ];
        addIndex++;
      }
      // IN not taken
    }
    return outputRowData;
  }

  public boolean processRow() throws HopException {
    boolean sendToErrorRow = false;
    String errorMessage = null;

    // A procedure/function could also have no input at all
    // However, we would still need to know how many times it gets executed.
    // In short: the procedure gets executed once for every input row.
    //
    Object[] r;

    if ( data.readsRows ) {
      r = getRow(); // Get row from input rowset & set row busy!
      if ( r == null ) { // no more input to be expected...

        setOutputDone();
        return false;
      }
      data.inputRowMeta = getInputRowMeta();
    } else {
      r = new Object[] {}; // empty row
      incrementLinesRead();
      data.inputRowMeta = new RowMeta(); // empty row metadata too
      data.readsRows = true; // make it drop out of the loop at the next entrance to this method
    }

    try {
      Object[] outputRowData = runProc( data.inputRowMeta, r ); // add new values to the row in rowset[0].
      putRow( data.outputMeta, outputRowData ); // copy row to output rowset(s);

      if ( checkFeedback( getLinesRead() ) ) {
        if ( log.isBasic() ) {
          logBasic( BaseMessages.getString( PKG, "DBProc.LineNumber" ) + getLinesRead() );
        }
      }
    } catch ( HopException e ) {

      if ( getTransformMeta().isDoingErrorHandling() ) {
        sendToErrorRow = true;
        errorMessage = e.toString();
        // CHE: Read the chained SQL exceptions and add them
        // to the errorMessage
        SQLException nextSqlExOnChain = null;
        if ( ( e.getCause() != null ) && ( e.getCause() instanceof SQLException ) ) {
          nextSqlExOnChain = ( (SQLException) e.getCause() ).getNextException();
          while ( nextSqlExOnChain != null ) {
            errorMessage = errorMessage + nextSqlExOnChain.getMessage() + Const.CR;
            nextSqlExOnChain = nextSqlExOnChain.getNextException();
          }
        }
      } else {

        logError( BaseMessages.getString( PKG, "DBProc.ErrorInTransformRunning" ) + e.getMessage() );
        setErrors( 1 );
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }

      if ( sendToErrorRow ) {
        // Simply add this row to the error row
        putError( getInputRowMeta(), r, 1, errorMessage, null, "DBP001" );
      }
    }

    return true;
  }

  public boolean init() {

    if ( super.init() ) {
      //      data.readsRows = getTransformMeta().getRemoteInputTransforms().size() > 0;
      List<TransformMeta> previous = getPipelineMeta().findPreviousTransforms( getTransformMeta() );
      if ( previous != null && previous.size() > 0 ) {
        data.readsRows = true;
      }

      data.db = new Database( this, this, meta.getDatabase() );
      try {
        data.db.connect( getPartitionId() );

        if ( !meta.isAutoCommit() ) {
          if ( log.isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "DBProc.Log.AutoCommit" ) );
          }
          data.db.setCommit( 9999 );
        }
        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "DBProc.Log.ConnectedToDB" ) );
        }

        return true;
      } catch ( HopException e ) {
        logError( BaseMessages.getString( PKG, "DBProc.Log.DBException" ) + e.getMessage() );
        if ( data.db != null ) {
          data.db.disconnect();
        }
      }
    }
    return false;
  }

  public void dispose() {

    if ( data.db != null ) {
      // CHE: Properly close the callable statement
      try {
        data.db.closeProcedureStatement();
      } catch ( HopDatabaseException e ) {
        logError( BaseMessages.getString( PKG, "DBProc.Log.CloseProcedureError" ) + e.getMessage() );
      }

      try {
        if ( !meta.isAutoCommit() ) {
          data.db.commit();
        }
      } catch ( HopDatabaseException e ) {
        logError( BaseMessages.getString( PKG, "DBProc.Log.CommitError" ) + e.getMessage() );
      } finally {
        data.db.disconnect();
      }
    }
    super.dispose();
  }

}
