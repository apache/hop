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

package org.apache.hop.pipeline.transforms.execsqlrow;

import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Execute one or more SQL statements in a script, one time or parameterised (for every row)
 *
 * @author Matt
 * @since 10-sep-2005
 */
public class ExecSqlRow extends BaseTransform<ExecSqlRowMeta, ExecSqlRowData> implements ITransform<ExecSqlRowMeta, ExecSqlRowData> {

  private static final Class<?> PKG = ExecSqlRowMeta.class; // For Translator

  public ExecSqlRow( TransformMeta transformMeta, ExecSqlRowMeta meta, ExecSqlRowData data, int copyNr, PipelineMeta pipelineMeta,
                     Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  public static final RowMetaAndData getResultRow( Result result, String upd, String ins, String del, String read ) {
    RowMetaAndData resultRow = new RowMetaAndData();

    if ( upd != null && upd.length() > 0 ) {
      IValueMeta meta = new ValueMetaInteger( upd );
      meta.setLength( IValueMeta.DEFAULT_INTEGER_LENGTH, 0 );
      resultRow.addValue( meta, new Long( result.getNrLinesUpdated() ) );
    }

    if ( ins != null && ins.length() > 0 ) {
      IValueMeta meta = new ValueMetaInteger( ins );
      meta.setLength( IValueMeta.DEFAULT_INTEGER_LENGTH, 0 );
      resultRow.addValue( meta, new Long( result.getNrLinesOutput() ) );
    }

    if ( del != null && del.length() > 0 ) {
      IValueMeta meta = new ValueMetaInteger( del );
      meta.setLength( IValueMeta.DEFAULT_INTEGER_LENGTH, 0 );
      resultRow.addValue( meta, new Long( result.getNrLinesDeleted() ) );
    }

    if ( read != null && read.length() > 0 ) {
      IValueMeta meta = new ValueMetaInteger( read );
      meta.setLength( IValueMeta.DEFAULT_INTEGER_LENGTH, 0 );
      resultRow.addValue( meta, new Long( result.getNrLinesRead() ) );
    }

    return resultRow;
  }

  @Override
  public boolean processRow() throws HopException {

    boolean sendToErrorRow = false;
    String errorMessage = null;

    Object[] row = getRow();
    if ( row == null ) { // no more input to be expected...

      setOutputDone();
      return false;
    }

    if ( first ) { // we just got started

      first = false;

      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );

      // Check is SQL field is provided
      if ( Utils.isEmpty( meta.getSqlFieldName() ) ) {
        throw new HopException( BaseMessages.getString( PKG, "ExecSqlRow.Error.SQLFieldFieldMissing" ) );
      }

      // cache the position of the field
      if ( data.indexOfSqlFieldname < 0 ) {
        data.indexOfSqlFieldname = this.getInputRowMeta().indexOfValue( meta.getSqlFieldName() );
        if ( data.indexOfSqlFieldname < 0 ) {
          // The field is unreachable !
          throw new HopException( BaseMessages.getString( PKG, "ExecSqlRow.Exception.CouldnotFindField", meta
            .getSqlFieldName() ) );
        }
      }

    }

    // get SQL
    String sql = getInputRowMeta().getString( row, data.indexOfSqlFieldname);

    try {
      if ( meta.isSqlFromfile() ) {
        if ( Utils.isEmpty( sql ) ) {
          // empty filename
          throw new HopException( BaseMessages.getString( PKG, "ExecSqlRow.Log.EmptySQLFromFile" ) );
        }
        if ( log.isDebug() ) {
          logDebug( BaseMessages.getString( PKG, "ExecSqlRow.Log.ExecutingSQLFromFile", sql ) );
        }
        data.result = data.db.execStatementsFromFile( sql, meta.IsSendOneStatement() );
      } else {
        if ( log.isDebug() ) {
          logDebug( BaseMessages.getString( PKG, "ExecSqlRow.Log.ExecutingSQLScript" ) + Const.CR + sql );
        }
        if ( meta.IsSendOneStatement() ) {
          data.result = data.db.execStatement( sql );
        } else {
          data.result = data.db.execStatements( sql );
        }
      }

      RowMetaAndData add =
        getResultRow( data.result, meta.getUpdateField(), meta.getInsertField(), meta.getDeleteField(), meta
          .getReadField() );
      row = RowDataUtil.addRowData( row, getInputRowMeta().size(), add.getData() );

      if ( meta.getCommitSize() > 0 ) {
        if ( !data.db.isAutoCommit() ) {
          if ( meta.getCommitSize() == 1 ) {
            data.db.commit();
          } else if ( getLinesWritten() % meta.getCommitSize() == 0 ) {
            data.db.commit();
          }
        }
      }

      putRow( data.outputRowMeta, row ); // send it out!

      if ( checkFeedback( getLinesWritten() ) ) {
        if ( log.isBasic() ) {
          logBasic( BaseMessages.getString( PKG, "ExecSqlRow.Log.LineNumber" ) + getLinesWritten() );
        }
      }
    } catch ( HopException e ) {
      if ( getTransformMeta().isDoingErrorHandling() ) {
        sendToErrorRow = true;
        errorMessage = e.toString();
      } else {
        logError( BaseMessages.getString( PKG, "ExecSqlRow.Log.ErrorInTransform" ) + e.getMessage() );
        setErrors( 1 );
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }
      if ( sendToErrorRow ) {
        // Simply add this row to the error row
        putError( getInputRowMeta(), row, 1, errorMessage, null, "ExecSqlRow001" );
      }
    }
    return true;
  }

  @Override
  public void dispose(){

    if ( log.isBasic() ) {
      logBasic( BaseMessages.getString( PKG, "ExecSqlRow.Log.FinishingReadingQuery" ) );
    }

    if ( data.db != null ) {
      try {
        if ( !data.db.isAutoCommit() ) {
          if ( getErrors() == 0 ) {
            data.db.commit();
          } else {
            data.db.rollback();
          }
        }
      } catch ( HopDatabaseException e ) {
        logError( BaseMessages.getString( PKG, "Update.Log.UnableToCommitUpdateConnection" )
          + data.db + "] :" + e.toString() );
        setErrors( 1 );
      } finally {
        data.db.disconnect();
      }
    }

    super.dispose();
  }

  /**
   * Stop the running query
   */
  @Override
  public void stopRunning()throws HopException {
    if ( data.db != null ) {
      data.db.cancelQuery();
    }
  }

  @Override
  public boolean init(){
    if ( super.init() ) {
      if ( meta.getDatabaseMeta() == null ) {
        logError( BaseMessages.getString( PKG, "ExecSqlRow.Init.ConnectionMissing", getTransformName() ) );
        return false;
      }
      data.db = new Database( this, this, meta.getDatabaseMeta() );

      // Connect to the database
      try {
        data.db.connect( getPartitionId() );

        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "ExecSqlRow.Log.ConnectedToDB" ) );
        }

        if ( meta.getCommitSize() >= 1 ) {
          data.db.setCommit( meta.getCommitSize() );
        }
        return true;
      } catch ( HopException e ) {
        logError( BaseMessages.getString( PKG, "ExecSqlRow.Log.ErrorOccurred" ) + e.getMessage() );
        setErrors( 1 );
        stopAll();
      }
    }

    return false;
  }

}
