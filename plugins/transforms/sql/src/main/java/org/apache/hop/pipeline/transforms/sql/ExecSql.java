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

package org.apache.hop.pipeline.transforms.sql;

import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.ArrayList;

/**
 * Execute one or more SQL statements in a script, one time or parameterised (for every row)
 *
 * @author Matt
 * @since 10-sep-2005
 */
public class ExecSql extends BaseTransform<ExecSqlMeta, ExecSqlData> implements ITransform<ExecSqlMeta, ExecSqlData> {

  private static final Class<?> PKG = ExecSqlMeta.class; // For Translator

  public ExecSql( TransformMeta transformMeta, ExecSqlMeta meta, ExecSqlData data, int copyNr, PipelineMeta pipelineMeta,
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

    if ( !meta.isExecutedEachInputRow() ) {
      RowMetaAndData resultRow =
        getResultRow( data.result, meta.getUpdateField(), meta.getInsertField(), meta.getDeleteField(), meta
          .getReadField() );
      putRow( resultRow.getRowMeta(), resultRow.getData() );
      setOutputDone(); // Stop processing, this is all we do!
      return false;
    }

    Object[] row = getRow();
    if ( row == null ) { // no more input to be expected...

      setOutputDone();
      return false;
    }

    if ( first ) { // we just got started

      first = false;

      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );

      // Find the indexes of the arguments
      data.argumentIndexes = new int[ meta.getArguments().length ];
      for ( int i = 0; i < meta.getArguments().length; i++ ) {
        data.argumentIndexes[ i ] = this.getInputRowMeta().indexOfValue( meta.getArguments()[ i ] );
        if ( data.argumentIndexes[ i ] < 0 ) {
          logError( BaseMessages.getString( PKG, "ExecSql.Log.ErrorFindingField" ) + meta.getArguments()[ i ] + "]" );
          throw new HopTransformException( BaseMessages.getString( PKG, "ExecSql.Exception.CouldNotFindField", meta
            .getArguments()[ i ] ) );
        }
        if ( meta.isParams() ) {
          if ( i == 0 ) {
            // Define parameters meta
            data.paramsMeta = new RowMeta();
          }
          data.paramsMeta.addValueMeta( getInputRowMeta().getValueMeta( data.argumentIndexes[ i ] ) );
        }
      }

      if ( !meta.isParams() ) {
        // We need to replace question marks by string value

        // Find the locations of the question marks in the String...
        // We replace the question marks with the values...
        // We ignore quotes etc. to make inserts easier...
        data.markerPositions = new ArrayList<>();
        int len = data.sql.length();
        int pos = len - 1;
        while ( pos >= 0 ) {
          if ( data.sql.charAt( pos ) == '?' ) {
            data.markerPositions.add( Integer.valueOf( pos ) ); // save the
          }
          // marker
          // position
          pos--;
        }
      }
    }

    String sql;
    Object[] paramsData = null;
    if ( meta.isParams() ) {
      // Get parameters data
      paramsData = new Object[ data.argumentIndexes.length ];
      sql = this.data.sql;
      for ( int i = 0; i < this.data.argumentIndexes.length; i++ ) {
        paramsData[ i ] = row[ data.argumentIndexes[ i ] ];
      }
    } else {
      int numMarkers = data.markerPositions.size();
      if ( numMarkers > 0 ) {
        StringBuilder buf = new StringBuilder( data.sql );

        // Replace the values in the SQL string...
        //
        for ( int i = 0; i < numMarkers; i++ ) {
          // Get the appropriate value from the input row...
          //
          int index = data.argumentIndexes[ data.markerPositions.size() - i - 1 ];
          IValueMeta valueMeta = getInputRowMeta().getValueMeta( index );
          Object valueData = row[ index ];

          // replace the '?' with the String in the row.
          //
          int pos = data.markerPositions.get( i );
          String replaceValue = valueMeta.getString( valueData );
          replaceValue = Const.NVL( replaceValue, "" );
          if ( meta.isQuoteString() && ( valueMeta.getType() == IValueMeta.TYPE_STRING ) ) {
            // Have the database dialect do the quoting.
            // This also adds the quotes around the string
            replaceValue = meta.getDatabaseMeta().quoteSqlString( replaceValue );
          }
          buf.replace( pos, pos + 1, replaceValue );
        }
        sql = buf.toString();
      } else {
        sql = data.sql;
      }
    }
    if ( log.isRowLevel() ) {
      logRowlevel( BaseMessages.getString( PKG, "ExecSql.Log.ExecutingSQLScript" ) + Const.CR + sql );
    }

    boolean sendToErrorRow = false;
    String errorMessage = null;
    try {

      if ( meta.isSingleStatement() ) {
        data.result = data.db.execStatement( sql, data.paramsMeta, paramsData );
      } else {
        data.result = data.db.execStatements( sql, data.paramsMeta, paramsData );
      }

      RowMetaAndData add =
        getResultRow( data.result, meta.getUpdateField(), meta.getInsertField(), meta.getDeleteField(), meta
          .getReadField() );

      row = RowDataUtil.addRowData( row, getInputRowMeta().size(), add.getData() );

      if ( !data.db.isAutoCommit() ) {
        data.db.commit();
      }

      putRow( data.outputRowMeta, row ); // send it out!

      if ( checkFeedback( getLinesWritten() ) ) {
        if ( log.isBasic() ) {
          logBasic( BaseMessages.getString( PKG, "ExecSql.Log.LineNumber" ) + getLinesWritten() );
        }
      }
    } catch ( HopException e ) {
      if ( getTransformMeta().isDoingErrorHandling() ) {
        sendToErrorRow = true;
        errorMessage = e.toString();
      } else {
        throw new HopTransformException( BaseMessages.getString( PKG, "ExecSql.Log.ErrorInTransform" ), e );
      }

      if ( sendToErrorRow ) {
        // Simply add this row to the error row
        putError( getInputRowMeta(), row, 1, errorMessage, null, "ExecSql001" );
      }
    }
    return true;
  }

  @Override
  public void dispose(){

    if ( log.isBasic() ) {
      logBasic( BaseMessages.getString( PKG, "ExecSql.Log.FinishingReadingQuery" ) );
    }

    if ( data.db != null ) {
      data.db.disconnect();
    }

    super.dispose();
  }

  /**
   * Stop the running query
   */
  @Override
  public void stopRunning()throws HopException {

    if ( data.db != null && !data.isCanceled ) {
      synchronized ( data.db ) {
        data.db.cancelQuery();
      }
      data.isCanceled = true;
    }
  }

  @Override
  public boolean init(){

    if ( super.init() ) {
      if ( meta.getDatabaseMeta() == null ) {
        logError( BaseMessages.getString( PKG, "ExecSql.Init.ConnectionMissing", getTransformName() ) );
        return false;
      }
      data.db = new Database( this, this, meta.getDatabaseMeta() );

      // Connect to the database
      try {
        data.db.connect( getPartitionId() );

        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "ExecSql.Log.ConnectedToDB" ) );
        }

        if ( meta.isReplaceVariables() ) {
          data.sql = resolve( meta.getSql() );
        } else {
          data.sql = meta.getSql();
        }
        // If the SQL needs to be executed once, this is a starting transform
        // somewhere.
        if ( !meta.isExecutedEachInputRow() ) {
          if ( meta.isSingleStatement() ) {
            data.result = data.db.execStatement( data.sql );
          } else {
            data.result = data.db.execStatements( data.sql );
          }
          if ( !data.db.isAutoCommit() ) {
            data.db.commit();
          }
        }
        return true;
      } catch ( HopException e ) {
        logError( BaseMessages.getString( PKG, "ExecSql.Log.ErrorOccurred" ) + e.getMessage() );
        setErrors( 1 );
        stopAll();
      }
    }

    return false;
  }

}
