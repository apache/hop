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

package org.apache.hop.pipeline.transforms.dynamicsqlrow;

import org.apache.hop.core.Const;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.sql.ResultSet;

/**
 * Run dynamic SQL. SQL is defined in a field.
 *
 * @author Samatar
 * @since 13-10-2008
 */
public class DynamicSqlRow extends BaseTransform<DynamicSqlRowMeta, DynamicSqlRowData> implements ITransform<DynamicSqlRowMeta, DynamicSqlRowData> {

  private static final Class<?> PKG = DynamicSqlRowMeta.class; // For Translator

  public DynamicSqlRow( TransformMeta transformMeta, DynamicSqlRowMeta meta, DynamicSqlRowData data, int copyNr, PipelineMeta pipelineMeta,
                        Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  private synchronized void lookupValues( IRowMeta rowMeta, Object[] rowData ) throws HopException {
    boolean loadFromBuffer = true;
    if ( first ) {
      first = false;
      data.outputRowMeta = rowMeta.clone();
      meta.getFields(
        data.outputRowMeta, getTransformName(), new IRowMeta[] { meta.getTableFields(this), }, null, this, metadataProvider );

      loadFromBuffer = false;
    }

    if ( log.isDetailed() ) {
      logDetailed( BaseMessages.getString( PKG, "DynamicSQLRow.Log.CheckingRow" ) + rowMeta.getString( rowData ) );
    }

    // get dynamic SQL statement
    String sqlTemp = getInputRowMeta().getString( rowData, data.indexOfSqlField);
    String sql = null;
    if ( meta.isVariableReplace() ) {
      sql = resolve( sqlTemp );
    } else {
      sql = sqlTemp;
    }

    if ( log.isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "DynamicSQLRow.Log.SQLStatement", sql ) );
    }

    if ( meta.isQueryOnlyOnChange() ) {
      if ( loadFromBuffer ) {
        if ( data.previousSql != null && !data.previousSql.equals( sql ) ) {
          loadFromBuffer = false;
        }
      }
      // Save current parameters value as previous ones
      data.previousSql = sql;
    } else {
      loadFromBuffer = false;
    }

    if ( loadFromBuffer ) {
      incrementLinesInput();

      if ( !data.skipPreviousRow ) {
        Object[] newRow = RowDataUtil.resizeArray( rowData, data.outputRowMeta.size() );
        int newIndex = rowMeta.size();
        IRowMeta addMeta = data.db.getReturnRowMeta();

        // read from Buffer
        for ( int p = 0; p < data.previousrowbuffer.size(); p++ ) {
          Object[] getBufferRow = data.previousrowbuffer.get( p );
          for ( int i = 0; i < addMeta.size(); i++ ) {
            newRow[ newIndex++ ] = getBufferRow[ i ];
          }
          putRow( data.outputRowMeta, data.outputRowMeta.cloneRow( newRow ) );
        }
      }
    } else {
      if ( meta.isQueryOnlyOnChange() ) {
        data.previousrowbuffer.clear();
      }

      // Set the values on the prepared statement (for faster exec.)
      ResultSet rs = data.db.openQuery( sql );

      // Get a row from the database...
      Object[] add = data.db.getRow( rs );
      IRowMeta addMeta = data.db.getReturnRowMeta();

      // Also validate the data types to make sure we've not place an incorrect template in the dialog...
      //
      if ( add != null ) {
        int nrTemplateFields = data.outputRowMeta.size() - getInputRowMeta().size();
        if ( addMeta.size() != nrTemplateFields ) {
          throw new HopException( BaseMessages.getString(
            PKG, "DynamicSQLRow.Exception.IncorrectNrTemplateFields", nrTemplateFields, addMeta.size(), sql ) );
        }
        StringBuilder typeErrors = new StringBuilder();
        for ( int i = 0; i < addMeta.size(); i++ ) {
          IValueMeta templateValueMeta = addMeta.getValueMeta( i );
          IValueMeta outputValueMeta = data.outputRowMeta.getValueMeta( getInputRowMeta().size() + i );

          if ( templateValueMeta.getType() != outputValueMeta.getType() ) {
            if ( typeErrors.length() > 0 ) {
              typeErrors.append( Const.CR );
            }
            typeErrors.append( BaseMessages.getString(
              PKG, "DynamicSQLRow.Exception.TemplateReturnDataTypeError", templateValueMeta.toString(),
              outputValueMeta.toString() ) );
          }
        }
        if ( typeErrors.length() > 0 ) {
          throw new HopException( typeErrors.toString() );
        }
      }

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

        if ( meta.isQueryOnlyOnChange() ) {
          // add row to the previous rows buffer
          data.previousrowbuffer.add( add );
          data.skipPreviousRow = false;
        }

        if ( log.isRowLevel() ) {
          logRowlevel( BaseMessages.getString( PKG, "DynamicSQLRow.Log.PutoutRow" )
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
          data.notfound = new Object[ data.db.getReturnRowMeta().size() ];
        }
        Object[] newRow = RowDataUtil.resizeArray( rowData, data.outputRowMeta.size() );
        int newIndex = rowMeta.size();
        for ( int i = 0; i < data.notfound.length; i++ ) {
          newRow[ newIndex++ ] = data.notfound[ i ];
        }
        putRow( data.outputRowMeta, newRow );

        if ( meta.isQueryOnlyOnChange() ) {
          // add row to the previous rows buffer
          data.previousrowbuffer.add( data.notfound );
          data.skipPreviousRow = false;
        }
      } else {
        if ( meta.isQueryOnlyOnChange() && counter == 0 && !meta.isOuterJoin() ) {
          data.skipPreviousRow = true;
        }
      }

      if ( data.db != null ) {
        data.db.closeQuery( rs );
      }
    }

  }

  public boolean processRow() throws HopException {

    Object[] r = getRow(); // Get row from input rowset & set row busy!
    if ( r == null ) { // no more input to be expected...
      setOutputDone();
      return false;
    }
    if ( first ) {
      if ( Utils.isEmpty( meta.getSqlFieldName() ) ) {
        throw new HopException( BaseMessages.getString( PKG, "DynamicSQLRow.Exception.SQLFieldNameEmpty" ) );
      }

      if ( Utils.isEmpty( meta.getSql() ) ) {
        throw new HopException( BaseMessages.getString( PKG, "DynamicSQLRow.Exception.SQLEmpty" ) );
      }

      // cache the position of the field
      if ( data.indexOfSqlField < 0 ) {
        data.indexOfSqlField = getInputRowMeta().indexOfValue( meta.getSqlFieldName() );
        if ( data.indexOfSqlField < 0 ) {
          // The field is unreachable !
          throw new HopException( BaseMessages.getString( PKG, "DynamicSQLRow.Exception.FieldNotFound", meta
            .getSqlFieldName() ) );
        }
      }
    }
    try {
      lookupValues( getInputRowMeta(), r );

      if ( checkFeedback( getLinesRead() ) ) {
        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "DynamicSQLRow.Log.LineNumber" ) + getLinesRead() );
        }
      }
    } catch ( HopException e ) {
      boolean sendToErrorRow = false;
      String errorMessage = null;

      if ( getTransformMeta().isDoingErrorHandling() ) {
        sendToErrorRow = true;
        errorMessage = e.toString();
      } else {
        logError( BaseMessages.getString( PKG, "DynamicSQLRow.Log.ErrorInTransformRunning" ) + e.getMessage() );
        setErrors( 1 );
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }
      if ( sendToErrorRow ) {
        // Simply add this row to the error row
        putError( getInputRowMeta(), r, 1, errorMessage, null, "DynamicSQLRow001" );
      }
    }

    return true;
  }

  /**
   * Stop the running query
   */
  public void stopRunning()throws HopException {

    if ( data.db != null && !data.isCanceled ) {
      synchronized ( data.db ) {
        data.db.cancelQuery();
      }
      setStopped( true );
      data.isCanceled = true;
    }
  }

  public boolean init(){

    if ( super.init() ) {
      if ( meta.getDatabaseMeta() == null ) {
        logError( BaseMessages.getString( PKG, "DynmaicSQLRow.Init.ConnectionMissing", getTransformName() ) );
        return false;
      }
      data.db = new Database( this, variables, meta.getDatabaseMeta() );
      try {
        data.db.connect( getPartitionId() );

        data.db.setCommit( 100 ); // we never get a commit, but it just turns off auto-commit.

        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "DynamicSQLRow.Log.ConnectedToDB" ) );
        }

        data.db.setQueryLimit( meta.getRowLimit() );

        return true;
      } catch ( HopException e ) {
        logError( BaseMessages.getString( PKG, "DynamicSQLRow.Log.DatabaseError" ) + e.getMessage() );
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
