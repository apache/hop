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

package org.apache.hop.pipeline.transforms.delete;

import org.apache.hop.core.Const;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.sql.SQLException;

/**
 * Delete data in a database table.
 *
 * @author Tom
 * @since 28-March-2006
 */
public class Delete extends BaseTransform<DeleteMeta, DeleteData> implements ITransform<DeleteMeta, DeleteData> {

  private static Class<?> PKG = DeleteMeta.class; // for i18n purposes, needed by Translator!!

  public Delete( TransformMeta transformMeta, DeleteMeta meta, DeleteData data, int copyNr, PipelineMeta pipelineMeta,
                 Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  private synchronized void deleteValues( IRowMeta rowMeta, Object[] row ) throws HopException {
    // OK, now do the lookup.
    // We need the lookupvalues for that.
    Object[] deleteRow = new Object[ data.deleteParameterRowMeta.size() ];
    int deleteIndex = 0;

    for ( int i = 0; i < meta.getKeyStream().length; i++ ) {
      if ( data.keynrs[ i ] >= 0 ) {
        deleteRow[ deleteIndex ] = row[ data.keynrs[ i ] ];
        deleteIndex++;
      }
      if ( data.keynrs2[ i ] >= 0 ) {
        deleteRow[ deleteIndex ] = row[ data.keynrs2[ i ] ];
        deleteIndex++;
      }
    }

    data.db.setValues( data.deleteParameterRowMeta, deleteRow, data.prepStatementDelete );

    if ( log.isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "Delete.Log.SetValuesForDelete", data.deleteParameterRowMeta
        .getString( deleteRow ), rowMeta.getString( row ) ) );
    }

    data.db.insertRow( data.prepStatementDelete );
    incrementLinesUpdated();
  }

  public boolean processRow() throws HopException {
    boolean sendToErrorRow = false;
    String errorMessage = null;

    Object[] r = getRow(); // Get row from input rowset & set row busy!
    if ( r == null ) { // no more input to be expected...

      setOutputDone();
      return false;
    }

    if ( first ) {
      first = false;

      // What's the output Row format?
      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metaStore );

      data.schemaTable =
        meta.getDatabaseMeta().getQuotedSchemaTableCombination(
          environmentSubstitute( meta.getSchemaName() ), environmentSubstitute( meta.getTableName() ) );

      // lookup the values!
      if ( log.isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "Delete.Log.CheckingRow" ) + getInputRowMeta().getString( r ) );
      }

      data.keynrs = new int[ meta.getKeyStream().length ];
      data.keynrs2 = new int[ meta.getKeyStream().length ];
      for ( int i = 0; i < meta.getKeyStream().length; i++ ) {
        data.keynrs[ i ] = getInputRowMeta().indexOfValue( meta.getKeyStream()[ i ] );
        if ( data.keynrs[ i ] < 0 && // couldn't find field!
          !"IS NULL".equalsIgnoreCase( meta.getKeyCondition()[ i ] ) && // No field needed!
          !"IS NOT NULL".equalsIgnoreCase( meta.getKeyCondition()[ i ] ) // No field needed!
        ) {
          throw new HopTransformException( BaseMessages.getString( PKG, "Delete.Exception.FieldRequired", meta
            .getKeyStream()[ i ] ) );
        }
        data.keynrs2[ i ] = getInputRowMeta().indexOfValue( meta.getKeyStream2()[ i ] );
        if ( data.keynrs2[ i ] < 0 && // couldn't find field!
          "BETWEEN".equalsIgnoreCase( meta.getKeyCondition()[ i ] ) // 2 fields needed!
        ) {
          throw new HopTransformException( BaseMessages.getString( PKG, "Delete.Exception.FieldRequired", meta
            .getKeyStream2()[ i ] ) );
        }

        if ( log.isDebug() ) {
          logDebug( BaseMessages.getString( PKG, "Delete.Log.FieldInfo", meta.getKeyStream()[ i ] ) + data.keynrs[ i ] );
        }
      }

      prepareDelete( getInputRowMeta() );
    }

    try {
      deleteValues( getInputRowMeta(), r ); // add new values to the row in rowset[0].
      putRow( data.outputRowMeta, r ); // output the same rows of data, but with a copy of the metadata

      if ( checkFeedback( getLinesRead() ) ) {
        if ( log.isBasic() ) {
          logBasic( BaseMessages.getString( PKG, "Delete.Log.LineNumber" ) + getLinesRead() );
        }
      }
    } catch ( HopException e ) {

      if ( getTransformMeta().isDoingErrorHandling() ) {
        sendToErrorRow = true;
        errorMessage = e.toString();
      } else {

        logError( BaseMessages.getString( PKG, "Delete.Log.ErrorInTransform" ) + e.getMessage() );
        setErrors( 1 );
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }

      if ( sendToErrorRow ) {
        // Simply add this row to the error row
        putError( getInputRowMeta(), r, 1, errorMessage, null, "DEL001" );
      }
    }

    return true;
  }

  // Lookup certain fields in a table
  public void prepareDelete( IRowMeta rowMeta ) throws HopDatabaseException {
    DatabaseMeta databaseMeta = meta.getDatabaseMeta();
    data.deleteParameterRowMeta = new RowMeta();

    String sql = "DELETE FROM " + data.schemaTable + Const.CR;

    sql += "WHERE ";

    for ( int i = 0; i < meta.getKeyLookup().length; i++ ) {
      if ( i != 0 ) {
        sql += "AND   ";
      }
      sql += databaseMeta.quoteField( meta.getKeyLookup()[ i ] );
      if ( "BETWEEN".equalsIgnoreCase( meta.getKeyCondition()[ i ] ) ) {
        sql += " BETWEEN ? AND ? ";
        data.deleteParameterRowMeta.addValueMeta( rowMeta.searchValueMeta( meta.getKeyStream()[ i ] ) );
        data.deleteParameterRowMeta.addValueMeta( rowMeta.searchValueMeta( meta.getKeyStream2()[ i ] ) );
      } else if ( "IS NULL".equalsIgnoreCase( meta.getKeyCondition()[ i ] )
        || "IS NOT NULL".equalsIgnoreCase( meta.getKeyCondition()[ i ] ) ) {
        sql += " " + meta.getKeyCondition()[ i ] + " ";
      } else {
        sql += " " + meta.getKeyCondition()[ i ] + " ? ";
        data.deleteParameterRowMeta.addValueMeta( rowMeta.searchValueMeta( meta.getKeyStream()[ i ] ) );
      }
    }

    try {
      if ( log.isDetailed() ) {
        logDetailed( "Setting delete preparedStatement to [" + sql + "]" );
      }
      data.prepStatementDelete = data.db.getConnection().prepareStatement( databaseMeta.stripCR( sql ) );
    } catch ( SQLException ex ) {
      throw new HopDatabaseException( "Unable to prepare statement for SQL statement [" + sql + "]", ex );
    }
  }

  public boolean init(){
    if ( super.init() ) {
      if ( meta.getDatabaseMeta() == null ) {
        logError( BaseMessages.getString( PKG, "Delete.Init.ConnectionMissing", getTransformName() ) );
        return false;
      }
      data.db = new Database( this, meta.getDatabaseMeta() );
      data.db.shareVariablesWith( this );
      try {
        data.db.connect( getPartitionId() );

        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "Delete.Log.ConnectedToDB" ) );
        }

        data.db.setCommit( meta.getCommitSize( this ) );

        return true;
      } catch ( HopException ke ) {
        logError( BaseMessages.getString( PKG, "Delete.Log.ErrorOccurred" ) + ke.getMessage() );
        setErrors( 1 );
        stopAll();
      }
    }
    return false;
  }

  public void dispose(){

    if ( data.db != null ) {
      try {
        if ( !data.db.isAutoCommit() ) {
          if ( getErrors() == 0 ) {
            data.db.commit();
          } else {
            data.db.rollback();
          }
        }
        data.db.closeUpdate();
      } catch ( HopDatabaseException e ) {
        logError( BaseMessages.getString( PKG, "Delete.Log.UnableToCommitUpdateConnection" )
          + data.db + "] :" + e.toString() );
        setErrors( 1 );
      } finally {
        data.db.disconnect();
      }
    }
    super.dispose();
  }

}
