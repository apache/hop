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

package org.apache.hop.pipeline.transforms.tableoutput;

import org.apache.hop.core.Const;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.exception.HopDatabaseBatchException;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * Writes rows to a database table.
 *
 * @author Matt Casters
 * @since 6-apr-2003
 */
public class TableOutput extends BaseTransform<TableOutputMeta, TableOutputData> implements ITransform<TableOutputMeta, TableOutputData> {

  private static final Class<?> PKG = TableOutputMeta.class; // For Translator

  public TableOutput( TransformMeta transformMeta, TableOutputMeta meta, TableOutputData data, int copyNr, PipelineMeta pipelineMeta,
                      Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  public boolean processRow() throws HopException {

    Object[] r = getRow(); // this also waits for a previous transform to be finished.
    if ( r == null ) { // no more input to be expected...
      // truncate the table if there are no rows at all coming into this transform
      if ( first && meta.truncateTable() ) {
        truncateTable();
      }
      return false;
    }

    if ( first ) {
      first = false;
      if ( meta.truncateTable() ) {
        truncateTable();
      }
      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );

      if ( !meta.specifyFields() ) {
        // Just take the input row
        data.insertRowMeta = getInputRowMeta().clone();
      } else {

        data.insertRowMeta = new RowMeta();

        //
        // Cache the position of the compare fields in Row row
        //
        data.valuenrs = new int[ meta.getFieldDatabase().length ];
        for ( int i = 0; i < meta.getFieldDatabase().length; i++ ) {
          data.valuenrs[ i ] = getInputRowMeta().indexOfValue( meta.getFieldStream()[ i ] );
          if ( data.valuenrs[ i ] < 0 ) {
            throw new HopTransformException( BaseMessages.getString(
              PKG, "TableOutput.Exception.FieldRequired", meta.getFieldStream()[ i ] ) );
          }
        }

        for ( int i = 0; i < meta.getFieldDatabase().length; i++ ) {
          IValueMeta insValue = getInputRowMeta().searchValueMeta( meta.getFieldStream()[ i ] );
          if ( insValue != null ) {
            IValueMeta insertValue = insValue.clone();
            insertValue.setName( meta.getFieldDatabase()[ i ] );
            data.insertRowMeta.addValueMeta( insertValue );
          } else {
            throw new HopTransformException( BaseMessages.getString(
              PKG, "TableOutput.Exception.FailedToFindField", meta.getFieldStream()[ i ] ) );
          }
        }
      }
    }

    try {
      Object[] outputRowData = writeToTable( getInputRowMeta(), r );
      if ( outputRowData != null ) {
        putRow( data.outputRowMeta, outputRowData ); // in case we want it go further...
        incrementLinesOutput();
      }

      if ( checkFeedback( getLinesRead() ) ) {
        if ( log.isBasic() ) {
          logBasic( "linenr " + getLinesRead() );
        }
      }
    } catch ( HopException e ) {
      logError( "Because of an error, this transform can't continue: ", e );
      setErrors( 1 );
      stopAll();
      setOutputDone(); // signal end to receiver(s)
      return false;
    }

    return true;
  }

  protected Object[] writeToTable( IRowMeta rowMeta, Object[] r ) throws HopException {

    if ( r == null ) { // Stop: last line or error encountered
      if ( log.isDetailed() ) {
        logDetailed( "Last line inserted: stop" );
      }
      return null;
    }

    PreparedStatement insertStatement = null;
    Object[] insertRowData;
    Object[] outputRowData = r;

    String tableName = null;

    boolean sendToErrorRow = false;
    String errorMessage = null;
    boolean rowIsSafe = false;
    int[] updateCounts = null;
    List<Exception> exceptionsList = null;
    boolean batchProblem = false;
    Object generatedKey = null;

    if ( meta.isTableNameInField() ) {
      // Cache the position of the table name field
      if ( data.indexOfTableNameField < 0 ) {
        String realTablename = resolve( meta.getTableNameField() );
        data.indexOfTableNameField = rowMeta.indexOfValue( realTablename );
        if ( data.indexOfTableNameField < 0 ) {
          String message = "Unable to find table name field [" + realTablename + "] in input row";
          logError( message );
          throw new HopTransformException( message );
        }
        if ( !meta.isTableNameInTable() && !meta.specifyFields() ) {
          data.insertRowMeta.removeValueMeta( data.indexOfTableNameField );
        }
      }
      tableName = rowMeta.getString( r, data.indexOfTableNameField );
      if ( !meta.isTableNameInTable() && !meta.specifyFields() ) {
        // If the name of the table should not be inserted itself, remove the table name
        // from the input row data as well. This forcibly creates a copy of r
        //
        insertRowData = RowDataUtil.removeItem( rowMeta.cloneRow( r ), data.indexOfTableNameField );
      } else {
        insertRowData = r;
      }
    } else if ( meta.isPartitioningEnabled()
      && ( meta.isPartitioningDaily() || meta.isPartitioningMonthly() )
      && ( meta.getPartitioningField() != null && meta.getPartitioningField().length() > 0 ) ) {
      // Initialize some stuff!
      if ( data.indexOfPartitioningField < 0 ) {
        data.indexOfPartitioningField =
          rowMeta.indexOfValue( resolve( meta.getPartitioningField() ) );
        if ( data.indexOfPartitioningField < 0 ) {
          throw new HopTransformException( "Unable to find field ["
            + meta.getPartitioningField() + "] in the input row!" );
        }

        if ( meta.isPartitioningDaily() ) {
          data.dateFormater = new SimpleDateFormat( "yyyyMMdd" );
        } else {
          data.dateFormater = new SimpleDateFormat( "yyyyMM" );
        }
      }

      IValueMeta partitioningValue = rowMeta.getValueMeta( data.indexOfPartitioningField );
      if ( !partitioningValue.isDate() || r[ data.indexOfPartitioningField ] == null ) {
        throw new HopTransformException(
          "Sorry, the partitioning field needs to contain a data value and can't be empty!" );
      }

      Object partitioningValueData = rowMeta.getDate( r, data.indexOfPartitioningField );
      tableName =
        resolve( meta.getTableName() )
          + "_" + data.dateFormater.format( (Date) partitioningValueData );
      insertRowData = r;
    } else {
      tableName = data.tableName;
      insertRowData = r;
    }

    if ( meta.specifyFields() ) {
      //
      // The values to insert are those in the fields sections
      //
      insertRowData = new Object[ data.valuenrs.length ];
      for ( int idx = 0; idx < data.valuenrs.length; idx++ ) {
        insertRowData[ idx ] = r[ data.valuenrs[ idx ] ];
      }
    }

    if ( Utils.isEmpty( tableName ) ) {
      throw new HopTransformException( "The tablename is not defined (empty)" );
    }

    insertStatement = data.preparedStatements.get( tableName );
    if ( insertStatement == null ) {
      String sql =
        data.db
          .getInsertStatement( resolve( meta.getSchemaName() ), tableName, data.insertRowMeta );
      if ( log.isDetailed() ) {
        logDetailed( "Prepared statement : " + sql );
      }
      insertStatement = data.db.prepareSql( sql, meta.isReturningGeneratedKeys() );
      data.preparedStatements.put( tableName, insertStatement );
    }

    try {
      // For PG & GP, we add a savepoint before the row.
      // Then revert to the savepoint afterwards... (not a transaction, so hopefully still fast)
      //
      if ( data.useSafePoints ) {
        data.savepoint = data.db.setSavepoint();
      }
      data.db.setValues( data.insertRowMeta, insertRowData, insertStatement );
      data.db.insertRow( insertStatement, data.batchMode, false ); // false: no commit, it is handled in this transform differently
      if ( isRowLevel() ) {
        logRowlevel( "Written row: " + data.insertRowMeta.getString( insertRowData ) );
      }

      // Get a commit counter per prepared statement to keep track of separate tables, etc.
      //
      Integer commitCounter = data.commitCounterMap.get( tableName );
      if ( commitCounter == null ) {
        commitCounter = Integer.valueOf( 1 );
      } else {
        commitCounter++;
      }
      data.commitCounterMap.put( tableName, Integer.valueOf( commitCounter.intValue() ) );

      // Release the savepoint if needed
      //
      if ( data.useSafePoints ) {
        if ( data.releaseSavepoint ) {
          data.db.releaseSavepoint( data.savepoint );
        }
      }

      // Perform a commit if needed
      //

      if ( ( data.commitSize > 0 ) && ( ( commitCounter % data.commitSize ) == 0 ) ) {
        if ( data.db.getUseBatchInsert( data.batchMode ) ) {
          try {
            insertStatement.executeBatch();
            data.db.commit();
            insertStatement.clearBatch();
          } catch ( SQLException ex ) {
            throw Database.createHopDatabaseBatchException( "Error updating batch", ex );
          } catch ( Exception ex ) {
            throw new HopDatabaseException( "Unexpected error inserting row", ex );
          }
        } else {
          // insertRow normal commit
          data.db.commit();
        }
        // Clear the batch/commit counter...
        //
        data.commitCounterMap.put( tableName, Integer.valueOf( 0 ) );
        rowIsSafe = true;
      } else {
        rowIsSafe = false;
      }

      // See if we need to get back the keys as well...
      if ( meta.isReturningGeneratedKeys() ) {
        RowMetaAndData extraKeys = data.db.getGeneratedKeys( insertStatement );

        if ( extraKeys.getRowMeta().size() > 0 ) {
          // Send out the good word!
          // Only 1 key at the moment. (should be enough for now :-)
          generatedKey = extraKeys.getRowMeta().getInteger( extraKeys.getData(), 0 );
        } else {
          // we have to throw something here, else we don't know what the
          // type is of the returned key(s) and we would violate our own rule
          // that a hop should always contain rows of the same type.
          throw new HopTransformException( "No generated keys while \"return generated keys\" is active!" );
        }
      }
    } catch ( HopDatabaseBatchException be ) {
      errorMessage = be.toString();
      batchProblem = true;
      sendToErrorRow = true;
      updateCounts = be.getUpdateCounts();
      exceptionsList = be.getExceptionsList();

      if ( getTransformMeta().isDoingErrorHandling() ) {
        data.db.clearBatch( insertStatement );
        data.db.commit( true );
      } else {
        data.db.clearBatch( insertStatement );
        data.db.rollback();
        StringBuilder msg = new StringBuilder( "Error batch inserting rows into table [" + tableName + "]." );
        msg.append( Const.CR );
        msg.append( "Errors encountered (first 10):" ).append( Const.CR );
        for ( int x = 0; x < be.getExceptionsList().size() && x < 10; x++ ) {
          Exception exception = be.getExceptionsList().get( x );
          if ( exception.getMessage() != null ) {
            msg.append( exception.getMessage() ).append( Const.CR );
          }
        }
        throw new HopException( msg.toString(), be );
      }
    } catch ( HopDatabaseException dbe ) {
      if ( getTransformMeta().isDoingErrorHandling() ) {
        if ( isRowLevel() ) {
          logRowlevel( "Written row to error handling : " + getInputRowMeta().getString( r ) );
        }

        if ( data.useSafePoints ) {
          data.db.rollback( data.savepoint );
          if ( data.releaseSavepoint ) {
            data.db.releaseSavepoint( data.savepoint );
          }
          // data.db.commit(true); // force a commit on the connection too.
        }

        sendToErrorRow = true;
        errorMessage = dbe.toString();
      } else {
        if ( meta.ignoreErrors() ) {
          if ( data.warnings < 20 ) {
            if ( log.isBasic() ) {
              logBasic( "WARNING: Couldn't insert row into table: "
                + rowMeta.getString( r ) + Const.CR + dbe.getMessage() );
            }
          } else if ( data.warnings == 20 ) {
            if ( log.isBasic() ) {
              logBasic( "FINAL WARNING (no more then 20 displayed): Couldn't insert row into table: "
                + rowMeta.getString( r ) + Const.CR + dbe.getMessage() );
            }
          }
          data.warnings++;
        } else {
          setErrors( getErrors() + 1 );
          data.db.rollback();
          throw new HopException( "Error inserting row into table ["
            + tableName + "] with values: " + rowMeta.getString( r ), dbe );
        }
      }
    }

    // We need to add a key
    if ( generatedKey != null ) {
      outputRowData = RowDataUtil.addValueData( outputRowData, rowMeta.size(), generatedKey );
    }

    if ( data.batchMode ) {
      if ( sendToErrorRow ) {
        if ( batchProblem ) {
          data.batchBuffer.add( outputRowData );
          outputRowData = null;

          processBatchException( errorMessage, updateCounts, exceptionsList );
        } else {
          // Simply add this row to the error row
          putError( rowMeta, r, 1L, errorMessage, null, "TOP001" );
          outputRowData = null;
        }
      } else {
        data.batchBuffer.add( outputRowData );
        outputRowData = null;

        if ( rowIsSafe ) { // A commit was done and the rows are all safe (no error)
          for ( int i = 0; i < data.batchBuffer.size(); i++ ) {
            Object[] row = data.batchBuffer.get( i );
            putRow( data.outputRowMeta, row );
            incrementLinesOutput();
          }
          // Clear the buffer
          data.batchBuffer.clear();
        }
      }
    } else {
      if ( sendToErrorRow ) {
        putError( rowMeta, r, 1, errorMessage, null, "TOP001" );
        outputRowData = null;
      }
    }

    return outputRowData;
  }

  public boolean isRowLevel() {
    return log.isRowLevel();
  }

  private void processBatchException( String errorMessage, int[] updateCounts, List<Exception> exceptionsList ) throws HopException {
    // There was an error with the commit
    // We should put all the failing rows out there...
    //
    if ( updateCounts != null ) {
      int errNr = 0;
      for ( int i = 0; i < updateCounts.length; i++ ) {
        Object[] row = data.batchBuffer.get( i );
        if ( updateCounts[ i ] > 0 ) {
          // send the error foward
          putRow( data.outputRowMeta, row );
          incrementLinesOutput();
        } else {
          String exMessage = errorMessage;
          if ( errNr < exceptionsList.size() ) {
            SQLException se = (SQLException) exceptionsList.get( errNr );
            errNr++;
            exMessage = se.toString();
          }
          putError( data.outputRowMeta, row, 1L, exMessage, null, "TOP0002" );
        }
      }
    } else {
      // If we don't have update counts, it probably means the DB doesn't support it.
      // In this case we don't have a choice but to consider all inserted rows to be error rows.
      //
      for ( int i = 0; i < data.batchBuffer.size(); i++ ) {
        Object[] row = data.batchBuffer.get( i );
        putError( data.outputRowMeta, row, 1L, errorMessage, null, "TOP0003" );
      }
    }

    // Clear the buffer afterwards...
    data.batchBuffer.clear();
  }

  public boolean init(){

    if ( super.init() ) {
      try {
        data.commitSize = Integer.parseInt( resolve( meta.getCommitSize() ) );

        data.databaseMeta = meta.getDatabaseMeta();
        IDatabase dbInterface = data.databaseMeta.getIDatabase();

        // Batch updates are not supported on PostgreSQL (and look-a-likes)
        // together with error handling (PDI-366).
        // For these situations we can use savepoints to help out.
        //
        data.useSafePoints =
          data.databaseMeta.getIDatabase().useSafePoints() && getTransformMeta().isDoingErrorHandling();

        // Get the boolean that indicates whether or not we can/should release
        // savepoints during data load.
        //
        data.releaseSavepoint = dbInterface.releaseSavepoint();

        // Disable batch mode in case
        // - we use an unlimited commit size
        // - if we need to pick up auto-generated keys
        // - if you are running the pipeline as a single database transaction (unique connections)
        // - if we are reverting to save-points
        //
        data.batchMode =
          meta.useBatchUpdate()
            && data.commitSize > 0 && !meta.isReturningGeneratedKeys()
            && !data.useSafePoints;

        // Per PDI-6211 : give a warning that batch mode operation in combination with transform error handling can lead to
        // incorrectly processed rows.
        //
        if ( getTransformMeta().isDoingErrorHandling() && !dbInterface.supportsErrorHandlingOnBatchUpdates() ) {
          log.logBasic( BaseMessages.getString(
            PKG, "TableOutput.Warning.ErrorHandlingIsNotFullySupportedWithBatchProcessing" ) );
        }

        if ( meta.getDatabaseMeta() == null ) {
          throw new HopException( BaseMessages.getString(
            PKG, "TableOutput.Exception.DatabaseNeedsToBeSelected" ) );
        }
        if ( meta.getDatabaseMeta() == null ) {
          logError( BaseMessages.getString( PKG, "TableOutput.Init.ConnectionMissing", getTransformName() ) );
          return false;
        }

        if ( !dbInterface.supportsStandardTableOutput() ) {
          throw new HopException( dbInterface.getUnsupportedTableOutputMessage() );
        }

        data.db = new Database( this, this, meta.getDatabaseMeta() );
        data.db.connect( getPartitionId() );

        if ( log.isBasic() ) {
          logBasic( "Connected to database [" + meta.getDatabaseMeta() + "] (commit=" + data.commitSize + ")" );
        }

        // Postpone commit as long as possible. PDI-2091
        //
        if ( data.commitSize == 0 ) {
          data.commitSize = Integer.MAX_VALUE;
        }
        data.db.setCommit( data.commitSize );

        if ( !meta.isPartitioningEnabled() && !meta.isTableNameInField() ) {
          data.tableName = resolve( meta.getTableName() );
        }

        return true;
      } catch ( HopException e ) {
        logError( "An error occurred intialising this transform: " + e.getMessage() );
        stopAll();
        setErrors( 1 );
      }
    }
    return false;
  }

  void truncateTable() throws HopDatabaseException {
    if ( !meta.isPartitioningEnabled() && !meta.isTableNameInField() ) {
      // Only the first one truncates in a non-partitioned transform copy
      //
      if ( meta.truncateTable()
        && ( ( getCopy() == 0 ) || !Utils.isEmpty( getPartitionId() ) ) ) {
        data.db.truncateTable( resolve( meta.getSchemaName() ), resolve( meta
          .getTableName() ) );

      }
    }
  }

  public void dispose(){

    if ( data.db != null ) {
      try {
        for ( String schemaTable : data.preparedStatements.keySet() ) {
          // Get a commit counter per prepared statement to keep track of separate tables, etc.
          //
          Integer batchCounter = data.commitCounterMap.get( schemaTable );
          if ( batchCounter == null ) {
            batchCounter = 0;
          }

          PreparedStatement insertStatement = data.preparedStatements.get( schemaTable );

          data.db.emptyAndCommit( insertStatement, data.batchMode, batchCounter );
        }
        for ( int i = 0; i < data.batchBuffer.size(); i++ ) {
          Object[] row = data.batchBuffer.get( i );
          putRow( data.outputRowMeta, row );
          incrementLinesOutput();
        }
        // Clear the buffer
        data.batchBuffer.clear();
      } catch ( HopDatabaseBatchException be ) {
        if ( getTransformMeta().isDoingErrorHandling() ) {
          // Right at the back we are experiencing a batch commit problem...
          // OK, we have the numbers...
          try {
            processBatchException( be.toString(), be.getUpdateCounts(), be.getExceptionsList() );
          } catch ( HopException e ) {
            logError( "Unexpected error processing batch error", e );
            setErrors( 1 );
            stopAll();
          }
        } else {
          logError( "Unexpected batch update error committing the database connection.", be );
          setErrors( 1 );
          stopAll();
        }
      } catch ( Exception dbe ) {
        logError( "Unexpected error committing the database connection.", dbe );
        logError( Const.getStackTracker( dbe ) );
        setErrors( 1 );
        stopAll();
      } finally {
        setOutputDone();

        if ( getErrors() > 0 ) {
          try {
            data.db.rollback();
          } catch ( HopDatabaseException e ) {
            logError( "Unexpected error rolling back the database connection.", e );
          }
        }

        data.db.disconnect();
      }
      super.dispose();
    }
  }
}
