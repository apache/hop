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

package org.apache.hop.pipeline.transforms.combinationlookup;

import org.apache.hop.core.Const;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopConfigException;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

/**
 * Manages or looks up information in a Type 1 or junk dimension.
 * <p>
 * <p>
 * 1) Lookup combination field1..n in a dimension
 * <p>
 * 2) If this combination exists, return technical key
 * <p>
 * 3) If this combination doesn't exist, insert & return technical key
 * <p>
 * 4) if replace is Y, remove all key fields from output.
 * <p>
 * <p>
 *
 * @author Matt
 * @since 22-jul-2003
 */
public class CombinationLookup extends BaseTransform<CombinationLookupMeta, CombinationLookupData> implements ITransform<CombinationLookupMeta, CombinationLookupData> {

  private static final Class<?> PKG = CombinationLookupMeta.class; // For Translator

  private static final int CREATION_METHOD_AUTOINC = 1;
  private static final int CREATION_METHOD_SEQUENCE = 2;
  private static final int CREATION_METHOD_TABLEMAX = 3;

  private int techKeyCreation;

  public CombinationLookup( TransformMeta transformMeta, CombinationLookupMeta meta, CombinationLookupData data, int copyNr,
                            PipelineMeta pipelineMeta, Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  private void setTechKeyCreation( int method ) {
    techKeyCreation = method;
  }

  private int getTechKeyCreation() {
    return techKeyCreation;
  }

  private void determineTechKeyCreation() {
    String keyCreation = meta.getTechKeyCreation();
    if ( meta.getDatabaseMeta().supportsAutoinc()
      && CombinationLookupMeta.CREATION_METHOD_AUTOINC.equals( keyCreation ) ) {
      setTechKeyCreation( CREATION_METHOD_AUTOINC );
    } else if ( meta.getDatabaseMeta().supportsSequences()
      && CombinationLookupMeta.CREATION_METHOD_SEQUENCE.equals( keyCreation ) ) {
      setTechKeyCreation( CREATION_METHOD_SEQUENCE );
    } else {
      setTechKeyCreation( CREATION_METHOD_TABLEMAX );
    }
  }

  private Long lookupInCache( IRowMeta rowMeta, Object[] row ) {
    // Short circuit if cache is disabled.
    if ( meta.getCacheSize() == -1 ) {
      return null;
    }

    // try to find the row in the cache...
    //
    Long tk = data.cache.get( new RowMetaAndData( rowMeta, row ) );
    return tk;
  }

  /**
   * Adds a row to the cache In case we are doing updates, we need to store the complete rows from the database. These
   * are the values we need to store
   * <p>
   * Key: - natural key fields Value: - Technical key - lookup fields / extra fields (allows us to compare or retrieve)
   * - Date_from - Date_to
   *
   * @param rowMeta
   * @param row
   * @param tk
   * @throws HopValueException
   */
  private void addToCache( IRowMeta rowMeta, Object[] row, Long tk ) throws HopValueException {
    // Short circuit if cache is disabled.
    if ( meta.getCacheSize() == -1 ) {
      return;
    }

    // store it in the cache if needed.
    data.cache.put( new RowMetaAndData( rowMeta, row ), tk );

    // check if the size is not too big...
    // Allow for a buffer overrun of 20% and then remove those 20% in one go.
    // Just to keep performance in track.
    //
    int tenPercent = meta.getCacheSize() / 10;
    if ( meta.getCacheSize() > 0 && data.cache.size() > meta.getCacheSize() + tenPercent ) {
      // Which cache entries do we delete here?
      // We delete those with the lowest technical key...
      // Those would arguably be the "oldest" dimension entries.
      // Oh well... Nothing is going to be perfect here...
      //
      // Getting the lowest 20% requires some kind of sorting algorithm and I'm not sure we want to do that.
      // Sorting is slow and even in the best case situation we need to do 2 passes over the cache entries...
      //
      // Perhaps we should get 20% random values and delete everything below the lowest but one TK.
      //
      List<RowMetaAndData> keys = new ArrayList<>( data.cache.keySet() );
      int sizeBefore = keys.size();
      List<Long> samples = new ArrayList<>();

      // Take 10 sample technical keys....
      int transformsize = keys.size() / 5;
      if ( transformsize < 1 ) {
        transformsize = 1; // make sure we have no endless loop
      }
      for ( int i = 0; i < keys.size(); i += transformsize ) {
        RowMetaAndData key = keys.get( i );
        Long value = data.cache.get( key );
        if ( value != null ) {
          samples.add( value );
        }
      }
      // Sort these 5 elements...
      Collections.sort( samples );

      // What is the smallest?
      // Take the second, not the fist in the list, otherwise we would be removing a single entry = not good.
      if ( samples.size() > 1 ) {
        data.smallestCacheKey = samples.get( 1 ).longValue();
      } else { // except when there is only one sample
        data.smallestCacheKey = samples.get( 0 ).longValue();
      }

      // Remove anything in the cache <= smallest.
      // This makes it almost single pass...
      // This algorithm is not 100% correct, but I guess it beats sorting the whole cache all the time.
      //
      for ( int i = 0; i < keys.size(); i++ ) {
        RowMetaAndData key = keys.get( i );
        Long value = data.cache.get( key );
        if ( value != null ) {
          if ( value.longValue() <= data.smallestCacheKey ) {
            data.cache.remove( key ); // this one has to go.
          }
        }
      }

      int sizeAfter = data.cache.size();
      logDetailed( "Reduced the lookup cache from " + sizeBefore + " to " + sizeAfter + " rows." );
    }

    if ( isRowLevel() ) {
      logRowlevel( "Cache store: key=" + rowMeta.getString( row ) + "    key=" + tk );
    }
  }

  protected boolean isAutoIncrement() {
    return techKeyCreation == CREATION_METHOD_AUTOINC;
  }

  @SuppressWarnings( "deprecation" )
  private Object[] lookupValues( IRowMeta rowMeta, Object[] row ) throws HopException {
    Long valKey = null;
    Long val_hash = null;
    Object[] hashRow = null;

    Object[] lookupRow = new Object[ data.lookupRowMeta.size() ];
    int lookupIndex = 0;

    if ( meta.useHash() || meta.getCacheSize() >= 0 ) {
      hashRow = new Object[ data.hashRowMeta.size() ];
      for ( int i = 0; i < meta.getKeyField().length; i++ ) {
        hashRow[ i ] = row[ data.keynrs[ i ] ];
      }

      if ( meta.useHash() ) {
        val_hash = new Long( data.hashRowMeta.oldXORHashCode( hashRow ) );
        lookupRow[ lookupIndex ] = val_hash;
        lookupIndex++;
      }
    }

    for ( int i = 0; i < meta.getKeyField().length; i++ ) {
      // Determine the index of this Key Field in the row meta/data
      int rowIndex = data.keynrs[ i ];
      lookupRow[ lookupIndex ] = row[ rowIndex ]; // KEYi = ?
      lookupIndex++;

      if ( meta.getDatabaseMeta().requiresCastToVariousForIsNull()
        && rowMeta.getValueMeta( rowIndex ).getType() == IValueMeta.TYPE_STRING ) {
        lookupRow[ lookupIndex ] =
          rowMeta.getValueMeta( rowIndex ).isNull( row[ rowIndex ] ) ? null : "NotNull"; // KEYi IS
        // NULL or
        // ? IS
        // NULL
      } else {
        lookupRow[ lookupIndex ] = row[ data.keynrs[ i ] ]; // KEYi IS NULL or ? IS NULL
      }
      lookupIndex++;
    }

    // Before doing the actual lookup in the database, see if it's not in the cache...
    valKey = lookupInCache( data.hashRowMeta, hashRow );
    if ( valKey == null ) {
      data.db.setValues( data.lookupRowMeta, lookupRow, data.prepStatementLookup );
      Object[] add = data.db.getLookup( data.prepStatementLookup );
      incrementLinesInput();

      if ( add == null ) { // The dimension entry was not found, we need to add it!
        // First try to use an AUTOINCREMENT field
        switch ( getTechKeyCreation() ) {
          case CREATION_METHOD_TABLEMAX:
            // Use our own counter: what's the next value for the technical key?
            valKey = data.db.getNextValue( data.realSchemaName, data.realTableName,
              meta.getTechnicalKeyField() );
            break;
          case CREATION_METHOD_AUTOINC:
            valKey = new Long( 0 ); // value to accept new key...
            break;
          case CREATION_METHOD_SEQUENCE:
            valKey =
              data.db.getNextSequenceValue( data.realSchemaName, meta.getSequenceFrom(), meta
                .getTechnicalKeyField() );
            if ( valKey != null && isRowLevel() ) {
              logRowlevel( BaseMessages.getString( PKG, "CombinationLookup.Log.FoundNextSequenceValue" )
                + valKey.toString() );
            }
            break;
          default:
            break;
        }

        valKey = combiInsert( rowMeta, row, valKey, val_hash );
        incrementLinesOutput();

        if ( isRowLevel() ) {
          logRowlevel( BaseMessages.getString( PKG, "CombinationLookup.Log.AddedDimensionEntry" ) + valKey );
        }

        // Also store it in our Hashtable...
        addToCache( data.hashRowMeta, hashRow, valKey );
      } else {
        // Entry already exists...
        //
        valKey = data.db.getReturnRowMeta().getInteger( add, 0 ); // Sometimes it's not an integer, believe it or not.
        addToCache( data.hashRowMeta, hashRow, valKey );
      }
    }

    Object[] outputRow = new Object[ data.outputRowMeta.size() ];
    int outputIndex = 0;

    // See if we need to replace the fields with the technical key
    if ( meta.replaceFields() ) {
      for ( int i = 0; i < rowMeta.size(); i++ ) {
        if ( !data.removeField[ i ] ) {
          outputRow[ outputIndex ] = row[ i ];
          outputIndex++;
        }
      }
    } else {
      // Just copy the input row and add the technical key
      for ( outputIndex = 0; outputIndex < rowMeta.size(); outputIndex++ ) {
        outputRow[ outputIndex ] = row[ outputIndex ];
      }
    }

    // Add the technical key...
    outputRow[ outputIndex ] = valKey;

    return outputRow;
  }

  @Override
  public boolean processRow() throws HopException {
    Object[] r = getRow(); // Get row from input rowset & set row busy!
    // if no more input to be expected set done
    if ( r == null ) {
      setOutputDone();
      return false;
    }

    if ( first ) {
      first = false;

      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );

      data.schemaTable =
        meta.getDatabaseMeta().getQuotedSchemaTableCombination( this, data.realSchemaName, data.realTableName );

      determineTechKeyCreation();

      // The indexes of the key values...
      //
      data.keynrs = new int[ meta.getKeyField().length ];

      for ( int i = 0; i < meta.getKeyField().length; i++ ) {
        data.keynrs[ i ] = getInputRowMeta().indexOfValue( meta.getKeyField()[ i ] );
        if ( data.keynrs[ i ] < 0 ) {
          // couldn't find field!
          throw new HopTransformException( BaseMessages.getString(
            PKG, "CombinationLookup.Exception.FieldNotFound", meta.getKeyField()[ i ] ) );
        }
      }

      // Determine for each input field if we want it removed or not.
      //
      data.removeField = new boolean[ getInputRowMeta().size() ];

      // Sort lookup values keys so that we
      //
      for ( int i = 0; i < getInputRowMeta().size(); i++ ) {
        IValueMeta valueMeta = getInputRowMeta().getValueMeta( i );
        // Is this one of the keys?
        int idx = Const.indexOfString( valueMeta.getName(), meta.getKeyField() );
        data.removeField[ i ] = idx >= 0;
      }

      // Determine the metadata row to calculate hashcodes.
      //
      data.hashRowMeta = new RowMeta();
      for ( int i = 0; i < meta.getKeyField().length; i++ ) {
        data.hashRowMeta.addValueMeta( getInputRowMeta().getValueMeta( data.keynrs[ i ] ) ); // KEYi = ?
      }

      setCombiLookup( getInputRowMeta() );
      preloadCache( data.hashRowMeta );
    }

    try {
      Object[] outputRow = lookupValues( getInputRowMeta(), r ); // add new values to the row in rowset[0].
      putRow( data.outputRowMeta, outputRow ); // copy row to output rowset(s);

      if ( checkFeedback( getLinesRead() ) ) {
        if ( log.isBasic() ) {
          logBasic( BaseMessages.getString( PKG, "CombinationLookup.Log.LineNumber" ) + getLinesRead() );
        }
      }
    } catch ( HopException e ) {
      if ( getTransformMeta().isDoingErrorHandling() ) {
        putError( getInputRowMeta(), r, 1L, Const.getStackTracker( e ), null, "CBL001" );
      } else {
        logError( BaseMessages.getString( PKG, "CombinationLookup.Log.ErrorInTransformRunning" ) + e.getMessage() );
        setErrors( 1 );
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }
    }

    return true;
  }

  /**
   * CombinationLookup table: dimension table keys[]: which dim-fields do we use to look up key? retval: name of the key
   * to return
   */
  public void setCombiLookup( IRowMeta inputRowMeta ) throws HopDatabaseException {
    DatabaseMeta databaseMeta = meta.getDatabaseMeta();

    String sql = "";
    boolean comma;
    data.lookupRowMeta = new RowMeta();

    /*
     * SELECT <retval> FROM <table> WHERE ( ( <key1> = ? ) OR ( <key1> IS NULL AND ? IS NULL ) ) AND ( ( <key2> = ? ) OR
     * ( <key1> IS NULL AND ? IS NULL ) ) ... ;
     *
     * OR
     *
     * SELECT <retval> FROM <table> WHERE <crcfield> = ? AND ( ( <key1> = ? ) OR ( <key1> IS NULL AND ? IS NULL ) ) AND
     * ( ( <key2> = ? ) OR ( <key1> IS NULL AND ? IS NULL ) ) ... ;
     */

    sql += "SELECT " + databaseMeta.quoteField( meta.getTechnicalKeyField() ) + Const.CR;
    sql += "FROM " + data.schemaTable + Const.CR;
    sql += "WHERE ";
    comma = false;

    if ( meta.useHash() ) {
      sql += databaseMeta.quoteField( meta.getHashField() ) + " = ? " + Const.CR;
      comma = true;
      data.lookupRowMeta.addValueMeta( new ValueMetaInteger( meta.getHashField() ) );
    } else {
      sql += "( ( ";
    }

    for ( int i = 0; i < meta.getKeyLookup().length; i++ ) {
      if ( comma ) {
        sql += " AND ( ( ";
      } else {
        comma = true;
      }
      sql +=
        databaseMeta.quoteField( meta.getKeyLookup()[ i ] )
          + " = ? ) OR ( " + databaseMeta.quoteField( meta.getKeyLookup()[ i ] );
      data.lookupRowMeta.addValueMeta( inputRowMeta.getValueMeta( data.keynrs[ i ] ) );

      sql += " IS NULL AND ";
      if ( databaseMeta.requiresCastToVariousForIsNull() ) {
        sql += "CAST(? AS VARCHAR(256)) IS NULL";
      } else {
        sql += "? IS NULL";
      }
      // Add the ValueMeta for the null check, BUT cloning needed.
      // Otherwise the field gets renamed and gives problems when referenced by previous transforms.
      data.lookupRowMeta.addValueMeta( inputRowMeta.getValueMeta( data.keynrs[ i ] ).clone() );

      sql += " ) )";
      sql += Const.CR;
    }

    try {
      if ( log.isDebug() ) {
        logDebug( "preparing combi-lookup statement:" + Const.CR + sql );
      }
      data.prepStatementLookup = data.db.getConnection().prepareStatement( databaseMeta.stripCR( sql ) );
      if ( databaseMeta.supportsSetMaxRows() ) {
        data.prepStatementLookup.setMaxRows( 1 ); // alywas get only 1 line back!
      }
    } catch ( SQLException ex ) {
      throw new HopDatabaseException( "Unable to prepare combi-lookup statement", ex );
    }
  }

  /**
   * This inserts new record into a junk dimension
   */
  public Long combiInsert( IRowMeta rowMeta, Object[] row, Long valKey, Long valCrc )
    throws HopDatabaseException {
    String debug = "Combination insert";
    DatabaseMeta databaseMeta = meta.getDatabaseMeta();
    try {
      if ( data.prepStatementInsert == null ) { // first time: construct prepared statement
        debug = "First: construct prepared statement";

        data.insertRowMeta = new RowMeta();

        /*
         * Construct the SQL statement...
         *
         * INSERT INTO d_test(keyfield, [crcfield,] keylookup[]) VALUES(val_key, [val_crc], row values with keynrs[]) ;
         */

        String sql = "";
        sql += "INSERT INTO " + data.schemaTable + ( "( " );
        boolean comma = false;

        if ( !isAutoIncrement() ) {
          // NO AUTOINCREMENT
          sql += databaseMeta.quoteField( meta.getTechnicalKeyField() );
          data.insertRowMeta.addValueMeta( new ValueMetaInteger( meta.getTechnicalKeyField() ) );
          comma = true;
        } else if ( databaseMeta.needsPlaceHolder() ) {
          sql += "0"; // placeholder on informix! Will be replaced in table by real autoinc value.
          data.insertRowMeta.addValueMeta( new ValueMetaInteger( meta.getTechnicalKeyField() ) );
          comma = true;
        }

        if ( meta.useHash() ) {
          if ( comma ) {
            sql += ", ";
          }
          sql += databaseMeta.quoteField( meta.getHashField() );
          data.insertRowMeta.addValueMeta( new ValueMetaInteger( meta.getHashField() ) );
          comma = true;
        }

        if ( !Utils.isEmpty( meta.getLastUpdateField() ) ) {
          if ( comma ) {
            sql += ", ";
          }
          sql += databaseMeta.quoteField( meta.getLastUpdateField() );
          data.insertRowMeta
            .addValueMeta( new ValueMetaDate( meta.getLastUpdateField() ) );
          comma = true;
        }

        for ( int i = 0; i < meta.getKeyLookup().length; i++ ) {
          if ( comma ) {
            sql += ", ";
          }
          sql += databaseMeta.quoteField( meta.getKeyLookup()[ i ] );
          data.insertRowMeta.addValueMeta( rowMeta.getValueMeta( data.keynrs[ i ] ) );
          comma = true;
        }

        sql += ") VALUES (";

        comma = false;

        if ( !isAutoIncrement() ) {
          sql += '?';
          comma = true;
        }
        if ( meta.useHash() ) {
          if ( comma ) {
            sql += ',';
          }
          sql += '?';
          comma = true;
        }
        if ( !Utils.isEmpty( meta.getLastUpdateField() ) ) {
          if ( comma ) {
            sql += ',';
          }
          sql += '?';
          comma = true;
        }

        for ( int i = 0; i < meta.getKeyLookup().length; i++ ) {
          if ( comma ) {
            sql += ',';
          } else {
            comma = true;
          }
          sql += '?';
        }

        sql += " )";

        String sqlStatement = sql;
        try {
          debug = "First: prepare statement";
          if ( isAutoIncrement() && databaseMeta.supportsAutoGeneratedKeys() ) {
            logDetailed( "SQL with return keys: " + sqlStatement );
            data.prepStatementInsert =
              data.db.getConnection().prepareStatement(
                databaseMeta.stripCR( sqlStatement ), Statement.RETURN_GENERATED_KEYS );
          } else {
            logDetailed( "SQL without return keys: " + sqlStatement );
            data.prepStatementInsert =
              data.db.getConnection().prepareStatement( databaseMeta.stripCR( sqlStatement ) );
          }
        } catch ( SQLException ex ) {
          throw new HopDatabaseException( "Unable to prepare combi insert statement : "
            + Const.CR + sqlStatement, ex );
        } catch ( Exception ex ) {
          throw new HopDatabaseException( "Unable to prepare combi insert statement : "
            + Const.CR + sqlStatement, ex );
        }
      }

      debug = "Create new insert row rins";
      Object[] insertRow = new Object[ data.insertRowMeta.size() ];
      int insertIndex = 0;

      if ( !isAutoIncrement() ) {
        insertRow[ insertIndex ] = valKey;
        insertIndex++;
      }
      if ( meta.useHash() ) {
        insertRow[ insertIndex ] = valCrc;
        insertIndex++;
      }
      if ( !Utils.isEmpty( meta.getLastUpdateField() ) ) {
        insertRow[ insertIndex ] = new Date();
        insertIndex++;
      }
      for ( int i = 0; i < data.keynrs.length; i++ ) {
        insertRow[ insertIndex ] = row[ data.keynrs[ i ] ];
        insertIndex++;
      }

      if ( isRowLevel() ) {
        logRowlevel( "rins=" + data.insertRowMeta.getString( insertRow ) );
      }

      debug = "Set values on insert";
      // INSERT NEW VALUE!
      data.db.setValues( data.insertRowMeta, insertRow, data.prepStatementInsert );

      debug = "Insert row";
      data.db.insertRow( data.prepStatementInsert );

      debug = "Retrieve key";
      if ( isAutoIncrement() && databaseMeta.supportsAutoGeneratedKeys() ) {
        ResultSet keys = null;
        try {
          keys = data.prepStatementInsert.getGeneratedKeys(); // 1 key
          if ( keys.next() ) {
            valKey = new Long( keys.getLong( 1 ) );
          } else {
            throw new HopDatabaseException( "Unable to retrieve auto-increment of combi insert key : "
              + meta.getTechnicalKeyField() + ", no fields in resultset" );
          }
        } catch ( SQLException ex ) {
          throw new HopDatabaseException( "Unable to retrieve auto-increment of combi insert key : "
            + meta.getTechnicalKeyField(), ex );
        } finally {
          try {
            if ( keys != null ) {
              keys.close();
            }
          } catch ( SQLException ex ) {
            throw new HopDatabaseException( "Unable to retrieve auto-increment of combi insert key : "
              + meta.getTechnicalKeyField(), ex );
          }
        }
      }
    } catch ( Exception e ) {
      logError( Const.getStackTracker( e ) );
      throw new HopDatabaseException( "Unexpected error in combination insert in part ["
        + debug + "] : " + e.toString(), e );
    }

    return valKey;
  }

  @Override
  public boolean isRowLevel() {
    return log.isRowLevel();
  }

  @Override
  public boolean init() {
    if ( super.init() ) {
      data.realSchemaName = resolve( meta.getSchemaName() );
      data.realTableName = resolve( meta.getTableName() );

      if ( meta.getCacheSize() > 0 ) {
        data.cache = new HashMap<>( (int) ( meta.getCacheSize() * 1.5 ) );
      } else {
        data.cache = new HashMap<>();
      }
      if ( meta.getDatabaseMeta() == null ) {
        logError( BaseMessages.getString( PKG, "CombinationLookup.Init.ConnectionMissing", getTransformName() ) );
        return false;
      }
      data.db = new Database( this, meta.getDatabaseMeta() );
      data.db.shareWith( this );
      try {
        data.db.connect( getPartitionId() );

        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "CombinationLookup.Log.ConnectedToDB" ) );
        }
        data.db.setCommit( meta.getCommitSize() );

        return true;
      } catch ( HopDatabaseException dbe ) {
        logError( BaseMessages.getString( PKG, "CombinationLookup.Log.UnableToConnectDB" ) + dbe.getMessage() );
      }
    }
    return false;
  }

  @Override
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
      } catch ( HopDatabaseException e ) {
        logError( BaseMessages.getString( PKG, "CombinationLookup.Log.UnexpectedError" ) + " : " + e.toString() );
      } finally {
        data.db.disconnect();
      }
    }

    super.dispose();
  }

  /**
   * Preload the cache
   *
   * @param hashRowMeta The RowMeta of the hashRow
   * @throws HopDatabaseException If something went wrong while selecting the values from the db
   * @throws HopValueException    If something went wrong while adding the data to the cache
   * @throws HopConfigException   If the transform configuration is incomplete
   * @author nwyrwa
   */
  private void preloadCache( IRowMeta hashRowMeta )
    throws HopDatabaseException, HopValueException, HopConfigException {
    // fast exit if no preload cache or no cache
    if ( meta.getPreloadCache() && meta.getCacheSize() >= 0 ) {
      if ( hashRowMeta == null ) {
        throw new HopConfigException( BaseMessages.getString( PKG, "CombinationLookup.Log.UnexpectedError" ) );
      }
      DatabaseMeta databaseMeta = meta.getDatabaseMeta();
      if ( databaseMeta == null ) {
        throw new HopConfigException( BaseMessages.getString( PKG, "CombinationLookup.Log.UnexpectedError" ) );
      }
      String lookupKeys = "";
      String sql = "";
      List<Object[]> cacheValues;

      /* build SQl Statement to preload cache
       *
       * SELECT
       * min(<retval>) as <retval>,
       * key1,
       * key2,
       * key3
       * FROM   <table>
       *
       * GROUP BY key1,
       * key2,
       * key3;
       *
       */

      // Build a string representation of the lookupKeys
      for ( int i = 0; i < meta.getKeyLookup().length; i++ ) {
        lookupKeys += databaseMeta.quoteField( meta.getKeyLookup()[ i ] );

        // No comma after last field
        if ( i < meta.getKeyLookup().length - 1 ) {
          lookupKeys += "," + Const.CR;
        }
      }

      // Use min in case of disambiguation
      sql += "SELECT " + Const.CR;
      sql += "MIN(" + databaseMeta.quoteField( meta.getTechnicalKeyField() ) + ") as "
        + databaseMeta.quoteField( meta.getTechnicalKeyField() ) + "," + Const.CR;
      sql += lookupKeys + Const.CR;
      sql += "FROM " + data.schemaTable + Const.CR;
      sql += "GROUP BY" + Const.CR;
      sql += lookupKeys + Const.CR;

      if ( log.isDebug() ) {
        logDebug( "Using preload cache statement:" + Const.CR + sql );
      }
      cacheValues = data.db.getRows( databaseMeta.stripCR( sql ), meta.getCacheSize() );
      for ( Object[] cacheRow : cacheValues ) {
        // Create a correctly structured array for the cache
        Object[] hashRow = new Object[ data.hashRowMeta.size() ];
        // Assumes the technical key is at position 0 !!
        System.arraycopy( cacheRow, 1, hashRow, 0, hashRow.length );
        // Potential Cache Overflow is ahndled inside
        addToCache( hashRowMeta, hashRow, (Long) cacheRow[ 0 ] );
        incrementLinesInput();
      }

    }

  }


}
