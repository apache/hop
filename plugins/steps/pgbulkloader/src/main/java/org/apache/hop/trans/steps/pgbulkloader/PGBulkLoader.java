/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.trans.steps.pgbulkloader;

//
// The "designer" notes of the PostgreSQL bulkloader:
// ----------------------------------------------
//
// Let's see how fast we can push data down the tube with the use of COPY FROM STDIN
//
//

import com.google.common.annotations.VisibleForTesting;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LoggingObjectInterface;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.trans.Trans;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.BaseStep;
import org.apache.hop.trans.step.StepDataInterface;
import org.apache.hop.trans.step.StepInterface;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.trans.step.StepMetaInterface;
import org.postgresql.PGConnection;
import org.postgresql.copy.PGCopyOutputStream;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Performs a bulk load to a postgres table.
 * <p>
 * Based on (copied from) Sven Boden's Oracle Bulk Loader step
 *
 * @author matt
 * @since 28-mar-2008
 */
public class PGBulkLoader extends BaseStep implements StepInterface {
  private static Class<?> PKG = PGBulkLoaderMeta.class; // for i18n purposes, needed by Translator2!!

  private Charset clientEncoding = Charset.defaultCharset();
  private PGBulkLoaderMeta meta;
  private PGBulkLoaderData data;
  private PGCopyOutputStream pgCopyOut;

  public PGBulkLoader( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
                       Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  /**
   * Get the contents of the control file as specified in the meta object
   *
   * @return a string containing the control file contents
   */
  public String getCopyCommand() throws HopException {
    DatabaseMeta dm = meta.getDatabaseMeta();

    StringBuilder contents = new StringBuilder( 500 );

    String tableName =
      dm.getQuotedSchemaTableCombination(
        environmentSubstitute( meta.getSchemaName() ), environmentSubstitute( meta.getTableName() ) );

    // Set the date style...
    //
    // contents.append("SET DATESTYLE ISO;"); // This is the default but we set it anyway...
    // contents.append(Const.CR);

    // Create a Postgres / Greenplum COPY string for use with a psql client
    contents.append( "COPY " );
    // Table name

    contents.append( tableName );

    // Names of columns

    contents.append( " ( " );

    String[] streamFields = meta.getFieldStream();
    String[] tableFields = meta.getFieldTable();

    if ( streamFields == null || streamFields.length == 0 ) {
      throw new HopException( "No fields defined to load to database" );
    }

    for ( int i = 0; i < streamFields.length; i++ ) {
      if ( i != 0 ) {
        contents.append( ", " );
      }
      contents.append( dm.quoteField( tableFields[ i ] ) );
    }

    contents.append( " ) " );

    // The "FROM" filename
    contents.append( " FROM STDIN" ); // FIFO file

    // The "FORMAT" clause
    contents.append( " WITH CSV DELIMITER AS '" ).append( environmentSubstitute( meta.getDelimiter() ) )
      .append( "' QUOTE AS '" ).append(
      environmentSubstitute( meta.getEnclosure() ) ).append( "'" );
    contents.append( ";" ).append( Const.CR );

    return contents.toString();
  }

  void checkClientEncoding() throws Exception {
    Connection connection = data.db.getConnection();

    Statement statement = connection.createStatement();

    try {
      try ( ResultSet rs = statement.executeQuery( "show client_encoding" ) ) {
        if ( !rs.next() || rs.getMetaData().getColumnCount() != 1 ) {
          logBasic( "Cannot detect client_encoding, using system default encoding" );
          return;
        }

        String clientEncodingStr = rs.getString( 1 );
        logBasic( "Detect client_encoding: " + clientEncodingStr );
        clientEncoding = Charset.forName( clientEncodingStr );
      }
    } catch ( SQLException | IllegalArgumentException ex ) {
      logError( "Cannot detect PostgreSQL client_encoding, using system default encoding", ex );
    } finally {
      statement.close();
    }
  }

  private void do_copy( PGBulkLoaderMeta meta, boolean wait ) throws HopException {
    data.db = getDatabase( this, meta );
    String copyCmd = getCopyCommand();
    try {
      connect();

      checkClientEncoding();

      processTruncate();

      logBasic( "Launching command: " + copyCmd );
      pgCopyOut = new PGCopyOutputStream( (PGConnection) data.db.getConnection(), copyCmd );

    } catch ( Exception ex ) {
      throw new HopException( "Error while preparing the COPY " + copyCmd, ex );
    }
  }

  @VisibleForTesting
  Database getDatabase( LoggingObjectInterface parentObject, PGBulkLoaderMeta pgBulkLoaderMeta ) {
    DatabaseMeta dbMeta = pgBulkLoaderMeta.getDatabaseMeta();
    // If dbNameOverride is present, clone the origin db meta and override the DB name
    String dbNameOverride = environmentSubstitute( pgBulkLoaderMeta.getDbNameOverride() );
    if ( !Utils.isEmpty( dbNameOverride ) ) {
      dbMeta = (DatabaseMeta) pgBulkLoaderMeta.getDatabaseMeta().clone();
      dbMeta.setDBName( dbNameOverride.trim() );
      logDebug( "DB name overridden to the value: " + dbNameOverride );
    }
    return new Database( parentObject, dbMeta );
  }

  void connect() throws HopException {
    if ( getTransMeta().isUsingUniqueConnections() ) {
      synchronized ( getTrans() ) {
        data.db.connect( getTrans().getTransactionId(), getPartitionID() );
      }
    } else {
      data.db.connect( getPartitionID() );
    }
  }

  void processTruncate() throws Exception {
    Connection connection = data.db.getConnection();

    String loadAction = environmentSubstitute( meta.getLoadAction() );

    if ( loadAction.equalsIgnoreCase( "truncate" ) ) {
      DatabaseMeta dm = meta.getDatabaseMeta();
      String tableName =
        dm.getQuotedSchemaTableCombination( environmentSubstitute( meta.getSchemaName() ),
          environmentSubstitute( meta.getTableName() ) );
      logBasic( "Launching command: " + "TRUNCATE " + tableName );

      Statement statement = connection.createStatement();

      try {
        statement.executeUpdate( "TRUNCATE " + tableName );
      } catch ( Exception ex ) {
        throw new HopException( "Error while truncating " + tableName, ex );
      } finally {
        statement.close();
      }
    }
  }

  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws HopException {
    meta = (PGBulkLoaderMeta) smi;
    data = (PGBulkLoaderData) sdi;

    try {
      Object[] r = getRow(); // Get row from input rowset & set row busy!

      if ( r == null ) { // no more input to be expected...

        setOutputDone();

        // Close the output stream...
        // will be null if no records (empty stream)
        if ( data != null && pgCopyOut != null ) {
          pgCopyOut.flush();
          pgCopyOut.endCopy();

        }

        return false;
      }

      if ( first ) {
        first = false;

        // Cache field indexes.
        //
        data.keynrs = new int[ meta.getFieldStream().length ];
        for ( int i = 0; i < data.keynrs.length; i++ ) {
          data.keynrs[ i ] = getInputRowMeta().indexOfValue( meta.getFieldStream()[ i ] );
        }

        // execute the copy statement... pgCopyOut is setup there
        //
        do_copy( meta, true );


        // Write rows of data hereafter...
        //
      }

      writeRowToPostgres( getInputRowMeta(), r );

      putRow( getInputRowMeta(), r );
      incrementLinesOutput();

      return true;
    } catch ( Exception e ) {
      logError( BaseMessages.getString( PKG, "GPBulkLoader.Log.ErrorInStep" ), e );
      setErrors( 1 );
      stopAll();
      setOutputDone(); // signal end to receiver(s)
      return false;
    }
  }

  private void writeRowToPostgres( RowMetaInterface rowMeta, Object[] r ) throws HopException {

    try {
      // So, we have this output stream to which we can write CSV data to.
      // Basically, what we need to do is write the binary data (from strings to it as part of this proof of concept)
      //
      // Let's assume the data is in the correct format here too.
      //
      for ( int i = 0; i < data.keynrs.length; i++ ) {
        if ( i > 0 ) {
          // Write a separator
          //
          pgCopyOut.write( data.separator );
        }

        int index = data.keynrs[ i ];
        ValueMetaInterface valueMeta = rowMeta.getValueMeta( index );
        Object valueData = r[ index ];

        if ( valueData != null ) {
          switch ( valueMeta.getType() ) {
            case ValueMetaInterface.TYPE_STRING:
              pgCopyOut.write( data.quote );

              // No longer dump the bytes for a Lazy Conversion;
              // We need to escape the quote characters in every string
              String quoteStr = new String( data.quote );
              String escapedString = valueMeta.getString( valueData ).replace( quoteStr, quoteStr + quoteStr );
              pgCopyOut.write( escapedString.getBytes( clientEncoding ) );

              pgCopyOut.write( data.quote );
              break;
            case ValueMetaInterface.TYPE_INTEGER:
              if ( valueMeta.isStorageBinaryString() ) {
                pgCopyOut.write( (byte[]) valueData );
              } else {
                pgCopyOut.write( Long.toString( valueMeta.getInteger( valueData ) ).getBytes( clientEncoding ) );
              }
              break;
            case ValueMetaInterface.TYPE_DATE:
              // Format the date in the right format.
              //
              switch ( data.dateFormatChoices[ i ] ) {
                // Pass the data along in the format chosen by the user OR in binary format...
                //
                case PGBulkLoaderMeta.NR_DATE_MASK_PASS_THROUGH:
                  if ( valueMeta.isStorageBinaryString() ) {
                    pgCopyOut.write( (byte[]) valueData );
                  } else {
                    String dateString = valueMeta.getString( valueData );
                    if ( dateString != null ) {
                      pgCopyOut.write( dateString.getBytes( clientEncoding ) );
                    }
                  }
                  break;

                // Convert to a "YYYY-MM-DD" format
                //
                case PGBulkLoaderMeta.NR_DATE_MASK_DATE:
                  String dateString = data.dateMeta.getString( valueMeta.getDate( valueData ) );
                  if ( dateString != null ) {
                    pgCopyOut.write( dateString.getBytes( clientEncoding ) );
                  }
                  break;

                // Convert to a "YYYY-MM-DD HH:MM:SS.mmm" format
                //
                case PGBulkLoaderMeta.NR_DATE_MASK_DATETIME:
                  String dateTimeString = data.dateTimeMeta.getString( valueMeta.getDate( valueData ) );
                  if ( dateTimeString != null ) {
                    pgCopyOut.write( dateTimeString.getBytes( clientEncoding ) );
                  }
                  break;

                default:
                  throw new HopException( "PGBulkLoader doesn't know how to handle date (neither passthrough, nor date or datetime for field " + valueMeta.getName() );
              }
              break;
            case ValueMetaInterface.TYPE_TIMESTAMP:
              // Format the date in the right format.
              //
              switch ( data.dateFormatChoices[ i ] ) {
                // Pass the data along in the format chosen by the user OR in binary format...
                //
                case PGBulkLoaderMeta.NR_DATE_MASK_PASS_THROUGH:
                  if ( valueMeta.isStorageBinaryString() ) {
                    pgCopyOut.write( (byte[]) valueData );
                  } else {
                    String dateString = valueMeta.getString( valueData );
                    if ( dateString != null ) {
                      pgCopyOut.write( dateString.getBytes( clientEncoding ) );
                    }
                  }
                  break;

                // Convert to a "YYYY-MM-DD" format
                //
                case PGBulkLoaderMeta.NR_DATE_MASK_DATE:
                  String dateString = data.dateMeta.getString( valueMeta.getDate( valueData ) );
                  if ( dateString != null ) {
                    pgCopyOut.write( dateString.getBytes( clientEncoding ) );
                  }
                  break;

                // Convert to a "YYYY-MM-DD HH:MM:SS.mmm" format
                //
                case PGBulkLoaderMeta.NR_DATE_MASK_DATETIME:
                  String dateTimeString = data.dateTimeMeta.getString( valueMeta.getDate( valueData ) );
                  if ( dateTimeString != null ) {
                    pgCopyOut.write( dateTimeString.getBytes( clientEncoding ) );
                  }
                  break;

                default:
                  throw new HopException( "PGBulkLoader doesn't know how to handle timestamp (neither passthrough, nor date or datetime for field " + valueMeta.getName() );
              }
              break;
            case ValueMetaInterface.TYPE_BOOLEAN:
              if ( valueMeta.isStorageBinaryString() ) {
                pgCopyOut.write( (byte[]) valueData );
              } else {
                pgCopyOut.write( Double.toString( valueMeta.getNumber( valueData ) ).getBytes( clientEncoding ) );
              }
              break;
            case ValueMetaInterface.TYPE_NUMBER:
              if ( valueMeta.isStorageBinaryString() ) {
                pgCopyOut.write( (byte[]) valueData );
              } else {
                pgCopyOut.write( Double.toString( valueMeta.getNumber( valueData ) ).getBytes( clientEncoding ) );
              }
              break;
            case ValueMetaInterface.TYPE_BIGNUMBER:
              if ( valueMeta.isStorageBinaryString() ) {
                pgCopyOut.write( (byte[]) valueData );
              } else {
                BigDecimal big = valueMeta.getBigNumber( valueData );
                if ( big != null ) {
                  pgCopyOut.write( big.toString().getBytes( clientEncoding ) );
                }
              }
              break;
            default:
              throw new HopException( "PGBulkLoader doesn't handle the type " + valueMeta.getTypeDesc() );
          }
        }
      }

      // Now write a newline
      //
      pgCopyOut.write( data.newline );
    } catch ( Exception e ) {
      throw new HopException( "Error serializing rows of data to the COPY command", e );
    }

  }

  protected void verifyDatabaseConnection() throws HopException {
    // Confirming Database Connection is defined.
    if ( meta.getDatabaseMeta() == null ) {
      throw new HopException( BaseMessages.getString( PKG, "PGBulkLoaderMeta.GetSQL.NoConnectionDefined" ) );
    }
  }

  public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
    meta = (PGBulkLoaderMeta) smi;
    data = (PGBulkLoaderData) sdi;

    String enclosure = environmentSubstitute( meta.getEnclosure() );
    String separator = environmentSubstitute( meta.getDelimiter() );

    if ( super.init( smi, sdi ) ) {

      // Confirming Database Connection is defined.
      try {
        verifyDatabaseConnection();
      } catch ( HopException ex ) {
        logError( ex.getMessage() );
        return false;
      }

      if ( enclosure != null ) {
        data.quote = enclosure.getBytes();
      } else {
        data.quote = new byte[] {};
      }
      if ( separator != null ) {
        data.separator = separator.getBytes();
      } else {
        data.separator = new byte[] {};
      }
      data.newline = Const.CR.getBytes();

      data.dateFormatChoices = new int[ meta.getFieldStream().length ];
      for ( int i = 0; i < data.dateFormatChoices.length; i++ ) {
        if ( Utils.isEmpty( meta.getDateMask()[ i ] ) ) {
          data.dateFormatChoices[ i ] = PGBulkLoaderMeta.NR_DATE_MASK_PASS_THROUGH;
        } else if ( meta.getDateMask()[ i ].equalsIgnoreCase( PGBulkLoaderMeta.DATE_MASK_DATE ) ) {
          data.dateFormatChoices[ i ] = PGBulkLoaderMeta.NR_DATE_MASK_DATE;
        } else if ( meta.getDateMask()[ i ].equalsIgnoreCase( PGBulkLoaderMeta.DATE_MASK_DATETIME ) ) {
          data.dateFormatChoices[ i ] = PGBulkLoaderMeta.NR_DATE_MASK_DATETIME;
        } else { // The default : just pass it along...
          data.dateFormatChoices[ i ] = PGBulkLoaderMeta.NR_DATE_MASK_PASS_THROUGH;
        }

      }
      return true;
    }
    return false;
  }

}
