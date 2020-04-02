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

package org.apache.hop.pipeline.transforms.orabulkloader;

//
// The "designer" notes of the Oracle bulkloader:
// ----------------------------------------------
//
// - "Enclosed" is used in the loader instead of "optionally enclosed" as optionally
//   encloses kind of destroys the escaping.
// - A Boolean is output as Y and N (as in the text output transform e.g.). If people don't
//   like this they can first convert the boolean value to something else before loading
//   it.
// - Filters (besides data and datetime) are not supported as it slows down.
//
//

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.vfs.HopVFS;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformDataInterface;
import org.apache.hop.pipeline.transform.TransformInterface;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaInterface;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

/**
 * Performs a bulk load to an oracle table.
 *
 * @author Sven Boden
 * @since 20-feb-2007
 */
public class OraBulkLoader extends BaseTransform implements TransformInterface {
  private static Class<?> PKG = OraBulkLoaderMeta.class; // for i18n purposes, needed by Translator!!

  public static final int EX_SUCC = 0;

  public static final int EX_WARN = 2;

  Process sqlldrProcess = null;

  private OraBulkLoaderMeta meta;
  protected OraBulkLoaderData data;
  private OraBulkDataOutput output = null;

  /*
   * Local copy of the pipeline "preview" property. We only forward the rows upon previewing, we don't do any of
   * the real stuff.
   */
  private boolean preview = false;

  //
  // This class continually reads from the stream, and sends it to the log
  // if the logging level is at least basic level.
  //
  private final class StreamLogger extends Thread {
    private InputStream input;
    private String type;

    StreamLogger( InputStream is, String type ) {
      this.input = is;
      this.type = type + ">";
    }

    public void run() {
      try {
        final BufferedReader br = new BufferedReader( new InputStreamReader( input ) );
        String line;
        while ( ( line = br.readLine() ) != null ) {
          // Only perform the concatenation if at basic level. Otherwise,
          // this just reads from the stream.
          if ( log.isBasic() ) {
            logBasic( type + line );
          }
        }
      } catch ( IOException ioe ) {
        ioe.printStackTrace();
      }

    }

  }

  public OraBulkLoader( TransformMeta transformMeta, TransformDataInterface transformDataInterface, int copyNr, PipelineMeta pipelineMeta,
                        Pipeline pipeline ) {
    super( transformMeta, transformDataInterface, copyNr, pipelineMeta, pipeline );
  }

  private String substituteRecordTerminator( String terminator ) {
    final StringBuilder in = new StringBuilder();
    int length;
    boolean escaped = false;

    terminator = environmentSubstitute( terminator );
    length = terminator.length();
    for ( int i = 0; i < length; i++ ) {
      final char c = terminator.charAt( i );

      if ( escaped ) {
        switch ( c ) {
          case 'n':
            in.append( '\n' );
            break;
          case 'r':
            in.append( '\r' );
            break;
          default:
            in.append( c );
            break;
        }
        escaped = false;
      } else if ( c == '\\' ) {
        escaped = true;
      } else {
        in.append( c );
      }
    }

    return in.toString();
  }

  private String encodeRecordTerminator( String terminator, String encoding ) throws HopException {
    final String in = substituteRecordTerminator( terminator );
    final StringBuilder out = new StringBuilder();
    byte[] bytes;

    try {
      // use terminator in hex representation due to character set
      // terminator in hex representation must be in character set
      // of data file
      if ( Utils.isEmpty( encoding ) ) {
        bytes = in.getBytes();
      } else {
        bytes = in.getBytes( encoding );
      }
      for ( byte aByte : bytes ) {
        final String hex = Integer.toHexString( aByte );

        if ( hex.length() == 1 ) {
          out.append( '0' );
        }
        out.append( hex );
      }
    } catch ( UnsupportedEncodingException e ) {
      throw new HopException( "Unsupported character encoding: " + encoding, e );
    }

    return out.toString();
  }

  /**
   * Get the contents of the control file as specified in the meta object
   *
   * @param meta the meta object to model the control file after
   * @return a string containing the control file contents
   */
  public String getControlFileContents( OraBulkLoaderMeta meta, RowMetaInterface rm, Object[] r ) throws HopException {
    DatabaseMeta dm = meta.getDatabaseMeta();
    String inputName = "'" + getFilename( getFileObject( meta.getDataFile(), getPipelineMeta() ) ) + "'";

    String loadAction = meta.getLoadAction();

    StringBuilder contents = new StringBuilder( 500 );
    contents.append( "OPTIONS(" ).append( Const.CR );
    contents.append( "  ERRORS=\'" ).append( meta.getMaxErrors() ).append( "\'" ).append( Const.CR );

    if ( meta.getCommitSizeAsInt( this ) != 0 && !( meta.isDirectPath() && getTransformMeta().getCopies() > 1 ) ) {
      // For the second part of the above expressions: ROWS is not supported
      // in parallel mode (by sqlldr).
      contents.append( "  , ROWS=\'" ).append( meta.getCommitSize() ).append( "\'" ).append( Const.CR );
    }

    if ( meta.getBindSizeAsInt( this ) != 0 ) {
      contents.append( "  , BINDSIZE=\'" ).append( meta.getBindSize() ).append( "\'" ).append( Const.CR );
    }

    if ( meta.getReadSizeAsInt( this ) != 0 ) {
      contents.append( "  , READSIZE=\'" ).append( meta.getReadSize() ).append( "\'" ).append( Const.CR );
    }

    contents.append( ")" ).append( Const.CR );

    contents.append( "LOAD DATA" ).append( Const.CR );
    if ( !Utils.isEmpty( meta.getCharacterSetName() ) ) {
      contents.append( "CHARACTERSET " ).append( meta.getCharacterSetName() ).append( Const.CR );
    }
    if ( !OraBulkLoaderMeta.METHOD_AUTO_CONCURRENT.equals( meta.getLoadMethod() )
      || !Utils.isEmpty( meta.getAltRecordTerm() ) ) {
      String infile = inputName;

      if ( OraBulkLoaderMeta.METHOD_AUTO_CONCURRENT.equals( meta.getLoadMethod() ) ) {
        infile = "''";
      }

      // For concurrent input, data command line argument must be specified
      contents.append( "INFILE " ).append( infile );
      if ( !Utils.isEmpty( meta.getAltRecordTerm() ) ) {
        contents.append( " \"STR x'" ).append(
          encodeRecordTerminator( meta.getAltRecordTerm(), meta.getEncoding() ) ).append( "'\"" );
      }
      contents.append( Const.CR );
    }
    contents
      .append( "INTO TABLE " ).append(
      dm.getQuotedSchemaTableCombination(
        environmentSubstitute( meta.getSchemaName() ), environmentSubstitute( meta.getTableName() ) ) )
      .append( Const.CR ).append( loadAction ).append( Const.CR ).append(
      "FIELDS TERMINATED BY ',' ENCLOSED BY '\"'" ).append( Const.CR ).append( "(" );

    String[] streamFields = meta.getFieldStream();
    String[] tableFields = meta.getFieldTable();
    String[] dateMask = meta.getDateMask();

    if ( streamFields == null || streamFields.length == 0 ) {
      throw new HopException( "No fields defined to load to database" );
    }

    for ( int i = 0; i < streamFields.length; i++ ) {
      if ( i != 0 ) {
        contents.append( ", " ).append( Const.CR );
      }
      contents.append( dm.quoteField( tableFields[ i ] ) );

      int pos = rm.indexOfValue( streamFields[ i ] );
      if ( pos < 0 ) {
        throw new HopException( "Could not find field " + streamFields[ i ] + " in stream" );
      }
      ValueMetaInterface v = rm.getValueMeta( pos );
      switch ( v.getType() ) {
        case ValueMetaInterface.TYPE_STRING:
          if ( v.getLength() > 255 ) {
            contents.append( " CHAR(" ).append( v.getLength() ).append( ")" );
          } else {
            contents.append( " CHAR" );
          }
          break;
        case ValueMetaInterface.TYPE_INTEGER:
        case ValueMetaInterface.TYPE_NUMBER:
        case ValueMetaInterface.TYPE_BIGNUMBER:
          break;
        case ValueMetaInterface.TYPE_DATE:
          if ( OraBulkLoaderMeta.DATE_MASK_DATE.equals( dateMask[ i ] ) ) {
            contents.append( " DATE 'yyyy-mm-dd'" );
          } else if ( OraBulkLoaderMeta.DATE_MASK_DATETIME.equals( dateMask[ i ] ) ) {
            contents.append( " TIMESTAMP 'yyyy-mm-dd hh24:mi:ss.ff'" );
          } else {
            // If not specified the default is date.
            contents.append( " DATE 'yyyy-mm-dd'" );
          }
          break;
        case ValueMetaInterface.TYPE_BINARY:
          contents.append( " ENCLOSED BY '<startlob>' AND '<endlob>'" );
          break;
        case ValueMetaInterface.TYPE_TIMESTAMP:
          contents.append( " TIMESTAMP 'yyyy-mm-dd hh24:mi:ss.ff'" );
          break;
        default:
          break;
      }
    }
    contents.append( ")" );

    return contents.toString();
  }

  /**
   * Create a control file.
   *
   * @param filename path to control file
   * @param meta     transform meta
   * @throws HopException
   */
  public void createControlFile( String filename, Object[] row, OraBulkLoaderMeta meta ) throws HopException {
    FileWriter fw = null;

    try {
      File controlFile = new File( getFileObject( filename, getPipelineMeta() ).getURL().getFile() );
      // Need to ensure that the parent directory they set exists for the control file.
      controlFile.getParentFile().mkdirs();
      controlFile.createNewFile();
      fw = new FileWriter( controlFile );
      fw.write( getControlFileContents( meta, getInputRowMeta(), row ) );
    } catch ( IOException ex ) {
      throw new HopException( ex.getMessage(), ex );
    } finally {
      try {
        if ( fw != null ) {
          fw.close();
        }
      } catch ( Exception ex ) {
        // Ignore errors
      }
    }
  }

  /**
   * Create the command line for an sqlldr process depending on the meta information supplied.
   *
   * @param meta     The meta data to create the command line from
   * @param password Use the real password or not
   * @return The string to execute.
   * @throws HopException Upon any exception
   */
  public String createCommandLine( OraBulkLoaderMeta meta, boolean password ) throws HopException {
    StringBuilder sb = new StringBuilder( 300 );

    if ( meta.getSqlldr() != null ) {
      try {
        FileObject fileObject =
          getFileObject( meta.getSqlldr(), getPipelineMeta() );
        String sqlldr = getFilename( fileObject );
        sb.append( sqlldr );
      } catch ( HopFileException ex ) {
        throw new HopException( "Error retrieving sqlldr string", ex );
      }
    } else {
      throw new HopException( "No sqlldr application specified" );
    }

    if ( meta.getControlFile() != null ) {
      try {
        FileObject fileObject =
          getFileObject( meta.getControlFile(), getPipelineMeta() );

        sb.append( " control=\'" );
        sb.append( getFilename( fileObject ) );
        sb.append( "\'" );
      } catch ( HopFileException ex ) {
        throw new HopException( "Error retrieving controlfile string", ex );
      }
    } else {
      throw new HopException( "No control file specified" );
    }

    if ( OraBulkLoaderMeta.METHOD_AUTO_CONCURRENT.equals( meta.getLoadMethod() ) ) {
      sb.append( " data=\'-\'" );
    }

    if ( meta.getLogFile() != null ) {
      try {
        FileObject fileObject =
          getFileObject( meta.getLogFile(), getPipelineMeta() );

        sb.append( " log=\'" );
        sb.append( getFilename( fileObject ) );
        sb.append( "\'" );
      } catch ( HopFileException ex ) {
        throw new HopException( "Error retrieving logfile string", ex );
      }
    }

    if ( meta.getBadFile() != null ) {
      try {
        FileObject fileObject =
          getFileObject( meta.getBadFile(), getPipelineMeta() );

        sb.append( " bad=\'" );
        sb.append( getFilename( fileObject ) );
        sb.append( "\'" );
      } catch ( HopFileException ex ) {
        throw new HopException( "Error retrieving badfile string", ex );
      }
    }

    if ( meta.getDiscardFile() != null ) {
      try {
        FileObject fileObject =
          getFileObject( meta.getDiscardFile(), getPipelineMeta() );

        sb.append( " discard=\'" );
        sb.append( getFilename( fileObject ) );
        sb.append( "\'" );
      } catch ( HopFileException ex ) {
        throw new HopException( "Error retrieving discardfile string", ex );
      }
    }

    DatabaseMeta dm = meta.getDatabaseMeta();
    if ( dm != null ) {
      String user = Const.NVL( dm.getUsername(), "" );
      String pass =
        Const.NVL( Encr.decryptPasswordOptionallyEncrypted( environmentSubstitute( dm.getPassword() ) ), "" );
      if ( !password ) {
        pass = "******";
      }
      String dns = Const.NVL( dm.getDatabaseName(), "" );
      sb.append( " userid=" ).append( environmentSubstitute( user ) ).append( "/" ).append(
        environmentSubstitute( pass ) ).append( "@" );

      String overrideName = meta.getDbNameOverride();
      if ( Utils.isEmpty( Const.rtrim( overrideName ) ) ) {
        sb.append( environmentSubstitute( dns ) );
      } else {
        // if the database name override is filled in, do that one.
        sb.append( environmentSubstitute( overrideName ) );
      }
    } else {
      throw new HopException( "No connection specified" );
    }

    if ( meta.isDirectPath() ) {
      sb.append( " DIRECT=TRUE" );

      if ( getTransformMeta().getCopies() > 1 || meta.isParallel() ) {
        sb.append( " PARALLEL=TRUE" );
      }
    }

    return sb.toString();
  }

  public void checkExitVal( int exitVal ) throws HopException {
    if ( exitVal == EX_SUCC ) {
      return;
    }

    if ( meta.isFailOnWarning() && ( exitVal == EX_WARN ) ) {
      throw new HopException( "sqlldr returned warning" );
    } else if ( meta.isFailOnError() && ( exitVal != EX_WARN ) ) {
      throw new HopException( "sqlldr returned an error (exit code " + exitVal + ")" );
    }
  }

  public boolean execute( OraBulkLoaderMeta meta, boolean wait ) throws HopException {
    Runtime rt = Runtime.getRuntime();

    try {
      sqlldrProcess = rt.exec( createCommandLine( meta, true ) );
      // any error message?
      StreamLogger errorLogger = new StreamLogger( sqlldrProcess.getErrorStream(), "ERROR" );

      // any output?
      StreamLogger outputLogger = new StreamLogger( sqlldrProcess.getInputStream(), "OUTPUT" );

      // kick them off
      errorLogger.start();
      outputLogger.start();

      if ( wait ) {
        // any error???
        int exitVal = sqlldrProcess.waitFor();
        sqlldrProcess = null;
        logBasic( BaseMessages.getString( PKG, "OraBulkLoader.Log.ExitValueSqlldr", "" + exitVal ) );
        checkExitVal( exitVal );
      }
    } catch ( Exception ex ) {
      // Don't throw the message upwards, the message contains the password.
      throw new HopException( "Error while executing sqlldr \'" + createCommandLine( meta, false ) + "\'" );
    }

    return true;
  }

  public boolean processRow( TransformMetaInterface smi, TransformDataInterface sdi ) throws HopException {
    meta = (OraBulkLoaderMeta) smi;
    data = (OraBulkLoaderData) sdi;

    try {
      Object[] r = getRow(); // Get row from input rowset & set row busy!
      if ( r == null ) {
        // no more input to be expected...

        setOutputDone();

        if ( !preview ) {
          if ( output != null ) {
            // Close the output
            try {
              output.close();
            } catch ( IOException e ) {
              throw new HopException( "Error while closing output", e );
            }

            output = null;
          }

          String loadMethod = meta.getLoadMethod();
          if ( OraBulkLoaderMeta.METHOD_AUTO_END.equals( loadMethod ) ) {
            // if this is the first line, we do not need to execute loader
            // control file may not exists
            if ( !first ) {
              execute( meta, true );
              sqlldrProcess = null;
            }
          } else if ( OraBulkLoaderMeta.METHOD_AUTO_CONCURRENT.equals( meta.getLoadMethod() ) ) {
            try {
              if ( sqlldrProcess != null ) {
                int exitVal = sqlldrProcess.waitFor();
                sqlldrProcess = null;
                logBasic( BaseMessages.getString( PKG, "OraBulkLoader.Log.ExitValueSqlldr", "" + exitVal ) );
                checkExitVal( exitVal );
              } else if ( !first ) {
                throw new HopException( "Internal error: no sqlldr process running" );
              }
            } catch ( Exception ex ) {
              throw new HopException( "Error while executing sqlldr", ex );
            }
          }
        }
        return false;
      }

      if ( !preview ) {
        if ( first ) {
          first = false;

          String recTerm = Const.CR;
          if ( !Utils.isEmpty( meta.getAltRecordTerm() ) ) {
            recTerm = substituteRecordTerminator( meta.getAltRecordTerm() );
          }

          createControlFile( environmentSubstitute( meta.getControlFile() ), r, meta );
          output = new OraBulkDataOutput( meta, recTerm );

          if ( OraBulkLoaderMeta.METHOD_AUTO_CONCURRENT.equals( meta.getLoadMethod() ) ) {
            execute( meta, false );
          }
          output.open( this, sqlldrProcess );
        }
        output.writeLine( getInputRowMeta(), r );
      }
      putRow( getInputRowMeta(), r );
      incrementLinesOutput();

    } catch ( HopException e ) {
      logError( BaseMessages.getString( PKG, "OraBulkLoader.Log.ErrorInTransform" ) + e.getMessage() );
      setErrors( 1 );
      stopAll();
      setOutputDone(); // signal end to receiver(s)
      return false;
    }

    return true;
  }

  public boolean init( TransformMetaInterface smi, TransformDataInterface sdi ) {
    meta = (OraBulkLoaderMeta) smi;
    data = (OraBulkLoaderData) sdi;

    Pipeline pipeline = getPipeline();
    preview = pipeline.isPreview();

    return super.init( smi, sdi );
  }

  public void dispose( TransformMetaInterface smi, TransformDataInterface sdi ) {
    meta = (OraBulkLoaderMeta) smi;
    data = (OraBulkLoaderData) sdi;

    super.dispose( smi, sdi );

    // close output stream (may terminate running sqlldr)
    if ( output != null ) {
      // Close the output
      try {
        output.close();
      } catch ( IOException e ) {
        logError( "Error while closing output", e );
      }

      output = null;
    }
    // running sqlldr process must be terminated
    if ( sqlldrProcess != null ) {
      try {
        int exitVal = sqlldrProcess.waitFor();
        sqlldrProcess = null;
        logBasic( BaseMessages.getString( PKG, "OraBulkLoader.Log.ExitValueSqlldr", "" + exitVal ) );
      } catch ( InterruptedException e ) {
        /* process should be destroyed */
        e.printStackTrace();
        if ( sqlldrProcess != null ) {
          sqlldrProcess.destroy();
        }
      }
    }

    if ( !preview && meta.isEraseFiles() ) {
      // Erase the created cfg/dat files if requested. We don't erase
      // the rest of the files because it would be "stupid" to erase them
      // right after creation. If you don't want them, don't fill them in.
      FileObject fileObject = null;

      String method = meta.getLoadMethod();
      // OraBulkLoaderMeta.METHOD_AUTO_CONCURRENT.equals(method) ||
      if ( OraBulkLoaderMeta.METHOD_AUTO_END.equals( method ) ) {
        if ( meta.getControlFile() != null ) {
          try {
            fileObject = getFileObject( meta.getControlFile(), getPipelineMeta() );
            fileObject.delete();
            fileObject.close();
          } catch ( Exception ex ) {
            logError( "Error deleting control file \'"
              + getFilename( fileObject ) + "\': " + ex.getMessage(), ex );
          }
        }
      }

      if ( OraBulkLoaderMeta.METHOD_AUTO_END.equals( method ) ) {
        // In concurrent mode the data is written to the control file.
        if ( meta.getDataFile() != null ) {
          try {
            fileObject = getFileObject( meta.getDataFile(), getPipelineMeta() );
            fileObject.delete();
            fileObject.close();
          } catch ( Exception ex ) {
            logError( "Error deleting data file \'"
              + getFilename( fileObject ) + "\': " + ex.getMessage(), ex );
          }
        }
      }

      if ( OraBulkLoaderMeta.METHOD_MANUAL.equals( method ) ) {
        logBasic( "Deletion of files is not compatible with \'manual load method\'" );
      }
    }
  }

  @VisibleForTesting
  String getFilename( FileObject fileObject ) {
    return HopVFS.getFilename( fileObject );
  }

  @VisibleForTesting
  FileObject getFileObject( String vfsFilename, VariableSpace space ) throws HopFileException {
    return HopVFS.getFileObject( environmentSubstitute( vfsFilename ), space );
  }
}
