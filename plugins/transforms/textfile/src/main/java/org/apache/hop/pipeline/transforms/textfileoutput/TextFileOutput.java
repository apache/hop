/*******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
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

package org.apache.hop.pipeline.transforms.textfileoutput;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.WriterOutputStream;
import org.apache.hop.core.compress.CompressionOutputStream;
import org.apache.hop.core.compress.ICompressionProvider;
import org.apache.hop.core.compress.CompressionProviderFactory;
import org.apache.hop.core.compress.zip.ZipCompressionProvider;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.fileinput.CharsetToolkit;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Converts input rows to text and then writes this text to one or more files.
 *
 * @author Matt
 * @since 4-apr-2003
 */
public class TextFileOutput<Meta extends TextFileOutputMeta, Data extends TextFileOutputData>
  extends BaseTransform<Meta, Data>
  implements ITransform<Meta, Data> {

  private static final Class<?> PKG = TextFileOutputMeta.class; // for i18n purposes, needed by Translator!!

  private static final String FILE_COMPRESSION_TYPE_NONE =
    TextFileOutputMeta.fileCompressionTypeCodes[ TextFileOutputMeta.FILE_COMPRESSION_TYPE_NONE ];
  private static final boolean COMPATIBILITY_APPEND_NO_HEADER = "Y".equals(
    Const.NVL( System.getProperty( Const.HOP_COMPATIBILITY_TEXT_FILE_OUTPUT_APPEND_NO_HEADER ), "N" ) );

  public TextFileOutput( TransformMeta transformMeta, Meta meta, Data data, int copyNr, PipelineMeta pipelineMeta,
                         Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  private void initFieldNumbers( IRowMeta outputRowMeta, TextFileField[] outputFields ) throws HopException {
    data.fieldnrs = new int[ outputFields.length ];
    for ( int i = 0; i < outputFields.length; i++ ) {
      data.fieldnrs[ i ] = outputRowMeta.indexOfValue( outputFields[ i ].getName() );
      if ( data.fieldnrs[ i ] < 0 ) {
        throw new HopTransformException( "Field [" + outputFields[ i ].getName()
          + "] couldn't be found in the input stream!" );
      }
    }
  }

  public boolean isFileExists( String filename ) throws HopException {
    try {
      return getFileObject( filename, getPipelineMeta() ).exists();
    } catch ( Exception e ) {
      throw new HopException( "Error opening new file : " + e.toString() );
    }
  }

  private ICompressionProvider getCompressionProvider() throws HopException {
    String compressionType = meta.getFileCompression();
    if ( Utils.isEmpty( compressionType ) ) {
      compressionType = FILE_COMPRESSION_TYPE_NONE;
    }
    ICompressionProvider compressionProvider = CompressionProviderFactory.getInstance().getCompressionProviderByName( compressionType );

    if ( compressionProvider == null ) {
      throw new HopException( "No compression provider found with name = " + compressionType );
    }

    if ( !compressionProvider.supportsOutput() ) {
      throw new HopException( "Compression provider " + compressionType + " does not support output streams!" );
    }
    return compressionProvider;
  }

  private void initServletStreamWriter() throws HopException {
    data.writer = null;
    try {
      if (getPipeline() instanceof Pipeline) {


        Writer writer = ((Pipeline)getPipeline()).getServletPrintWriter();
        if ( Utils.isEmpty( meta.getEncoding() ) ) {
          data.writer = new WriterOutputStream( writer );
        } else {
          data.writer = new WriterOutputStream( writer, meta.getEncoding() );
        }
      }
    } catch ( Exception e ) {
      throw new HopException( "Error opening new file : " + e.toString() );
    }
  }

  public void initFileStreamWriter( String filename ) throws HopException {
    data.writer = null;
    try {
      TextFileOutputData.FileStream fileStreams = null;

      try {
        if ( data.splitEvery > 0 ) {
          if ( filename.equals( data.getFileStreamsCollection().getLastFileName() ) ) {
            fileStreams = data.getFileStreamsCollection().getLastStream();
          }
        } else {
          fileStreams = data.getFileStreamsCollection().getStream( filename );
        }

        boolean writingToFileForFirstTime = fileStreams == null;

        if ( writingToFileForFirstTime ) { // Opening file for first time

          if ( meta.isAddToResultFiles() ) {
            // Add this to the result file names...
            ResultFile resultFile =
              new ResultFile( ResultFile.FILE_TYPE_GENERAL, getFileObject( filename, getPipelineMeta() ),
                getPipelineMeta().getName(), getTransformName() );
            resultFile.setComment( BaseMessages.getString( PKG, "TextFileOutput.AddResultFile" ) );
            addResultFile( resultFile );
          }

          ICompressionProvider compressionProvider = getCompressionProvider();
          boolean isZipFile = compressionProvider instanceof ZipCompressionProvider;
          boolean createParentDirIfNotExists = meta.isCreateParentFolder();
          boolean appendToExistingFile = meta.isFileAppended();

          if ( appendToExistingFile && isZipFile && isFileExists( filename ) ) {
            throw new HopException( "Can not append to an existing zip file : " + filename );
          }

          int maxOpenFiles = getMaxOpenFiles();
          if ( ( maxOpenFiles > 0 ) && ( data.getFileStreamsCollection().getNumOpenFiles() >= maxOpenFiles ) ) {
            // If the file we're going to close is a zip file,  going to remove it from the collection of files
            // that have been opened. We do this because it is not possible to reopen a
            // zip file for append. By removing it from the collection, if the same file is referenced later, it will look
            // like we're opening the file for the first time, and if we're set up to append to existing files it will cause and
            // exception to be thrown, which is the desired result.
            data.getFileStreamsCollection().closeOldestOpenFile( isZipFile );
          }

          if ( createParentDirIfNotExists && ( ( data.getFileStreamsCollection().size() == 0 ) || meta.isFileNameInField() ) ) {
            createParentFolder( filename );
          }
          if ( log.isDetailed() ) {
            logDetailed( "Opening output stream using provider: " + compressionProvider.getName() );
          }

          OutputStream fileOutputStream =
            getOutputStream( filename, getPipelineMeta(), !isZipFile && appendToExistingFile );
          CompressionOutputStream compressionOutputStream = compressionProvider.createOutputStream( fileOutputStream );

          // The compression output stream may also archive entries. For this we create the filename
          // (with appropriate extension) and add it as an entry to the output stream. For providers
          // that do not archive entries, they should use the default no-op implementation.
          compressionOutputStream.addEntry( filename, environmentSubstitute( meta.getExtension() ) );

          if ( log.isDetailed() ) {
            if ( !Utils.isEmpty( meta.getEncoding() ) ) {
              logDetailed( "Opening output stream in encoding: " + meta.getEncoding() );
            } else {
              logDetailed( "Opening output stream in default encoding" );
            }
          }

          BufferedOutputStream bufferedOutputStream = new BufferedOutputStream( compressionOutputStream, 5000 );

          fileStreams = data.new FileStream( fileOutputStream, compressionOutputStream, bufferedOutputStream );

          data.getFileStreamsCollection().add( filename, fileStreams );

          if ( log.isDetailed() ) {
            logDetailed( "Opened new file with name [" + HopVfs.getFriendlyURI( filename ) + "]" );
          }
        } else if ( fileStreams.getBufferedOutputStream() == null ) { // File was previously opened and now needs to be reopened.
          int maxOpenFiles = getMaxOpenFiles();
          if ( ( maxOpenFiles > 0 ) && ( data.getFileStreamsCollection().getNumOpenFiles() >= maxOpenFiles ) ) {
            data.getFileStreamsCollection().closeOldestOpenFile( false );
          }

          OutputStream fileOutputStream = getOutputStream( filename, getPipelineMeta(), true );
          ICompressionProvider compressionProvider = getCompressionProvider();
          CompressionOutputStream compressionOutputStream = compressionProvider.createOutputStream( fileOutputStream );
          compressionOutputStream.addEntry( filename, environmentSubstitute( meta.getExtension() ) );
          BufferedOutputStream bufferedOutputStream = new BufferedOutputStream( compressionOutputStream, 5000 );

          fileStreams.setFileOutputStream( fileOutputStream );
          fileStreams.setCompressedOutputStream( compressionOutputStream );
          fileStreams.setBufferedOutputStream( bufferedOutputStream );
        }
      } catch ( Exception e ) {
        if ( !( e instanceof HopException ) ) {
          throw new HopException( "Error opening new file : " + e.toString() );
        } else {
          throw (HopException) e;
        }
      }

      fileStreams.setDirty( true );

      data.fos = fileStreams.getFileOutputStream();
      data.out = fileStreams.getCompressedOutputStream();
      data.writer = fileStreams.getBufferedOutputStream();
    } catch ( HopException ke ) {
      throw ke;
    } catch ( Exception e ) {
      throw new HopException( "Error opening new file : " + e.toString() );
    }
  }

  public String getOutputFileName( Object[] row ) throws HopException {
    String filename = null;
    if ( row == null ) {
      if ( data.writer != null ) {
        filename = data.getFileStreamsCollection().getLastFileName();
      } else {
        filename = meta.getFileName();
        if ( filename == null ) {
          throw new HopFileException( BaseMessages.getString( PKG, "TextFileOutput.Exception.FileNameNotSet" ) );
        }
        filename = buildFilename( environmentSubstitute( filename ), true );
      }
    } else {
      data.fileNameFieldIndex = getInputRowMeta().indexOfValue( meta.getFileNameField() );
      if ( data.fileNameFieldIndex < 0 ) {
        throw new HopTransformException( BaseMessages.getString( PKG, "TextFileOutput.Exception.FileNameFieldNotFound", meta.getFileNameField() ) );
      }
      data.fileNameMeta = getInputRowMeta().getValueMeta( data.fileNameFieldIndex );
      data.fileName = data.fileNameMeta.getString( row[ data.fileNameFieldIndex ] );

      if ( data.fileName == null ) {
        throw new HopFileException( BaseMessages.getString( PKG, "TextFileOutput.Exception.FileNameNotSet" ) );
      }

      filename = buildFilename( environmentSubstitute( data.fileName ), true );
    }
    return filename;
  }

  public int getFlushInterval() {
    String var = getPipelineMeta().getVariable( "HOP_FILE_OUTPUT_MAX_STREAM_LIFE" );
    int flushInterval = 0;
    if ( var != null ) {
      try {
        flushInterval = Integer.parseInt( var );
      } catch ( Exception ex ) {
        // Do nothing
      }
    }
    return flushInterval;
  }

  public int getMaxOpenFiles() {
    String var = getPipelineMeta().getVariable( "HOP_FILE_OUTPUT_MAX_STREAM_COUNT" );
    int maxStreamCount = 0;
    if ( var != null ) {
      try {
        maxStreamCount = Integer.parseInt( var );
      } catch ( Exception ex ) {
        // Do nothing
      }
    }
    return maxStreamCount;
  }


  private boolean writeRowToServlet( Object[] row ) throws HopException {
    if ( row != null ) {
      if ( data.writer == null ) {
        initServletStreamWriter();
      }
      first = false;
      writeRow( data.outputRowMeta, row );
      putRow( data.outputRowMeta, row ); // in case we want it to go further...

      if ( checkFeedback( getLinesOutput() ) ) {
        logBasic( "linenr " + getLinesOutput() );
      }

      return true;
    } else {
      if ( ( data.writer == null ) && !Utils.isEmpty( environmentSubstitute( meta.getEndedLine() ) ) ) {
        initServletStreamWriter();
        initBinaryDataFields();
      }
      writeEndedLine();
      setOutputDone();
      return false;
    }
  }


  // Warning!!!
  // We need to be very particular about how we go about determining whether or not to write a file header before writing the row data.
  // There are two performance issues in play. 1: Don't hit the file system unnecessarily. 2: Don't search the collection of
  // file streams unnecessarily. Messing around with this method could have serious performance impacts.
  public boolean isWriteHeader( String filename ) throws HopException {
    boolean writingToFileForFirstTime = first;
    boolean isWriteHeader = meta.isHeaderEnabled();
    if ( isWriteHeader ) {
      if ( data.splitEvery > 0 ) {
        writingToFileForFirstTime |= !filename.equals( data.getFileStreamsCollection().getLastFileName() );
      } else {
        writingToFileForFirstTime |= data.getFileStreamsCollection().getStream( filename ) == null;
      }
    }
    isWriteHeader &= writingToFileForFirstTime && ( !meta.isFileAppended() || ( !COMPATIBILITY_APPEND_NO_HEADER && !isFileExists( filename ) ) );
    return isWriteHeader;
  }

  private boolean writeRowToFile( Object[] row ) throws HopException {
    if ( row != null ) {
      String filename = getOutputFileName( meta.isFileNameInField() ? row : null );
      boolean isWriteHeader = isWriteHeader( filename );
      if ( data.writer == null || meta.isFileNameInField() ) {
        initFileStreamWriter( filename );
      }

      first = false;

      if ( isWriteHeader ) {
        writeHeader();
      }

      // If file has reached max user defined size. Close current file and open a new file.
      if ( !meta.isFileNameInField()
        && ( getLinesOutput() > 0 )
        && ( data.splitEvery > 0 )
        && ( ( getLinesOutput() + meta.getFooterShift() ) % data.splitEvery ) == 0 ) {
        // If needed write footer to file before closing it.
        if ( meta.isFooterEnabled() ) {
          writeHeader();
        }
        closeFile( filename );

        // Open a new file and write footer if needed.
        data.splitnr++;
        data.fos = null;
        data.out = null;
        data.writer = null;
        filename = getOutputFileName( null );
        isWriteHeader = isWriteHeader( filename );
        initFileStreamWriter( filename );
        if ( isWriteHeader ) {
          writeHeader();
        }
      }

      writeRow( data.outputRowMeta, row );
      putRow( data.outputRowMeta, row ); // in case we want it to go further...

      if ( checkFeedback( getLinesOutput() ) ) {
        logBasic( "linenr " + getLinesOutput() );
      }

      int flushInterval = getFlushInterval();
      if ( flushInterval > 0 ) {
        long currentTime = new Date().getTime();
        if ( data.lastFileFlushTime == 0 ) {
          data.lastFileFlushTime = currentTime;
        } else if ( data.lastFileFlushTime - currentTime > flushInterval ) {
          try {
            data.getFileStreamsCollection().flushOpenFiles( false );
          } catch ( IOException e ) {
            throw new HopException( "Unable to flush open files", e );
          }
          data.lastFileFlushTime = new Date().getTime();
        }
      }
      return true;
    } else {
      if ( data.writer != null ) {
        if ( data.outputRowMeta != null && meta.isFooterEnabled() ) {
          writeHeader();
        }
      } else if ( !Utils.isEmpty( environmentSubstitute( meta.getEndedLine() ) ) && !meta.isFileNameInField() ) {
        String filename = getOutputFileName( null );
        initFileStreamWriter( filename );
        initBinaryDataFields();
      }
      if ( data.writer != null ) {
        writeEndedLine();
      }
      try {
        flushOpenFiles( true );
      } catch ( IOException e ) {
        throw new HopException( "Unable to flush open files", e );
      }
      setOutputDone();
      return false;
    }
  }

  public void flushOpenFiles( boolean closeAfterFlush ) throws IOException {
    data.getFileStreamsCollection().flushOpenFiles( true );
  }

  public synchronized boolean processRow() throws HopException {

    if ( ( meta.getEncoding() == null ) || ( meta.getEncoding().isEmpty() ) ) {
      meta.setEncoding( CharsetToolkit.getDefaultSystemCharset().name() );
    }

    Object[] row = getRow(); // This also waits for a row to be finished.

    if ( row != null && first ) {
      data.outputRowMeta = getInputRowMeta().clone();
    }

    if ( first ) {
      initBinaryDataFields();
      if ( data.outputRowMeta != null ) {
        initFieldNumbers( data.outputRowMeta, meta.getOutputFields() );
        if ( row != null ) {
          meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );
        }
      }
    }
    return writeRowTo( row );
  }

  protected boolean writeRowTo( Object[] row ) throws HopException {
    if ( meta.isServletOutput() ) {
      return writeRowToServlet( row );
    } else {
      return writeRowToFile( row );
    }
  }

  public void writeRow( IRowMeta rowMeta, Object[] r ) throws HopTransformException {
    try {
      if ( meta.getOutputFields() == null || meta.getOutputFields().length == 0 ) {
        /*
         * Write all values in stream to text file.
         */
        for ( int i = 0; i < rowMeta.size(); i++ ) {
          if ( i > 0 && data.binarySeparator.length > 0 ) {
            data.writer.write( data.binarySeparator );
          }
          IValueMeta v = rowMeta.getValueMeta( i );
          Object valueData = r[ i ];

          // no special null value default was specified since no fields are specified at all
          // As such, we pass null
          //
          writeField( v, valueData, null );
        }
        data.writer.write( data.binaryNewline );
      } else {
        /*
         * Only write the fields specified!
         */
        for ( int i = 0; i < meta.getOutputFields().length; i++ ) {
          if ( i > 0 && data.binarySeparator.length > 0 ) {
            data.writer.write( data.binarySeparator );
          }

          IValueMeta v = rowMeta.getValueMeta( data.fieldnrs[ i ] );
          Object valueData = r[ data.fieldnrs[ i ] ];
          writeField( v, valueData, data.binaryNullValue[ i ] );
        }
        data.writer.write( data.binaryNewline );
      }

      incrementLinesOutput();

    } catch ( Exception e ) {
      throw new HopTransformException( "Error writing line", e );
    }
  }

  private byte[] formatField( IValueMeta v, Object valueData ) throws HopValueException {
    if ( v.isString() ) {
      if ( v.isStorageBinaryString() && v.getTrimType() == IValueMeta.TRIM_TYPE_NONE && v.getLength() < 0
        && Utils.isEmpty( v.getStringEncoding() ) ) {
        return (byte[]) valueData;
      } else {
        String svalue = ( valueData instanceof String ) ? (String) valueData : v.getString( valueData );
        return convertStringToBinaryString( v, Const.trimToType( svalue, v.getTrimType() ) );
      }
    } else {
      return v.getBinaryString( valueData );
    }
  }

  private byte[] convertStringToBinaryString( IValueMeta v, String string ) throws HopValueException {
    int length = v.getLength();

    if ( string == null ) {
      return new byte[] {};
    }

    if ( length > -1 && length < string.length() ) {
      // we need to truncate
      String tmp = string.substring( 0, length );
      if ( Utils.isEmpty( v.getStringEncoding() ) ) {
        return tmp.getBytes();
      } else {
        try {
          return tmp.getBytes( v.getStringEncoding() );
        } catch ( UnsupportedEncodingException e ) {
          throw new HopValueException( "Unable to convert String to Binary with specified string encoding ["
            + v.getStringEncoding() + "]", e );
        }
      }
    } else {
      byte[] text;
      if ( Utils.isEmpty( meta.getEncoding() ) ) {
        text = string.getBytes();
      } else {
        try {
          text = string.getBytes( meta.getEncoding() );
        } catch ( UnsupportedEncodingException e ) {
          throw new HopValueException( "Unable to convert String to Binary with specified string encoding ["
            + v.getStringEncoding() + "]", e );
        }
      }
      if ( length > string.length() ) {
        // we need to pad this

        // Also for PDI-170: not all encoding use single characters, so we need to cope
        // with this.
        int size = 0;
        byte[] filler = null;
        try {
          if ( !Utils.isEmpty( meta.getEncoding() ) ) {
            filler = " ".getBytes( meta.getEncoding() );
          } else {
            filler = " ".getBytes();
          }
          size = text.length + filler.length * ( length - string.length() );
        } catch ( UnsupportedEncodingException uee ) {
          throw new HopValueException( uee );
        }
        byte[] bytes = new byte[ size ];
        System.arraycopy( text, 0, bytes, 0, text.length );
        if ( filler.length == 1 ) {
          java.util.Arrays.fill( bytes, text.length, size, filler[ 0 ] );
        } else {
          int currIndex = text.length;
          for ( int i = 0; i < ( length - string.length() ); i++ ) {
            for ( int j = 0; j < filler.length; j++ ) {
              bytes[ currIndex++ ] = filler[ j ];
            }
          }
        }
        return bytes;
      } else {
        // do not need to pad or truncate
        return text;
      }
    }
  }

  private byte[] getBinaryString( String string ) throws HopTransformException {
    try {
      if ( data.hasEncoding ) {
        return string.getBytes( meta.getEncoding() );
      } else {
        return string.getBytes();
      }
    } catch ( Exception e ) {
      throw new HopTransformException( e );
    }
  }

  private void writeField( IValueMeta v, Object valueData, byte[] nullString ) throws HopTransformException {
    try {
      byte[] str;

      // First check whether or not we have a null string set
      // These values should be set when a null value passes
      //
      if ( nullString != null && v.isNull( valueData ) ) {
        str = nullString;
      } else {
        if ( meta.isFastDump() ) {
          if ( valueData instanceof byte[] ) {
            str = (byte[]) valueData;
          } else {
            str = getBinaryString( ( valueData == null ) ? "" : valueData.toString() );
          }
        } else {
          str = formatField( v, valueData );
        }
      }

      if ( str != null && str.length > 0 ) {
        List<Integer> enclosures = null;
        boolean writeEnclosures = false;

        if ( v.isString() ) {
          if ( meta.isEnclosureForced() && !meta.isPadded() ) {
            writeEnclosures = true;
          } else if ( !meta.isEnclosureFixDisabled()
            && containsSeparatorOrEnclosure( str, data.binarySeparator, data.binaryEnclosure ) ) {
            writeEnclosures = true;
          }
        }

        if ( writeEnclosures ) {
          data.writer.write( data.binaryEnclosure );
          enclosures = getEnclosurePositions( str );
        }

        if ( enclosures == null ) {
          data.writer.write( str );
        } else {
          // Skip the enclosures, double them instead...
          int from = 0;
          for ( int i = 0; i < enclosures.size(); i++ ) {
            int position = enclosures.get( i );
            data.writer.write( str, from, position + data.binaryEnclosure.length - from );
            data.writer.write( data.binaryEnclosure ); // write enclosure a second time
            from = position + data.binaryEnclosure.length;
          }
          if ( from < str.length ) {
            data.writer.write( str, from, str.length - from );
          }
        }

        if ( writeEnclosures ) {
          data.writer.write( data.binaryEnclosure );
        }
      }
    } catch ( Exception e ) {
      throw new HopTransformException( "Error writing field content to file", e );
    }
  }

  private List<Integer> getEnclosurePositions( byte[] str ) {
    List<Integer> positions = null;
    if ( data.binaryEnclosure != null && data.binaryEnclosure.length > 0 ) {
      // +1 because otherwise we will not find it at the end
      for ( int i = 0, len = str.length - data.binaryEnclosure.length + 1; i < len; i++ ) {
        // verify if on position i there is an enclosure
        //
        boolean found = true;
        for ( int x = 0; found && x < data.binaryEnclosure.length; x++ ) {
          if ( str[ i + x ] != data.binaryEnclosure[ x ] ) {
            found = false;
          }
        }
        if ( found ) {
          if ( positions == null ) {
            positions = new ArrayList<Integer>();
          }
          positions.add( i );
        }
      }
    }
    return positions;
  }

  protected boolean writeEndedLine() {
    boolean retval = false;
    try {
      String sLine = environmentSubstitute( meta.getEndedLine() );
      if ( sLine != null ) {
        if ( sLine.trim().length() > 0 ) {
          data.writer.write( getBinaryString( sLine ) );
          incrementLinesOutput();
        }
      }
    } catch ( Exception e ) {
      logError( "Error writing ended tag line: " + e.toString() );
      logError( Const.getStackTracker( e ) );
      retval = true;
    }

    return retval;
  }

  protected boolean writeHeader() {
    boolean retval = false;
    IRowMeta r = data.outputRowMeta;

    try {
      // If we have fields specified: list them in this order!
      if ( meta.getOutputFields() != null && meta.getOutputFields().length > 0 ) {
        for ( int i = 0; i < meta.getOutputFields().length; i++ ) {
          String fieldName = meta.getOutputFields()[ i ].getName();
          IValueMeta v = r.searchValueMeta( fieldName );

          if ( i > 0 && data.binarySeparator.length > 0 ) {
            data.writer.write( data.binarySeparator );
          }

          boolean writeEnclosure =
            ( meta.isEnclosureForced() && data.binaryEnclosure.length > 0 && v != null && v.isString() )
              || ( ( !meta.isEnclosureFixDisabled() && containsSeparatorOrEnclosure( fieldName.getBytes(),
              data.binarySeparator, data.binaryEnclosure ) ) );

          if ( writeEnclosure ) {
            data.writer.write( data.binaryEnclosure );
          }
          data.writer.write( getBinaryString( fieldName ) );
          if ( writeEnclosure ) {
            data.writer.write( data.binaryEnclosure );
          }
        }
        data.writer.write( data.binaryNewline );
      } else if ( r != null ) {
        // Just put all field names in the header/footer
        for ( int i = 0; i < r.size(); i++ ) {
          if ( i > 0 && data.binarySeparator.length > 0 ) {
            data.writer.write( data.binarySeparator );
          }
          IValueMeta v = r.getValueMeta( i );

          boolean writeEnclosure =
            ( meta.isEnclosureForced() && data.binaryEnclosure.length > 0 && v != null && v.isString() )
              || ( ( !meta.isEnclosureFixDisabled() && containsSeparatorOrEnclosure( v.getName().getBytes(),
              data.binarySeparator, data.binaryEnclosure ) ) );

          if ( writeEnclosure ) {
            data.writer.write( data.binaryEnclosure );
          }
          data.writer.write( getBinaryString( v.getName() ) );
          if ( writeEnclosure ) {
            data.writer.write( data.binaryEnclosure );
          }
        }
        data.writer.write( data.binaryNewline );
      } else {
        data.writer.write( getBinaryString( "no rows selected" + Const.CR ) );
      }
    } catch ( Exception e ) {
      logError( "Error writing header line: " + e.toString() );
      logError( Const.getStackTracker( e ) );
      retval = true;
    }
    incrementLinesOutput();
    return retval;
  }

  public String buildFilename( String filename, boolean ziparchive ) {
    return meta.buildFilename( filename, meta.getExtension(), this, getCopy(), getPartitionId(), data.splitnr, ziparchive, meta );
  }

  protected boolean closeFile( String filename ) {
    try {
      data.getFileStreamsCollection().closeFile( filename );
    } catch ( Exception e ) {
      logError( "Exception trying to close file: " + e.toString() );
      setErrors( 1 );
      return false;
    }
    return true;
  }

  protected boolean closeFile() {
    boolean retval;

    try {
      if ( data.writer != null ) {
        data.getFileStreamsCollection().closeStream( data.writer );
      }
      data.writer = null;
      data.out = null;
      data.fos = null;
      if ( log.isDebug() ) {
        logDebug( "Closing normal file ..." );
      }
      retval = true;
    } catch ( Exception e ) {
      logError( "Exception trying to close file: " + e.toString() );
      setErrors( 1 );
      //Clean resources
      data.writer = null;
      data.out = null;
      data.fos = null;
      retval = false;
    }

    return retval;
  }

  @Override
  public boolean init() {

    if ( super.init() ) {
      data.splitnr = 0;
      // In case user want to create file at first row
      // In that case, DO NOT create file at Init
      if ( !meta.isDoNotOpenNewFileInit() && !meta.isFileNameInField() ) {
        try {
          initOutput();
        } catch ( Exception e ) {
          logError( "Couldn't open file "
            + HopVfs.getFriendlyURI( getParentVariableSpace().environmentSubstitute( meta.getFileName() ) )
            + "." + getParentVariableSpace().environmentSubstitute( meta.getExtension() ), e );
          setErrors( 1L );
          stopAll();
        }
      }

      try {
        initBinaryDataFields();
      } catch ( Exception e ) {
        logError( "Couldn't initialize binary data fields", e );
        setErrors( 1L );
        stopAll();
      }

      return true;
    }

    return false;
  }

  protected void initOutput() throws HopException {
    if ( meta.isServletOutput() ) {
      initServletStreamWriter();
    } else {
      String filename = getOutputFileName( null );
      initFileStreamWriter( filename );
    }
  }

  protected void initBinaryDataFields() throws HopException {
    try {
      data.hasEncoding = !Utils.isEmpty( meta.getEncoding() );
      data.binarySeparator = new byte[] {};
      data.binaryEnclosure = new byte[] {};
      data.binaryNewline = new byte[] {};

      if ( data.hasEncoding ) {
        if ( !Utils.isEmpty( meta.getSeparator() ) ) {
          data.binarySeparator = environmentSubstitute( meta.getSeparator() ).getBytes( meta.getEncoding() );
        }
        if ( !Utils.isEmpty( meta.getEnclosure() ) ) {
          data.binaryEnclosure = environmentSubstitute( meta.getEnclosure() ).getBytes( meta.getEncoding() );
        }
        if ( !Utils.isEmpty( meta.getNewline() ) ) {
          data.binaryNewline = meta.getNewline().getBytes( meta.getEncoding() );
        }
      } else {
        if ( !Utils.isEmpty( meta.getSeparator() ) ) {
          data.binarySeparator = environmentSubstitute( meta.getSeparator() ).getBytes();
        }
        if ( !Utils.isEmpty( meta.getEnclosure() ) ) {
          data.binaryEnclosure = environmentSubstitute( meta.getEnclosure() ).getBytes();
        }
        if ( !Utils.isEmpty( meta.getNewline() ) ) {
          data.binaryNewline = environmentSubstitute( meta.getNewline() ).getBytes();
        }
      }

      data.binaryNullValue = new byte[ meta.getOutputFields().length ][];
      for ( int i = 0; i < meta.getOutputFields().length; i++ ) {
        data.binaryNullValue[ i ] = null;
        String nullString = meta.getOutputFields()[ i ].getNullString();
        if ( !Utils.isEmpty( nullString ) ) {
          if ( data.hasEncoding ) {
            data.binaryNullValue[ i ] = nullString.getBytes( meta.getEncoding() );
          } else {
            data.binaryNullValue[ i ] = nullString.getBytes();
          }
        }
      }
      data.splitEvery = meta.getSplitEvery( variables );
    } catch ( Exception e ) {
      throw new HopException( "Unexpected error while encoding binary fields", e );
    }
  }

  protected void close() throws IOException {
    if ( !meta.isServletOutput() ) {
      data.getFileStreamsCollection().flushOpenFiles( true );
    }
  }

  @Override
  public void dispose() {
    try {
      close();
    } catch ( Exception e ) {
      logError( "Unexpected error closing file", e );
      setErrors( 1 );
    }
    data.writer = null;
    data.out = null;
    data.fos = null;

    super.dispose();
  }

  public boolean containsSeparatorOrEnclosure( byte[] source, byte[] separator, byte[] enclosure ) {
    boolean result = false;

    boolean enclosureExists = enclosure != null && enclosure.length > 0;
    boolean separatorExists = separator != null && separator.length > 0;

    // Skip entire test if neither separator nor enclosure exist
    if ( separatorExists || enclosureExists ) {

      // Search for the first occurrence of the separator or enclosure
      for ( int index = 0; !result && index < source.length; index++ ) {
        if ( enclosureExists && source[ index ] == enclosure[ 0 ] ) {

          // Potential match found, make sure there are enough bytes to support a full match
          if ( index + enclosure.length <= source.length ) {
            // First byte of enclosure found
            result = true; // Assume match
            for ( int i = 1; i < enclosure.length; i++ ) {
              if ( source[ index + i ] != enclosure[ i ] ) {
                // Enclosure match is proven false
                result = false;
                break;
              }
            }
          }

        } else if ( separatorExists && source[ index ] == separator[ 0 ] ) {

          // Potential match found, make sure there are enough bytes to support a full match
          if ( index + separator.length <= source.length ) {
            // First byte of separator found
            result = true; // Assume match
            for ( int i = 1; i < separator.length; i++ ) {
              if ( source[ index + i ] != separator[ i ] ) {
                // Separator match is proven false
                result = false;
                break;
              }
            }
          }

        }
      }

    }

    return result;
  }

  private void createParentFolder( String filename ) throws Exception {
    // Check for parent folder
    FileObject parentfolder = null;
    try {
      // Get parent folder
      parentfolder = getFileObject( filename, getPipelineMeta() ).getParent();
      if ( parentfolder.exists() ) {
        if ( isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "TextFileOutput.Log.ParentFolderExist",
            HopVfs.getFriendlyURI( parentfolder ) ) );
        }
      } else {
        if ( isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "TextFileOutput.Log.ParentFolderNotExist",
            HopVfs.getFriendlyURI( parentfolder ) ) );
        }
        if ( meta.isCreateParentFolder() ) {
          parentfolder.createFolder();
          if ( isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "TextFileOutput.Log.ParentFolderCreated",
              HopVfs.getFriendlyURI( parentfolder ) ) );
          }
        } else {
          throw new HopException( BaseMessages.getString( PKG, "TextFileOutput.Log.ParentFolderNotExistCreateIt",
            HopVfs.getFriendlyURI( parentfolder ), HopVfs.getFriendlyURI( filename ) ) );
        }
      }
    } finally {
      if ( parentfolder != null ) {
        try {
          parentfolder.close();
        } catch ( Exception ex ) {
          // Ignore
        }
      }
    }
  }

  protected FileObject getFileObject( String vfsFilename ) throws HopFileException {
    return HopVfs.getFileObject( vfsFilename );
  }

  protected FileObject getFileObject( String vfsFilename, IVariables variables ) throws HopFileException {
    return HopVfs.getFileObject( vfsFilename );
  }

  protected OutputStream getOutputStream( String vfsFilename, IVariables variables, boolean append ) throws HopFileException {
    return HopVfs.getOutputStream( vfsFilename, append );
  }

}
