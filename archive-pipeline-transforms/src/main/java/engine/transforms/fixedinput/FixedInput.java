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

package org.apache.hop.pipeline.transforms.fixedinput;

import org.apache.commons.io.FileUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVFS;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransform;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;

/**
 * Read a simple fixed width file Just output fields found in the file...
 *
 * @author Matt
 * @since 2007-07-06
 */
public class FixedInput extends BaseTransform implements ITransform {
  private static Class<?> PKG = FixedInputMeta.class; // for i18n purposes, needed by Translator!!

  private FixedInputMeta meta;
  private FixedInputData data;

  public FixedInput( TransformMeta transformMeta, ITransformData data, int copyNr, PipelineMeta pipelineMeta,
                     Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  public boolean processRow() throws HopException {
    meta = (FixedInputMeta) smi;
    data = (FixedInputData) sdi;

    if ( first ) {
      first = false;

      data.outputRowMeta = new RowMeta();
      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metaStore );

      // The conversion logic for when the lazy conversion is turned of is simple:
      // Pretend it's a lazy conversion object anyway and get the native type during conversion.
      //
      data.convertRowMeta = data.outputRowMeta.clone();
      for ( IValueMeta valueMeta : data.convertRowMeta.getValueMetaList() ) {
        valueMeta.setStorageType( IValueMeta.STORAGE_TYPE_BINARY_STRING );
      }

      if ( meta.isHeaderPresent() ) {
        readOneRow( false ); // skip this row.
      }
    }

    Object[] outputRowData = readOneRow( true );
    if ( outputRowData == null ) { // no more input to be expected...
      setOutputDone();
      return false;
    }

    putRow( data.outputRowMeta, outputRowData ); // copy row to possible alternate rowset(s).

    if ( checkFeedback( getLinesInput() ) ) {
      logBasic( BaseMessages.getString( PKG, "FixedInput.Log.LineNumber", Long.toString( getLinesInput() ) ) );
    }

    return true;
  }

  /**
   * Read a single row of data from the file...
   *
   * @param doConversions if you want to do conversions, set to false for the header row.
   * @return a row of data...
   * @throws HopException
   */
  private Object[] readOneRow( boolean doConversions ) throws HopException {

    try {

      // See if we need to call it a day...
      //
      if ( meta.isRunningInParallel() ) {
        if ( getLinesInput() >= data.rowsToRead ) {
          return null; // We're done. The rest is for the other transforms in the cluster
        }
      }

      Object[] outputRowData = RowDataUtil.allocateRowData( data.convertRowMeta.size() );
      int outputIndex = 0;

      // The strategy is as follows...
      // We read a block of byte[] from the file.
      //
      // Then we scan that block of data.
      // We keep a byte[] that we extend if needed..
      // At the end of the block we read another, etc.
      //
      // Let's start by looking where we left off reading.
      //

      if ( data.stopReading ) {
        return null;
      }

      FixedFileInputField[] fieldDefinitions = meta.getFieldDefinition();
      for ( int i = 0; i < fieldDefinitions.length; i++ ) {

        int fieldWidth = fieldDefinitions[ i ].getWidth();
        data.endBuffer = data.startBuffer + fieldWidth;
        if ( data.endBuffer > data.bufferSize ) {
          // Oops, we need to read more data...
          // Better resize this before we read other things in it...
          //
          data.resizeByteBuffer();

          // Also read another chunk of data, now that we have the space for it...
          // Ignore EOF, there might be other stuff in the buffer.
          //
          data.readBufferFromFile();
        }

        // re-verify the buffer after we tried to read extra data from file...
        //
        if ( data.endBuffer > data.bufferSize ) {
          // still a problem?
          // We hit an EOF and are trying to read beyond the EOF...

          // If we are on the first field and there
          // is nothing left in the buffer, don't return
          // a row because we're done.
          if ( ( 0 == i ) && data.bufferSize <= 0 ) {
            return null;
          }

          // This is the last record of data in the file.
          data.stopReading = true;

          // Just take what's left for the current field.
          fieldWidth = data.bufferSize;
        }
        byte[] field = new byte[ fieldWidth ];
        System.arraycopy( data.byteBuffer, data.startBuffer, field, 0, fieldWidth );

        if ( doConversions ) {
          if ( meta.isLazyConversionActive() ) {
            outputRowData[ outputIndex++ ] = field;
          } else {
            // We're not lazy so we convert the data right here and now.
            // The convert object uses binary storage as such we just have to ask the native type from it.
            // That will do the actual conversion.
            //
            IValueMeta sourceValueMeta = data.convertRowMeta.getValueMeta( outputIndex );
            outputRowData[ outputIndex++ ] = sourceValueMeta.convertBinaryStringToNativeType( field );
          }
        } else {
          outputRowData[ outputIndex++ ] = null; // nothing for the header, no conversions here.
        }

        // OK, onto the next field...
        //
        data.startBuffer = data.endBuffer;
      }

      // Now that we have all the data, see if there are any linefeed characters to remove from the buffer...
      //
      if ( meta.isLineFeedPresent() ) {

        data.endBuffer += 2;

        if ( data.endBuffer >= data.bufferSize ) {
          // Oops, we need to read more data...
          // Better resize this before we read other things in it...
          //
          data.resizeByteBuffer();

          // Also read another chunk of data, now that we have the space for it...
          data.readBufferFromFile();
        }

        // CR + Line feed in the worst case.
        //
        if ( data.byteBuffer[ data.startBuffer ] == '\n' || data.byteBuffer[ data.startBuffer ] == '\r' ) {

          data.startBuffer++;

          if ( data.byteBuffer[ data.startBuffer ] == '\n' || data.byteBuffer[ data.startBuffer ] == '\r' ) {

            data.startBuffer++;
          }
        }
        data.endBuffer = data.startBuffer;
      }

      incrementLinesInput();
      return outputRowData;
    } catch ( Exception e ) {
      throw new HopFileException( "Exception reading line using NIO: " + e.toString(), e );
    }

  }

  private FileInputStream getFileInputStream( URL url ) throws FileNotFoundException {
    return new FileInputStream( FileUtils.toFile( url ) );
  }

  public boolean init() {
    meta = (FixedInputMeta) smi;
    data = (FixedInputData) sdi;

    if ( super.init() ) {
      try {
        data.preferredBufferSize = Integer.parseInt( environmentSubstitute( meta.getBufferSize() ) );
        data.lineWidth = Integer.parseInt( environmentSubstitute( meta.getLineWidth() ) );
        data.filename = environmentSubstitute( meta.getFilename() );

        if ( Utils.isEmpty( data.filename ) ) {
          logError( BaseMessages.getString( PKG, "FixedInput.MissingFilename.Message" ) );
          return false;
        }

        FileObject fileObject = HopVFS.getFileObject( data.filename, getPipelineMeta() );
        try {
          data.fis = getFileInputStream( fileObject.getURL() );
          data.fc = data.fis.getChannel();
          data.bb = ByteBuffer.allocateDirect( data.preferredBufferSize );
        } catch ( IOException e ) {
          logError( e.toString() );
          return false;
        }

        // Add filename to result filenames ?
        if ( meta.isAddResultFile() ) {
          ResultFile resultFile =
            new ResultFile( ResultFile.FILE_TYPE_GENERAL, fileObject, getPipelineMeta().getName(), toString() );
          resultFile.setComment( "File was read by a Fixed input transform" );
          addResultFile( resultFile );
        }

        logBasic( "Opened file with name [" + data.filename + "]" );

        data.stopReading = false;

        if ( meta.isRunningInParallel() ) {
          data.transformNumber = getUniqueTransformNrAcrossSlaves();
          data.totalNumberOfTransforms = getUniqueTransformCountAcrossSlaves();
          data.fileSize = fileObject.getContent().getSize();
        }

        // OK, now we need to skip a number of bytes in case we're doing a parallel read.
        //
        if ( meta.isRunningInParallel() ) {

          int totalLineWidth = data.lineWidth + meta.getLineSeparatorLength(); // including line separator bytes
          long nrRows = data.fileSize / totalLineWidth; // 100.000 / 100 = 1000 rows
          long rowsToSkip = Math.round( data.transformNumber * nrRows / (double) data.totalNumberOfTransforms ); // 0, 333, 667
          // 333, 667, 1000
          long nextRowsToSkip = Math.round( ( data.transformNumber + 1 ) * nrRows / (double) data.totalNumberOfTransforms );
          data.rowsToRead = nextRowsToSkip - rowsToSkip;
          long bytesToSkip = rowsToSkip * totalLineWidth;

          logBasic( "Transform #"
            + data.transformNumber + " is skipping " + bytesToSkip + " to position in file, then it's reading "
            + data.rowsToRead + " rows." );

          data.fc.position( bytesToSkip );
        }

        return true;
      } catch ( Exception e ) {
        logError( "Error opening file '" + meta.getFilename() + "'", e );
      }
    }
    return false;
  }

  @Override
  public void.dispose() {

    try {
      if ( data.fc != null ) {
        data.fc.close();
      }
      if ( data.fis != null ) {
        data.fis.close();
      }
    } catch ( IOException e ) {
      logError( "Unable to close file channel for file '" + meta.getFilename() + "' : " + e.toString() );
      logError( Const.getStackTracker( e ) );
    }

    super.dispose();
  }

}
