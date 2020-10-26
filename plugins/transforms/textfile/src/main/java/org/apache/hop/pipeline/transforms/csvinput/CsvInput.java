/*! ******************************************************************************
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

package org.apache.hop.pipeline.transforms.csvinput;

import org.apache.commons.io.ByteOrderMark;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.provider.local.LocalFile;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.exception.HopConversionException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.file.EncodingType;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.*;
import org.apache.hop.pipeline.transforms.fileinput.TextFileInput;
import org.apache.hop.core.file.TextFileInputField;
import org.apache.hop.pipeline.transforms.fileinput.TextFileInputMeta;
import org.apache.hop.pipeline.transforms.fileinput.text.BOMDetector;
import org.apache.hop.ui.pipeline.transform.common.TextFileLineUtil;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Read a simple CSV file Just output Strings found in the file...
 *
 * @author Matt
 * @since 2007-07-05
 */
public class CsvInput
  extends BaseTransform<CsvInputMeta, CsvInputData>
  implements ITransform<CsvInputMeta, CsvInputData> {

  private static final Class<?> PKG = CsvInput.class; // for i18n purposes, needed by Translator!!

  public CsvInput( TransformMeta transformMeta, CsvInputMeta meta, CsvInputData data, int copyNr, PipelineMeta pipelineMeta,
                   Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  @Override
  public boolean processRow() throws HopException {

    if ( first ) {
      first = false;

      data.outputRowMeta = new RowMeta();
      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );

      if ( data.filenames == null ) {
        // We're expecting the list of filenames from the previous transform(s)...
        //
        getFilenamesFromPreviousTransforms();
      }

      // We only run in parallel if we have at least one file to process
      // AND if we have more than one transform copy running...
      //
      data.parallel = meta.isRunningInParallel() && data.totalNumberOfTransforms > 1;

      // The conversion logic for when the lazy conversion is turned of is simple:
      // Pretend it's a lazy conversion object anyway and get the native type during conversion.
      //
      data.convertRowMeta = data.outputRowMeta.clone();
      for ( IValueMeta valueMeta : data.convertRowMeta.getValueMetaList() ) {
        valueMeta.setStorageType( IValueMeta.STORAGE_TYPE_BINARY_STRING );
      }

      // Calculate the indexes for the filename and row number fields
      //
      data.filenameFieldIndex = -1;
      if ( !Utils.isEmpty( meta.getFilenameField() ) && meta.isIncludingFilename() ) {
        data.filenameFieldIndex = meta.getInputFields().length;
      }

      data.rownumFieldIndex = -1;
      if ( !Utils.isEmpty( meta.getRowNumField() ) ) {
        data.rownumFieldIndex = meta.getInputFields().length;
        if ( data.filenameFieldIndex >= 0 ) {
          data.rownumFieldIndex++;
        }
      }

      // Now handle the parallel reading aspect: determine total of all the file sizes
      // Then skip to the appropriate file and location in the file to start reading...
      // Also skip to right after the first newline
      //
      if ( data.parallel ) {
        prepareToRunInParallel();
      }

      // Open the next file...
      //
      if ( !openNextFile() ) {
        setOutputDone();
        return false; // nothing to see here, move along...
      }
    }

    // If we are running in parallel, make sure we don't read too much in this transform copy...
    //
    if ( data.parallel ) {
      if ( data.totalBytesRead >= data.blockToRead ) {
        setOutputDone(); // stop reading
        return false;
      }
    }

    try {
      Object[] outputRowData = readOneRow( false, false ); // get row, set busy!
      // no more input to be expected...
      if ( outputRowData == null ) {
        if ( openNextFile() ) {
          return true; // try again on the next loop...
        } else {
          setOutputDone(); // last file, end here
          return false;
        }
      } else {
        putRow( data.outputRowMeta, outputRowData ); // copy row to possible alternate rowset(s).
        if ( checkFeedback( getLinesInput() ) ) {
          if ( log.isBasic() ) {
            logBasic( BaseMessages.getString( PKG, "CsvInput.Log.LineNumber", Long.toString( getLinesInput() ) ) );
          }
        }
      }
    } catch ( HopConversionException e ) {
      if ( getTransformMeta().isDoingErrorHandling() ) {
        StringBuilder errorDescriptions = new StringBuilder( 100 );
        StringBuilder errorFields = new StringBuilder( 50 );
        for ( int i = 0; i < e.getCauses().size(); i++ ) {
          if ( i > 0 ) {
            errorDescriptions.append( ", " );
            errorFields.append( ", " );
          }
          errorDescriptions.append( e.getCauses().get( i ).getMessage() );
          errorFields.append( e.getFields().get( i ).toStringMeta() );
        }

        putError(
          data.outputRowMeta, e.getRowData(), e.getCauses().size(), errorDescriptions.toString(), errorFields
            .toString(), "CSVINPUT001" );
      } else {
        // Only forward the first cause.
        //
        throw new HopException( e.getMessage(), e.getCauses().get( 0 ) );
      }
    }

    return true;
  }

  public void prepareToRunInParallel() throws HopException {
    try {
      // At this point it doesn't matter if we have 1 or more files.
      // We'll use the same algorithm...
      //
      for ( String filename : data.filenames ) {
        long size = HopVfs.getFileObject( filename ).getContent().getSize();
        data.fileSizes.add( size );
        data.totalFileSize += size;
      }

      // Now we can determine the range to read.
      //
      // For example, the total file size is 50000, spread over 5 files of 10000
      // Suppose we have 2 transform copies running (clustered or not)
      // That means transform 0 has to read 0-24999 and transform 1 has to read 25000-49999
      //
      // The size of the block to read (25000 in the example) :
      //
      data.blockToRead = Math.round( (double) data.totalFileSize / (double) data.totalNumberOfTransforms );

      // Now we calculate the position to read (0 and 25000 in our sample) :
      //
      data.startPosition = data.blockToRead * data.transformNumber;
      data.endPosition = data.startPosition + data.blockToRead;

      // Determine the start file number (0 or 2 in our sample) :
      // >0<,1000,>2000<,3000,4000
      //
      long totalFileSize = 0L;
      for ( int i = 0; i < data.fileSizes.size(); i++ ) {
        long size = data.fileSizes.get( i );

        // Start of file range: totalFileSize
        // End of file range: totalFileSize+size

        if ( data.startPosition >= totalFileSize && data.startPosition < totalFileSize + size ) {
          // This is the file number to start reading from...
          //
          data.filenr = i;

          // remember where we started to read to allow us to know that we have to skip the header row in the next files
          // (if any)
          //
          data.startFilenr = i;

          // How many bytes do we skip in that first file?
          //
          if ( data.startPosition == 0 ) {
            data.bytesToSkipInFirstFile = 0L;
          } else {
            data.bytesToSkipInFirstFile = data.startPosition - totalFileSize;
          }

          break;
        }
        totalFileSize += size;
      }

      if ( data.filenames.length > 0 ) {
        logBasic( BaseMessages.getString(
          PKG, "CsvInput.Log.ParallelFileNrAndPositionFeedback", data.filenames[ data.filenr ], Long
            .toString( data.fileSizes.get( data.filenr ) ), Long.toString( data.bytesToSkipInFirstFile ), Long
            .toString( data.blockToRead ) ) );
      }
    } catch ( Exception e ) {
      throw new HopException( BaseMessages.getString( PKG, "CsvInput.Exception.ErrorPreparingParallelRun" ), e );
    }
  }

  private void getFilenamesFromPreviousTransforms() throws HopException {
    List<String> filenames = new ArrayList<>();
    boolean firstRow = true;
    int index = -1;
    Object[] row = getRow();
    while ( row != null ) {

      if ( firstRow ) {
        firstRow = false;

        // Get the filename field index...
        //
        String filenameField = environmentSubstitute( meta.getFilenameField() );
        index = getInputRowMeta().indexOfValue( filenameField );
        if ( index < 0 ) {
          throw new HopException( BaseMessages.getString(
            PKG, "CsvInput.Exception.FilenameFieldNotFound", filenameField ) );
        }
      }

      String filename = getInputRowMeta().getString( row, index );
      filenames.add( filename ); // add it to the list...

      row = getRow(); // Grab another row...
    }

    data.filenames = filenames.toArray( new String[ filenames.size() ] );

    logBasic( BaseMessages.getString( PKG, "CsvInput.Log.ReadingFromNrFiles", Integer
      .toString( data.filenames.length ) ) );
  }

  @Override
  public void dispose() {
    try {
      // Close the previous file...
      //
      if ( data.fc != null ) {
        data.fc.close();
      }
    } catch ( Exception e ) {
      logError( "Error closing file channel", e );
    }

    try {
      if ( data.fis != null ) {
        data.fis.close();
      }
    } catch ( Exception e ) {
      logError( "Error closing file input stream", e );
    }

    super.dispose();
  }

  private boolean openNextFile() throws HopException {
    try {

      // Close the previous file...
      //
      data.closeFile();

      if ( data.filenr >= data.filenames.length ) {
        return false;
      }

      // Open the next one...
      //
      data.fieldsMapping = createFieldMapping( data.filenames[ data.filenr ], meta );
      FileObject fileObject = HopVfs.getFileObject( data.filenames[ data.filenr ] );
      if ( !( fileObject instanceof LocalFile ) ) {
        // We can only use NIO on local files at the moment, so that's what we limit ourselves to.
        //
        throw new HopException( BaseMessages.getString( PKG, "CsvInput.Log.OnlyLocalFilesAreSupported" ) );
      }

      if ( meta.isLazyConversionActive() ) {
        data.binaryFilename = data.filenames[ data.filenr ].getBytes();
      }

      String vfsFilename = HopVfs.getFilename( fileObject );

      int bomSize = getBOMSize( vfsFilename );

      data.fis = new FileInputStream( vfsFilename );
      if ( 0 != bomSize ) {
        data.fis.skip( bomSize );
      }

      data.fc = data.fis.getChannel();
      data.bb = ByteBuffer.allocateDirect( data.preferredBufferSize );

      // If we are running in parallel and we need to skip bytes in the first file, let's do so here.
      //
      if ( data.parallel ) {
        if ( data.bytesToSkipInFirstFile > 0 ) {
          data.fc.position( data.bytesToSkipInFirstFile );

          // evaluate whether there is a need to skip a row
          if ( needToSkipRow() ) {
            // PDI-16589 - when reading in parallel, the previous code would introduce additional rows and / or invalid data in the output.
            // in parallel mode we don't support new lines inside field data so it's safe to fast forward until we find a new line.
            // when a newline is found we need to check for an additional new line character, while in unix systems it's just a single '\n',
            // on windows systems, it's a sequence of '\r' and '\n'. finally we set the start of the buffer to the end buffer position.
            while ( !data.newLineFound() ) {
              data.moveEndBufferPointer();
            }

            data.moveEndBufferPointer();

            if ( data.newLineFound() ) {
              data.moveEndBufferPointer();
            }
          }

          data.setStartBuffer( data.getEndBuffer() );
        }
      }

      // Add filename to result filenames ?
      if ( meta.isAddResultFile() ) {
        ResultFile resultFile =
          new ResultFile( ResultFile.FILE_TYPE_GENERAL, fileObject, getPipelineMeta().getName(), toString() );
        resultFile.setComment( "File was read by a Csv input transform" );
        addResultFile( resultFile );
      }

      // Move to the next filename
      //
      data.filenr++;

      // See if we need to skip a row...
      // - If you have a header row checked and if you're not running in parallel
      // - If you're running in parallel, if a header row is checked, if you're at the beginning of a file
      //
      if ( meta.isHeaderPresent() ) {
        // Standard flat file : skip header
        if ( !data.parallel || data.bytesToSkipInFirstFile <= 0 ) {
          readOneRow( true, false ); // skip this row.
          logBasic( BaseMessages.getString( PKG, "CsvInput.Log.HeaderRowSkipped", data.filenames[ data.filenr - 1 ] ) );
          if ( data.fieldsMapping.size() == 0 ) {
            return false;
          }
        }
      }
      // Reset the row number pointer...
      //
      data.rowNumber = 1L;

      // Don't skip again in the next file...
      //
      data.bytesToSkipInFirstFile = -1L;

      return true;
    } catch ( HopException e ) {
      throw e;
    } catch ( Exception e ) {
      throw new HopException( e );
    }
  }

  protected int getBOMSize( String vfsFilename ) throws Exception {
    int bomSize = 0;
    try ( FileInputStream fis = new FileInputStream( vfsFilename );
          BufferedInputStream bis = new BufferedInputStream( fis ) ) {
      BOMDetector bom = new BOMDetector( bis );

      if ( bom.bomExist() ) {
        bomSize = bom.getBomSize();
      }
    }

    return bomSize;
  }

  IFieldsMapping createFieldMapping( String fileName, CsvInputMeta csvInputMeta )
    throws HopException {
    IFieldsMapping mapping = null;
    if ( csvInputMeta.isHeaderPresent() ) {
      String[] fieldNames = readFieldNamesFromFile( fileName, csvInputMeta );
      mapping = NamedFieldsMapping.mapping( fieldNames, fieldNames( csvInputMeta ) );
    } else {
      int fieldsCount = csvInputMeta.getInputFields() == null ? 0 : csvInputMeta.getInputFields().length;
      mapping = UnnamedFieldsMapping.mapping( fieldsCount );
    }
    return mapping;
  }

  String[] readFieldNamesFromFile( String fileName, CsvInputMeta csvInputMeta ) throws HopException {
    String delimiter = environmentSubstitute( csvInputMeta.getDelimiter() );
    String enclosure = environmentSubstitute( csvInputMeta.getEnclosure() );
    String realEncoding = environmentSubstitute( csvInputMeta.getEncoding() );

    try ( FileObject fileObject = HopVfs.getFileObject( fileName );
          BOMInputStream inputStream =
            new BOMInputStream( HopVfs.getInputStream( fileObject ), ByteOrderMark.UTF_8, ByteOrderMark.UTF_16LE,
              ByteOrderMark.UTF_16BE ) ) {
      InputStreamReader reader = null;
      if ( Utils.isEmpty( realEncoding ) ) {
        reader = new InputStreamReader( inputStream );
      } else {
        reader = new InputStreamReader( inputStream, realEncoding );
      }
      EncodingType encodingType = EncodingType.guessEncodingType( reader.getEncoding() );
      String line =
        TextFileInput.getLine( log, reader, encodingType, TextFileInputMeta.FILE_FORMAT_UNIX, new StringBuilder(
          1000 ) );
      String[] fieldNames = TextFileLineUtil.guessStringsFromLine( log, line, delimiter, enclosure, csvInputMeta.getEscapeCharacter() );
      if ( !Utils.isEmpty( csvInputMeta.getEnclosure() ) ) {
        removeEnclosure( fieldNames, csvInputMeta.getEnclosure() );
      }
      trimFieldNames( fieldNames );
      return fieldNames;
    } catch ( IOException e ) {
      throw new HopFileException( BaseMessages.getString( PKG, "CsvInput.Exception.CreateFieldMappingError" ), e );
    }
  }

  static String[] fieldNames( CsvInputMeta csvInputMeta ) {
    TextFileInputField[] fields = csvInputMeta.getInputFields();
    String[] fieldNames = new String[ fields.length ];
    for ( int i = 0; i < fields.length; i++ ) {
      // TODO: We need to sanitize field names because existing ktr files may contain field names with leading BOM
      fieldNames[ i ] = fields[ i ].getName();
    }
    return fieldNames;
  }

  static void trimFieldNames( String[] strings ) {
    if ( strings != null ) {
      for ( int i = 0; i < strings.length; i++ ) {
        strings[ i ] = strings[ i ].trim();
      }
    }
  }

  static void removeEnclosure( String[] fields, String enclosure ) {
    if ( fields != null ) {
      for ( int i = 0; i < fields.length; i++ ) {
        if ( fields[ i ].startsWith( enclosure ) && fields[ i ].endsWith( enclosure ) && fields[ i ].length() > 1 ) {
          fields[ i ] = fields[ i ].substring( 1, fields[ i ].length() - 1 );
        }
      }
    }
  }

  /**
   * We need to skip row only if a line, that we are currently on is read by the previous transform <b>partly</b>.
   * In other words, we DON'T skip a line if we are just beginning to read it from the first symbol.
   * We have to do some work for this: read last byte from the previous transform and make sure that it is a new line byte.
   * But it's not enough. There could be a situation, where new line is indicated by '\r\n' construction. And if we are
   * <b>between</b> this construction, we want to skip last '\n', and don't want to include it in our line.
   * <p>
   * So, we DON'T skip line only if the previous char is new line indicator AND we are not between '\r\n'.
   */
  private boolean needToSkipRow() {
    try {
      // first we move pointer to the last byte of the previous transform
      data.fc.position( data.fc.position() - 1 );
      // read data, if not yet
      data.resizeBufferIfNeeded();

      // check whether the last symbol from the previous transform is a new line
      if ( data.newLineFound() ) {
        // don't increase bytes read for this transform, as it is actually content of another transform
        // and we are reading this just for evaluation.
        data.moveEndBufferPointer( false );
        // now we are at the first char of our thread.
        // there is still a situation we want to avoid: when there is a windows style "/r/n", and we are between two
        // of this chars. In this case we need to skip a line. Otherwise we don't skip it.
        return data.newLineFound();
      } else {
        // moving to the first char of our line.
        data.moveEndBufferPointer( false );
      }

    } catch ( IOException e ) {
      e.printStackTrace();
    } finally {
      try {
        data.fc.position( data.fc.position() + 1 );
      } catch ( IOException e ) {
        // nothing to do here
      }
    }

    return true;
  }

  /**
   * Read a single row of data from the file...
   *
   * @param skipRow          if row should be skipped: header row or part of row in case of parallel read
   * @param ignoreEnclosures if enclosures should be ignored, i.e. in case of we need to skip part of the row during
   *                         parallel read
   * @return a row of data...
   * @throws HopException
   */
  private Object[] readOneRow( boolean skipRow, boolean ignoreEnclosures ) throws HopException {

    try {

      Object[] outputRowData = RowDataUtil.allocateRowData( data.outputRowMeta.size() );
      int outputIndex = 0;
      boolean newLineFound = false;
      boolean endOfBuffer = false;
      List<Exception> conversionExceptions = null;
      List<IValueMeta> exceptionFields = null;

      // The strategy is as follows...
      // We read a block of byte[] from the file.
      // We scan for the separators in the file (NOT for line feeds etc)
      // Then we scan that block of data.
      // We keep a byte[] that we extend if needed..
      // At the end of the block we read another, etc.
      //
      // Let's start by looking where we left off reading.
      //
      while ( !newLineFound && outputIndex < meta.getInputFields().length ) {

        if ( data.resizeBufferIfNeeded() ) {
          // Last row was being discarded if the last item is null and
          // there is no end of line delimiter
          if ( outputRowData != null ) {
            // Make certain that at least one record exists before
            // filling the rest of them with null
            if ( outputIndex > 0 ) {
              // Optionally add the current filename to the mix as well...
              //
              if ( meta.isIncludingFilename() && !Utils.isEmpty( meta.getFilenameField() ) ) {
                if ( meta.isLazyConversionActive() ) {
                  outputRowData[ data.filenameFieldIndex ] = data.binaryFilename;
                } else {
                  outputRowData[ data.filenameFieldIndex ] = data.filenames[ data.filenr - 1 ];
                }
              }

              if ( data.isAddingRowNumber ) {
                outputRowData[ data.rownumFieldIndex ] = data.rowNumber++;
              }

              incrementLinesInput();
              return outputRowData;
            }
          }

          return null; // nothing more to read, call it a day.
        }

        // OK, at this point we should have data in the byteBuffer and we should be able to scan for the next
        // delimiter (;)
        // So let's look for a delimiter.
        // Also skip over the enclosures ("), it is NOT taking into account escaped enclosures.
        // Later we can add an option for having escaped or double enclosures in the file. <sigh>
        //
        boolean delimiterFound = false;
        boolean enclosureFound = false;
        boolean doubleLineEnd = false;
        int escapedEnclosureFound = 0;
        boolean ignoreEnclosuresInField = ignoreEnclosures;
        while ( !delimiterFound && !newLineFound && !endOfBuffer ) {
          // If we find the first char, we might find others as well ;-)
          // Single byte delimiters only for now.
          //
          if ( data.delimiterFound() ) {
            delimiterFound = true;
          } else if ( ( !meta.isNewlinePossibleInFields() || outputIndex == data.fieldsMapping.size() - 1 )
            && data.newLineFound() ) {
            // Perhaps we found a (pre-mature) new line?
            //
            // In case we are not using an enclosure and in case fields contain new lines
            // we need to make sure that we check the newlines possible flag.
            // If the flag is enable we skip newline checking except for the last field in the row.
            // In that one we can't support newlines without enclosure (handled below).
            //
            newLineFound = true;

            // Skip new line character
            for ( int i = 0; i < data.encodingType.getLength(); i++ ) {
              data.moveEndBufferPointer();
            }

            // Re-check for double new line (\r\n)...
            if ( data.newLineFound() ) {
              // Found another one, need to skip it later
              doubleLineEnd = true;
            }
          } else if ( data.enclosureFound() && !ignoreEnclosuresInField ) {
            int enclosurePosition = data.getEndBuffer();
            int fieldFirstBytePosition = data.getStartBuffer();
            if ( fieldFirstBytePosition == enclosurePosition ) {
              // Perhaps we need to skip over an enclosed part?
              // We always expect exactly one enclosure character
              // If we find the enclosure doubled, we consider it escaped.
              // --> "" is converted to " later on.
              //
              enclosureFound = true;
              boolean keepGoing;
              do {
                if ( data.moveEndBufferPointer() ) {
                  enclosureFound = false;
                  break;
                }
                keepGoing = !data.enclosureFound();
                if ( !keepGoing ) {
                  // We found an enclosure character.
                  // Read another byte...
                  if ( !data.endOfBuffer() && data.moveEndBufferPointer() ) {
                    break;
                  }
                  if ( data.enclosure.length > 1 ) {
                    data.moveEndBufferPointer();
                  }
                  // If this character is also an enclosure, we can consider the enclosure "escaped".
                  // As such, if this is an enclosure, we keep going...
                  //
                  keepGoing = data.enclosureFound();
                  if ( keepGoing ) {
                    escapedEnclosureFound++;
                  }
                }
              } while ( keepGoing );

              // Did we reach the end of the buffer?
              //
              if ( data.endOfBuffer() ) {
                endOfBuffer = true;
                break;
              }
            } else {
              // Ignoring enclosure if it's not at the field start
              ignoreEnclosuresInField = true;
            }
          } else {
            if ( data.moveEndBufferPointer() ) {
              endOfBuffer = true;
              break;
            }
          }
        }

        // If we're still here, we found a delimiter...
        // Since the starting point never changed really, we just can grab range:
        //
        // [startBuffer-endBuffer[
        //
        // This is the part we want.
        // data.byteBuffer[data.startBuffer]
        //

        byte[] field = data.getField( delimiterFound, enclosureFound, newLineFound, endOfBuffer );

        // Did we have any escaped characters in there?
        //
        if ( escapedEnclosureFound > 0 ) {
          if ( log.isRowLevel() ) {
            logRowlevel( "Escaped enclosures found in " + new String( field ) );
          }
          field = data.removeEscapedEnclosures( field, escapedEnclosureFound );
        }

        final int actualFieldIndex = outputIndex++;
        if ( actualFieldIndex != IFieldsMapping.FIELD_DOES_NOT_EXIST ) {
          if ( !skipRow ) {
            if ( meta.isLazyConversionActive() ) {
              outputRowData[ actualFieldIndex ] = field;
            } else {
              // We're not lazy so we convert the data right here and now.
              // The convert object uses binary storage as such we just have to ask the native type from it.
              // That will do the actual conversion.
              //
              IValueMeta sourceValueMeta = data.convertRowMeta.getValueMeta( actualFieldIndex );
              try {
                outputRowData[ actualFieldIndex ] = sourceValueMeta.convertBinaryStringToNativeType( field );
              } catch ( HopValueException e ) {
                // There was a conversion error,
                //
                outputRowData[ actualFieldIndex ] = null;

                if ( conversionExceptions == null ) {
                  conversionExceptions = new ArrayList<Exception>();
                  exceptionFields = new ArrayList<IValueMeta>();
                }

                conversionExceptions.add( e );
                exceptionFields.add( sourceValueMeta );
              }
            }
          } else {
            outputRowData[ actualFieldIndex ] = null; // nothing for the header, no conversions here.
          }
        }

        // OK, move on to the next field...
        // PDI-8187: Before we increment, we should check to see if the while condition is about to fail.
        // this will prevent the endBuffer from being incremented twice (once by this block and once in the
        // do-while loop below) and possibly skipping a newline character. This can occur if there is an
        // empty column at the end of the row (see the Jira case for details)
        if ( ( !newLineFound && outputIndex < meta.getInputFields().length ) || ( newLineFound && doubleLineEnd ) ) {

          int i = 0;
          while ( ( !data.newLineFound() && ( i < data.delimiter.length ) ) ) {
            data.moveEndBufferPointer();
            i++;
          }

          if ( data.isCarriageReturn() || doubleLineEnd ) {
            data.moveEndBufferPointerXTimes( data.encodingType.getLength() );
          }

        }

        data.setStartBuffer( data.getEndBuffer() );
      }

      // See if we reached the end of the line.
      // If not, we need to skip the remaining items on the line until the next newline...
      //
      if ( !newLineFound && !data.resizeBufferIfNeeded() ) {
        do {
          data.moveEndBufferPointer();
          if ( data.resizeBufferIfNeeded() ) {
            break; // nothing more to read.
          }

          // TODO: if we're using quoting we might be dealing with a very dirty file with quoted newlines in trailing
          // fields. (imagine that)
          // In that particular case we want to use the same logic we use above (refactored a bit) to skip these fields.

        } while ( !data.newLineFound() );

        if ( !data.resizeBufferIfNeeded() ) {
          while ( data.newLineFound() ) {
            data.moveEndBufferPointer();
            if ( data.resizeBufferIfNeeded() ) {
              break; // nothing more to read.
            }
          }
        }

        // Make sure we start at the right position the next time around.
        data.setStartBuffer( data.getEndBuffer() );
      }

      // Optionally add the current filename to the mix as well...
      //
      if ( meta.isIncludingFilename() && !Utils.isEmpty( meta.getFilenameField() ) ) {
        if ( meta.isLazyConversionActive() ) {
          outputRowData[ data.filenameFieldIndex ] = data.binaryFilename;
        } else {
          outputRowData[ data.filenameFieldIndex ] = data.filenames[ data.filenr - 1 ];
        }
      }

      if ( data.isAddingRowNumber ) {
        outputRowData[ data.rownumFieldIndex ] = data.rowNumber++;
      }

      if ( !ignoreEnclosures ) {
        incrementLinesInput();
      }

      if ( conversionExceptions != null && conversionExceptions.size() > 0 ) {
        // Forward the first exception
        //
        throw new HopConversionException(
          "There were " + conversionExceptions.size() + " conversion errors on line " + getLinesInput(),
          conversionExceptions, exceptionFields, outputRowData );
      }

      return outputRowData;
    } catch ( HopConversionException e ) {
      throw e;
    } catch ( IOException e ) {
      throw new HopFileException( "Exception reading line using NIO", e );
    }
  }


  public boolean init() {

    if ( super.init() ) {
      // PDI-10242 see if a variable is used as encoding value
      String realEncoding = environmentSubstitute( meta.getEncoding() );
      data.preferredBufferSize = Integer.parseInt( environmentSubstitute( meta.getBufferSize() ) );

      // If the transform doesn't have any previous transforms, we just get the filename.
      // Otherwise, we'll grab the list of file names later...
      //
      if ( getPipelineMeta().findNrPrevTransforms( getTransformMeta() ) == 0 ) {
        String filename = environmentSubstitute( meta.getFilename() );

        if ( Utils.isEmpty( filename ) ) {
          logError( BaseMessages.getString( PKG, "CsvInput.MissingFilename.Message" ) );
          return false;
        }

        data.filenames = new String[] { filename, };
      } else {
        data.filenames = null;
        data.filenr = 0;
      }

      data.totalBytesRead = 0L;

      data.encodingType = EncodingType.guessEncodingType( realEncoding );

      // PDI-2489 - set the delimiter byte value to the code point of the
      // character as represented in the input file's encoding
      try {
        data.delimiter = data.encodingType.getBytes( environmentSubstitute( meta.getDelimiter() ), realEncoding );

        if ( Utils.isEmpty( meta.getEnclosure() ) ) {
          data.enclosure = null;
        } else {
          data.enclosure = data.encodingType.getBytes( environmentSubstitute( meta.getEnclosure() ), realEncoding );
        }

      } catch ( UnsupportedEncodingException e ) {
        logError( BaseMessages.getString( PKG, "CsvInput.BadEncoding.Message" ), e );
        return false;
      }

      data.isAddingRowNumber = !Utils.isEmpty( meta.getRowNumField() );

      // Handle parallel reading capabilities...
      //
      data.stopReading = false;

      if ( meta.isRunningInParallel() ) {
        data.transformNumber = getCopyNr();
        data.totalNumberOfTransforms = getTransformMeta().getCopies();

        // We are not handling a single file, but possibly a list of files...
        // As such, the fair thing to do is calculate the total size of the files
        // Then read the required block.
        //
        data.fileSizes = new ArrayList<Long>();
        data.totalFileSize = 0L;
      }

      // Set the most efficient pattern matcher to match the delimiter.
      //
      if ( data.delimiter.length == 1 ) {
        data.delimiterMatcher = new SingleBytePatternMatcher();
      } else {
        data.delimiterMatcher = new MultiBytePatternMatcher();
      }

      // Set the most efficient pattern matcher to match the enclosure.
      //
      if ( data.enclosure == null ) {
        data.enclosureMatcher = new EmptyPatternMatcher();
      } else {
        if ( data.enclosure.length == 1 ) {
          data.enclosureMatcher = new SingleBytePatternMatcher();
        } else {
          data.enclosureMatcher = new MultiBytePatternMatcher();
        }
      }

      switch ( data.encodingType ) {
        case DOUBLE_BIG_ENDIAN:
          data.crLfMatcher = new MultiByteBigCrLfMatcher();
          break;
        case DOUBLE_LITTLE_ENDIAN:
          data.crLfMatcher = new MultiByteLittleCrLfMatcher();
          break;
        default:
          data.crLfMatcher = new SingleByteCrLfMatcher();
          break;
      }

      return true;

    }
    return false;
  }

}
