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

package org.apache.hop.pipeline.transforms.getfilesrowcount;

import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.Const;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Read all files, count rows number
 *
 * @author Samatar
 * @since 24-05-2007
 */
public class GetFilesRowsCount
  extends BaseTransform<GetFilesRowsCountMeta, GetFilesRowsCountData>
  implements ITransform<GetFilesRowsCountMeta, GetFilesRowsCountData> {

  private static final Class<?> PKG = GetFilesRowsCountMeta.class; // For Translator

  // private static final int BUFFER_SIZE_INPUT_STREAM = 500;

  public GetFilesRowsCount( TransformMeta transformMeta, GetFilesRowsCountMeta meta, GetFilesRowsCountData data, int copyNr,
                            PipelineMeta pipelineMeta, Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  private Object[] getOneRow() throws HopException {
    if ( !openNextFile() ) {
      return null;
    }

    // Build an empty row based on the meta-data
    Object[] r;
    try {
      // Create new row or clone
      if ( meta.isFileField() ) {
        r = data.readrow.clone();
        r = RowDataUtil.resizeArray( r, data.outputRowMeta.size() );
      } else {
        r = RowDataUtil.allocateRowData( data.outputRowMeta.size() );
      }

      if ( meta.isSmartCount() && data.foundData ) {
        // We have data right the last separator,
        // we need to update the row count
        data.rownr++;
      }

      r[ data.totalpreviousfields ] = data.rownr;

      if ( meta.includeCountFiles() ) {
        r[ data.totalpreviousfields + 1 ] = data.filenr;
      }

      incrementLinesInput();

    } catch ( Exception e ) {
      throw new HopException( "Unable to read row from file", e );
    }

    return r;
  }

  public boolean processRow() throws HopException {

    try {
      // Grab one row
      Object[] outputRowData = getOneRow();
      if ( outputRowData == null ) {
        setOutputDone(); // signal end to receiver(s)
        return false; // end of data or error.
      }
      if ( ( !meta.isFileField() && data.last_file ) || meta.isFileField() ) {
        putRow( data.outputRowMeta, outputRowData ); // copy row to output rowset(s);
        if ( log.isDetailed() ) {
          logDetailed(
            BaseMessages.getString( PKG, "GetFilesRowsCount.Log.TotalRowsFiles" ), data.rownr, data.filenr );
        }
      }

    } catch ( HopException e ) {

      logError( BaseMessages.getString( PKG, "GetFilesRowsCount.ErrorInTransformRunning", e.getMessage() ) );
      setErrors( 1 );
      stopAll();
      setOutputDone(); // signal end to receiver(s)
      return false;
    }
    return true;

  }

  private void getRowNumber() throws HopException {
    try {

      if ( data.file.getType() == FileType.FILE ) {
        data.fr = HopVfs.getInputStream( data.file );
        // Avoid method calls - see here:
        // http://java.sun.com/developer/technicalArticles/Programming/PerfTuning/
        byte[] buf = new byte[ 8192 ]; // BufferedaInputStream default buffer size
        int n;
        boolean prevCR = false;
        while ( ( n = data.fr.read( buf ) ) != -1 ) {
          for ( int i = 0; i < n; i++ ) {
            data.foundData = true;
            if ( meta.getRowSeparatorFormat().equals( "CRLF" ) ) {
              // We need to check for CRLF
              if ( buf[ i ] == '\r' || buf[ i ] == '\n' ) {
                if ( buf[ i ] == '\r' ) {
                  // we have a carriage return
                  // keep track of it..maybe we will have a line feed right after :-)
                  prevCR = true;
                } else if ( buf[ i ] == '\n' ) {
                  // we have a line feed
                  // let's see if we had previously a carriage return
                  if ( prevCR ) {
                    // we have a carriage return followed by a line feed
                    data.rownr++;
                    // Maybe we won't have data after
                    data.foundData = false;
                    prevCR = false;
                  }
                }
              } else {
                // we have another char (other than \n , \r)
                prevCR = false;
              }

            } else {
              if ( buf[ i ] == data.separator ) {
                data.rownr++;
                // Maybe we won't have data after
                data.foundData = false;
              }
            }
          }
        }
      }
      if ( isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "GetFilesRowsCount.Log.RowsInFile", data.file.toString(), ""
          + data.rownr ) );
      }
    } catch ( Exception e ) {
      throw new HopException( e );
    } finally {
      // Close inputstream - not used except for counting
      if ( data.fr != null ) {
        BaseTransform.closeQuietly( data.fr );
        data.fr = null;
      }
    }

  }

  private boolean openNextFile() {
    if ( data.last_file ) {
      return false; // Done!
    }

    try {
      if ( !meta.isFileField() ) {
        if ( data.filenr >= data.files.nrOfFiles() ) {
          // finished processing!

          if ( log.isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "GetFilesRowsCount.Log.FinishedProcessing" ) );
          }
          return false;
        }

        // Is this the last file?
        data.last_file = ( data.filenr == data.files.nrOfFiles() - 1 );
        data.file = data.files.getFile( (int) data.filenr );

      } else {
        data.readrow = getRow(); // Get row from input rowset & set row busy!
        if ( data.readrow == null ) {
          if ( log.isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "GetFilesRowsCount.Log.FinishedProcessing" ) );
          }
          return false;
        }

        if ( first ) {
          first = false;

          data.inputRowMeta = getInputRowMeta();
          data.outputRowMeta = data.inputRowMeta.clone();
          meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );

          // Get total previous fields
          data.totalpreviousfields = data.inputRowMeta.size();

          // Check is filename field is provided
          if ( Utils.isEmpty( meta.setOutputFilenameField() ) ) {
            logError( BaseMessages.getString( PKG, "GetFilesRowsCount.Log.NoField" ) );
            throw new HopException( BaseMessages.getString( PKG, "GetFilesRowsCount.Log.NoField" ) );
          }

          // cache the position of the field
          if ( data.indexOfFilenameField < 0 ) {
            data.indexOfFilenameField = getInputRowMeta().indexOfValue( meta.setOutputFilenameField() );
            if ( data.indexOfFilenameField < 0 ) {
              // The field is unreachable !
              logError( BaseMessages.getString( PKG, "GetFilesRowsCount.Log.ErrorFindingField", meta
                .setOutputFilenameField() ) );
              throw new HopException( BaseMessages.getString(
                PKG, "GetFilesRowsCount.Exception.CouldnotFindField", meta.setOutputFilenameField() ) );
            }
          }

        } // End if first

        String filename = getInputRowMeta().getString( data.readrow, data.indexOfFilenameField );
        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "GetFilesRowsCount.Log.FilenameInStream", meta
            .setOutputFilenameField(), filename ) );
        }

        data.file = HopVfs.getFileObject( filename );

        // Init Row number
        if ( meta.isFileField() ) {
          data.rownr = 0;
        }
      }

      // Move file pointer ahead!
      data.filenr++;

      if ( meta.isAddResultFile() ) {
        // Add this to the result file names...
        ResultFile resultFile =
          new ResultFile( ResultFile.FILE_TYPE_GENERAL, data.file, getPipelineMeta().getName(), getTransformName() );
        resultFile.setComment( BaseMessages.getString( PKG, "GetFilesRowsCount.Log.FileAddedResult" ) );
        addResultFile( resultFile );
      }

      if ( log.isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "GetFilesRowsCount.Log.OpeningFile", data.file.toString() ) );
      }
      getRowNumber();
      if ( log.isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "GetFilesRowsCount.Log.FileOpened", data.file.toString() ) );
      }

    } catch ( Exception e ) {
      logError( BaseMessages.getString( PKG, "GetFilesRowsCount.Log.UnableToOpenFile", "" + data.filenr, data.file
        .toString(), e.toString() ) );
      stopAll();
      setErrors( 1 );
      return false;
    }
    return true;
  }

  public boolean init(){
    if ( super.init() ) {

      if ( ( meta.getRowSeparatorFormat().equals( "CUSTOM" ) ) && ( Utils.isEmpty( meta.getRowSeparator() ) ) ) {
        logError( BaseMessages.getString( PKG, "GetFilesRowsCount.Error.NoSeparator.Title" ), BaseMessages
          .getString( PKG, "GetFilesRowsCount.Error.NoSeparator.Msg" ) );
        setErrors( 1 );
        stopAll();
      } else {
        // Checking for 'LF' for backwards compatibility.
        if ( meta.getRowSeparatorFormat().equals( "CARRIAGERETURN" ) || meta.getRowSeparatorFormat().equals( "LF" ) ) {
          data.separator = '\r';
          if ( isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "GetFilesRowsCount.Log.Separator.Title" ), BaseMessages
              .getString( PKG, "GetFilesRowsCount.Log.Separatoris.Infos" )
              + " \\n" );
          }
        } else if ( meta.getRowSeparatorFormat().equals( "LINEFEED" )
          || meta.getRowSeparatorFormat().equals( "CR" ) ) {
          // Checking for 'CR' for backwards compatibility.
          data.separator = '\n';
          if ( isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "GetFilesRowsCount.Log.Separator.Title" ), BaseMessages
              .getString( PKG, "GetFilesRowsCount.Log.Separatoris.Infos" )
              + " \\r" );
          }
        } else if ( meta.getRowSeparatorFormat().equals( "TAB" ) ) {
          data.separator = '\t';
          if ( isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "GetFilesRowsCount.Log.Separator.Title" ), BaseMessages
              .getString( PKG, "GetFilesRowsCount.Log.Separatoris.Infos" )
              + " \\t" );
          }
        } else if ( meta.getRowSeparatorFormat().equals( "CRLF" ) ) {
          if ( isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "GetFilesRowsCount.Log.Separator.Title" ), BaseMessages
              .getString( PKG, "GetFilesRowsCount.Log.Separatoris.Infos" )
              + " \\r\\n" );
          }
        } else {

          data.separator = resolve( meta.getRowSeparator() ).charAt( 0 );

          if ( isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "GetFilesRowsCount.Log.Separator.Title" ), BaseMessages
              .getString( PKG, "GetFilesRowsCount.Log.Separatoris.Infos" )
              + " " + data.separator );
          }
        }
      }

      if ( !meta.isFileField() ) {
        data.files = meta.getFiles( this );
        if ( data.files == null || data.files.nrOfFiles() == 0 ) {
          logError( BaseMessages.getString( PKG, "GetFilesRowsCount.Log.NoFiles" ) );
          return false;
        }
        try {
          // Create the output row meta-data
          data.outputRowMeta = new RowMeta();
          meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider ); // get the
          // metadata
          // populated

        } catch ( Exception e ) {
          logError( "Error initializing transform: " + e.toString() );
          logError( Const.getStackTracker( e ) );
          return false;
        }
      }
      data.rownr = 0;
      data.filenr = 0;
      data.totalpreviousfields = 0;

      return true;
    }
    return false;
  }

  public void dispose(){

    if ( data.file != null ) {
      try {
        data.file.close();
        data.file = null;
      } catch ( Exception e ) {
        log.logError( "Error closing file", e );
      }
    }
    if ( data.fr != null ) {
      BaseTransform.closeQuietly( data.fr );
      data.fr = null;
    }
    if ( data.lineStringBuilder != null ) {
      data.lineStringBuilder = null;
    }

    super.dispose();
  }

}
