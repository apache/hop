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

package org.apache.hop.pipeline.transforms.yamlinput;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.List;

/**
 * Read YAML files, parse them and convert them to rows and writes these to one or more output streams.
 *
 * @author Samatar
 * @since 20-06-2007
 */
public class YamlInput extends BaseTransform<YamlInputMeta,YamlInputData> implements ITransform<YamlInputMeta,YamlInputData> {
  private static final Class<?> PKG = YamlInputMeta.class; // For Translator

  public YamlInput(TransformMeta transformMeta, YamlInputMeta meta, YamlInputData data, int copyNr, PipelineMeta pipelineMeta,
                 Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  private void handleMissingFiles() throws HopException {
    List<FileObject> nonExistantFiles = data.files.getNonExistantFiles();
    if ( nonExistantFiles.size() != 0 ) {
      String message = FileInputList.getRequiredFilesDescription( nonExistantFiles );
      logError( BaseMessages.getString( PKG, "YamlInput.Log.RequiredFilesTitle" ), BaseMessages.getString(
        PKG, "YamlInput.Log.RequiredFiles", message ) );

      throw new HopException( BaseMessages.getString( PKG, "YamlInput.Log.RequiredFilesMissing", message ) );
    }

    List<FileObject> nonAccessibleFiles = data.files.getNonAccessibleFiles();
    if ( nonAccessibleFiles.size() != 0 ) {
      String message = FileInputList.getRequiredFilesDescription( nonAccessibleFiles );
      logError( BaseMessages.getString( PKG, "YamlInput.Log.RequiredFilesTitle" ), BaseMessages.getString(
        PKG, "YamlInput.Log.RequiredNotAccessibleFiles", message ) );

      throw new HopException( BaseMessages.getString(
        PKG, "YamlInput.Log.RequiredNotAccessibleFilesMissing", message ) );
    }
  }

  private boolean readNextString() {

    try {
      data.readrow = getRow(); // Grab another row ...

      if ( data.readrow == null ) {
        // finished processing!
        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "YamlInput.Log.FinishedProcessing" ) );
        }
        return false;
      }

      if ( first ) {
        first = false;

        data.outputRowMeta = getInputRowMeta().clone();
        // Get total previous fields
        data.totalPreviousFields = data.outputRowMeta.size();
        data.totalOutFields = data.totalPreviousFields + data.nrInputFields;
        meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );

        // Check is Yaml field is provided
        if ( Utils.isEmpty( meta.getYamlField() ) ) {
          logError( BaseMessages.getString( PKG, "YamlInput.Log.NoField" ) );
          throw new HopException( BaseMessages.getString( PKG, "YamlInput.Log.NoField" ) );
        }

        // cache the position of the field
        data.indexOfYamlField = getInputRowMeta().indexOfValue( meta.getYamlField() );
        if ( data.indexOfYamlField < 0 ) {
          // The field is unreachable !
          logError( BaseMessages.getString( PKG, "YamlInput.Log.ErrorFindingField", meta.getYamlField() ) );
          throw new HopException( BaseMessages.getString( PKG, "YamlInput.Exception.CouldnotFindField", meta
            .getYamlField() ) );
        }
      }

      // get field value
      String Fieldvalue = getInputRowMeta().getString( data.readrow, data.indexOfYamlField );

      getLinesInput();

      if ( log.isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "YamlInput.Log.YAMLStream", meta.getYamlField(), Fieldvalue ) );
      }

      if ( meta.getIsAFile() ) {

        // source is a file.

        data.yaml = new YamlReader();
        data.yaml.loadFile( HopVfs.getFileObject( Fieldvalue ) );

        addFileToResultFilesname( data.yaml.getFile() );

      } else {
        data.yaml = new YamlReader();
        data.yaml.loadString( Fieldvalue );
      }
    } catch ( Exception e ) {
      logError( BaseMessages.getString( PKG, "YamlInput.Log.UnexpectedError", e.toString() ) );
      stopAll();
      logError( Const.getStackTracker( e ) );
      setErrors( 1 );
      return false;
    }
    return true;

  }

  private void addFileToResultFilesname( FileObject file ) throws Exception {
    if ( meta.addResultFile() ) {
      // Add this to the result file names...
      ResultFile resultFile =
        new ResultFile( ResultFile.FILE_TYPE_GENERAL, file, getPipelineMeta().getName(), getTransformName() );
      resultFile.setComment( BaseMessages.getString( PKG, "YamlInput.Log.FileAddedResult" ) );
      addResultFile( resultFile );
    }
  }

  private boolean openNextFile() {
    try {
      if ( data.filenr >= data.files.nrOfFiles() ) {
        // finished processing!
        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "YamlInput.Log.FinishedProcessing" ) );
        }
        return false;
      }
      // Get file to process from list
      data.file = data.files.getFile( data.filenr );

      // Move file pointer ahead!
      data.filenr++;

      if ( meta.isIgnoreEmptyFile() && data.file.getContent().getSize() == 0 ) {
        if ( isBasic() ) {
          logBasic( BaseMessages.getString( PKG, "YamlInput.Error.FileSizeZero", data.file.getName() ) );
        }
        // Let's open the next file
        openNextFile();

      } else {
        if ( isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "YamlInput.Log.OpeningFile", data.file.toString() ) );
        }

        // We have a file
        // define a Yaml reader and load file
        data.yaml = new YamlReader();
        data.yaml.loadFile( data.file );

        addFileToResultFilesname( data.file );

        if ( isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "YamlInput.Log.FileOpened", data.file.toString() ) );
        }
      }
    } catch ( Exception e ) {
      logError( BaseMessages.getString( PKG, "YamlInput.Log.UnableToOpenFile", "" + data.filenr, data.file
        .toString(), e.toString() ) );
      stopAll();
      setErrors( 1 );
      logError( Const.getStackTracker( e ) );
      return false;
    }
    return true;
  }

  public boolean processRow() throws HopException {
    if ( first && !meta.isInFields() ) {
      first = false;

      data.files = meta.getFiles( this );

      if ( !meta.isdoNotFailIfNoFile() && data.files.nrOfFiles() == 0 ) {
        throw new HopException( BaseMessages.getString( PKG, "YamlInput.Log.NoFiles" ) );
      }

      handleMissingFiles();

      // Create the output row meta-data
      data.outputRowMeta = new RowMeta();
      data.totalPreviousFields = 0;
      data.totalOutFields = data.totalPreviousFields + data.nrInputFields;
      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );
      data.totalOutStreamFields = data.outputRowMeta.size();

    }
    // Grab a row
    Object[] r = getOneRow();

    if ( r == null ) {
      setOutputDone(); // signal end to receiver(s)
      return false; // end of data or error.
    }

    if ( log.isRowLevel() ) {
      logRowlevel( BaseMessages.getString( PKG, "YamlInput.Log.ReadRow", data.outputRowMeta.getString( r ) ) );
    }

    incrementLinesOutput();

    data.rownr++;
    putRow( data.outputRowMeta, r ); // copy row to output rowset(s);

    if ( meta.getRowLimit() > 0 && data.rownr > meta.getRowLimit() ) {
      // limit has been reached: stop now.
      setOutputDone();
      return false;
    }
    return true;
  }

  private Object[] getOneRow() throws HopException {
    Object[] row = null;
    boolean rowAvailable = false;
    boolean fileOpened = false;
    if ( !meta.isInFields() ) {
      while ( data.file == null || ( data.file != null && !fileOpened && !rowAvailable ) ) {
        if ( data.file != null ) {
          // We have opened a file
          // read one row
          row = getRowData();

          if ( row == null ) {
            // No row extracted
            // let's see for the next file
            if ( !openNextFile() ) {
              return null;
            }
            fileOpened = true;
          } else {
            // We had extracted one row
            rowAvailable = true;
          }
        } else {
          // First time we get there
          // we have to open a new file
          if ( !openNextFile() ) {
            return null;
          }
          fileOpened = true;
        }
      }
    } else {
      while ( data.readrow == null || ( data.readrow != null && !fileOpened && !rowAvailable ) ) {
        if ( data.readrow != null ) {
          // We have red the incoming Yaml value
          // let's get one row
          row = getRowData();
          if ( row == null ) {
            // No row.. reader next row
            if ( !readNextString() ) {
              return null;
            }
            fileOpened = true;
          } else {
            // We have returned one row
            rowAvailable = true;
          }
        } else {
          // First time we get there
          // We have to parse incoming Yaml value
          if ( !readNextString() ) {
            return null;
          }
          fileOpened = true;
        }
        if ( data.readrow == null ) {
          return null;
        }
      }
    }

    if ( !rowAvailable ) {
      row = getRowData();
    }

    return row;
  }

  private Object[] getRowData() throws HopException {
    // Build an empty row based on the meta-data
    Object[] outputRowData = null;

    try {
      // Create new row...
      outputRowData = data.yaml.getRow( data.rowMeta );
      if ( outputRowData == null ) {
        return null;
      }

      if ( data.readrow != null ) {
        outputRowData = RowDataUtil.addRowData( data.readrow, data.totalPreviousFields, outputRowData );
      } else {
        outputRowData = RowDataUtil.resizeArray( outputRowData, data.totalOutStreamFields );
      }

      int rowIndex = data.totalOutFields;

      // See if we need to add the filename to the row...
      if ( meta.includeFilename() && !Utils.isEmpty( meta.getFilenameField() ) ) {
        outputRowData[ rowIndex++ ] = HopVfs.getFilename( data.file );
      }
      // See if we need to add the row number to the row...
      if ( meta.includeRowNumber() && !Utils.isEmpty( meta.getRowNumberField() ) ) {
        outputRowData[ rowIndex++ ] = new Long( data.rownr );
      }

    } catch ( Exception e ) {
      boolean sendToErrorRow = false;
      String errorMessage = null;

      if ( getTransformMeta().isDoingErrorHandling() ) {
        sendToErrorRow = true;
        errorMessage = e.toString();
      } else {
        logError( BaseMessages.getString( PKG, "YamlInput.ErrorInTransformRunning", e.toString() ) );
        setErrors( 1 );
        stopAll();
        logError( Const.getStackTracker( e ) );
        setOutputDone(); // signal end to receiver(s)
      }
      if ( sendToErrorRow ) {
        // Simply add this row to the error row
        putError( getInputRowMeta(), outputRowData, 1, errorMessage, null, "YamlInput001" );
      }
    }

    return outputRowData;
  }

  public boolean init() {

    if ( super.init() ) {
      data.rownr = 1L;
      data.nrInputFields = meta.getInputFields().length;

      data.rowMeta = new RowMeta();
      for ( int i = 0; i < data.nrInputFields; i++ ) {
        YamlInputField field = meta.getInputFields()[ i ];
        String path = resolve( field.getPath() );

        try {
          IValueMeta valueMeta = ValueMetaFactory.createValueMeta( path, field.getType() );
          valueMeta.setTrimType( field.getTrimType() );
          data.rowMeta.addValueMeta( valueMeta );
        } catch ( Exception e ) {
          log.logError( "Unable to create value meta", e );
          return false;
        }
      }

      return true;
    }
    return false;
  }

}
