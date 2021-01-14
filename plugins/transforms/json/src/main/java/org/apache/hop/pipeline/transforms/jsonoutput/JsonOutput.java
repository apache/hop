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

package org.apache.hop.pipeline.transforms.jsonoutput;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.BufferedOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

/**
 * Converts input rows to one or more Xml files.
 *
 * @author Matt
 * @since 14-jan-2006
 */
public class JsonOutput extends BaseTransform<JsonOutputMeta, JsonOutputData> implements ITransform<JsonOutputMeta, JsonOutputData> {
  private static final Class<?> PKG = JsonOutput.class; // For Translator

  public JsonOutput( TransformMeta transformMeta, JsonOutputMeta meta, JsonOutputData data, int copyNr, PipelineMeta pipelineMeta,
                     Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );

    // Here we decide whether or not to build the structure in
    // compatible mode or fixed mode
    JsonOutputMeta jsonOutputMeta = (JsonOutputMeta) ( transformMeta.getTransform() );
    if ( jsonOutputMeta.isCompatibilityMode() ) {
      compatibilityFactory = new CompatibilityMode();
    } else {
      compatibilityFactory = new FixedMode();
    }
  }

  private interface CompatibilityFactory {
    public void execute( Object[] row ) throws HopException;
  }

  @SuppressWarnings( "unchecked" )
  private class CompatibilityMode implements CompatibilityFactory {
    public void execute( Object[] row ) throws HopException {

      for ( int i = 0; i < data.nrFields; i++ ) {
        JsonOutputField outputField = meta.getOutputFields()[ i ];

        IValueMeta v = data.inputRowMeta.getValueMeta( data.fieldIndexes[ i ] );

        // Create a new object with specified fields
        JSONObject jo = new JSONObject();

        switch ( v.getType() ) {
          case IValueMeta.TYPE_BOOLEAN:
            jo.put( outputField.getElementName(), data.inputRowMeta.getBoolean( row, data.fieldIndexes[ i ] ) );
            break;
          case IValueMeta.TYPE_INTEGER:
            jo.put( outputField.getElementName(), data.inputRowMeta.getInteger( row, data.fieldIndexes[ i ] ) );
            break;
          case IValueMeta.TYPE_NUMBER:
            jo.put( outputField.getElementName(), data.inputRowMeta.getNumber( row, data.fieldIndexes[ i ] ) );
            break;
          case IValueMeta.TYPE_BIGNUMBER:
            jo.put( outputField.getElementName(), data.inputRowMeta.getBigNumber( row, data.fieldIndexes[ i ] ) );
            break;
          default:
            jo.put( outputField.getElementName(), data.inputRowMeta.getString( row, data.fieldIndexes[ i ] ) );
            break;
        }
        data.ja.add( jo );
      }

      data.nrRow++;

      if ( data.nrRowsInBloc > 0 ) {
        // System.out.println("data.nrRow%data.nrRowsInBloc = "+ data.nrRow%data.nrRowsInBloc);
        if ( data.nrRow % data.nrRowsInBloc == 0 ) {
          // We can now output an object
          // System.out.println("outputting the row.");
          outPutRow( row );
        }
      }
    }
  }

  @SuppressWarnings( "unchecked" )
  private class FixedMode implements CompatibilityFactory {
    public void execute( Object[] row ) throws HopException {

      // Create a new object with specified fields
      JSONObject jo = new JSONObject();

      for ( int i = 0; i < data.nrFields; i++ ) {
        JsonOutputField outputField = meta.getOutputFields()[ i ];

        IValueMeta v = data.inputRowMeta.getValueMeta( data.fieldIndexes[ i ] );

        switch ( v.getType() ) {
          case IValueMeta.TYPE_BOOLEAN:
            jo.put( outputField.getElementName(), data.inputRowMeta.getBoolean( row, data.fieldIndexes[ i ] ) );
            break;
          case IValueMeta.TYPE_INTEGER:
            jo.put( outputField.getElementName(), data.inputRowMeta.getInteger( row, data.fieldIndexes[ i ] ) );
            break;
          case IValueMeta.TYPE_NUMBER:
            jo.put( outputField.getElementName(), data.inputRowMeta.getNumber( row, data.fieldIndexes[ i ] ) );
            break;
          case IValueMeta.TYPE_BIGNUMBER:
            jo.put( outputField.getElementName(), data.inputRowMeta.getBigNumber( row, data.fieldIndexes[ i ] ) );
            break;
          default:
            jo.put( outputField.getElementName(), data.inputRowMeta.getString( row, data.fieldIndexes[ i ] ) );
            break;
        }
      }
      data.ja.add( jo );

      data.nrRow++;

      if ( data.nrRowsInBloc > 0 ) {
        // System.out.println("data.nrRow%data.nrRowsInBloc = "+ data.nrRow%data.nrRowsInBloc);
        if ( data.nrRow % data.nrRowsInBloc == 0 ) {
          // We can now output an object
          // System.out.println("outputting the row.");
          outPutRow( row );
        }
      }
    }
  }

  private CompatibilityFactory compatibilityFactory;


  public boolean processRow() throws HopException {
    Object[] r = getRow(); // This also waits for a row to be finished.
    if ( r == null ) {
      // no more input to be expected...
      if ( !data.rowsAreSafe ) {
        // Let's output the remaining unsafe data
        outPutRow( r );
      }

      setOutputDone();
      return false;
    }

    if ( first ) {
      first = false;
      data.inputRowMeta = getInputRowMeta();
      data.inputRowMetaSize = data.inputRowMeta.size();
      if ( data.outputValue ) {
        data.outputRowMeta = data.inputRowMeta.clone();
        meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );
      }

      // Cache the field name indexes
      //
      data.nrFields = meta.getOutputFields().length;
      data.fieldIndexes = new int[ data.nrFields ];
      for ( int i = 0; i < data.nrFields; i++ ) {
        data.fieldIndexes[ i ] = data.inputRowMeta.indexOfValue( meta.getOutputFields()[ i ].getFieldName() );
        if ( data.fieldIndexes[ i ] < 0 ) {
          throw new HopException( BaseMessages.getString( PKG, "JsonOutput.Exception.FieldNotFound" ) );
        }
        JsonOutputField field = meta.getOutputFields()[ i ];
        field.setElementName( resolve( field.getElementName() ) );
      }
    }

    data.rowsAreSafe = false;
    compatibilityFactory.execute( r );

    if ( data.writeToFile && !data.outputValue ) {
      putRow( data.inputRowMeta, r ); // in case we want it go further...
      incrementLinesOutput();
    }
    return true;
  }

  @SuppressWarnings( "unchecked" )
  private void outPutRow( Object[] rowData ) throws HopTransformException {
    // We can now output an object
    data.jg = new JSONObject();
    data.jg.put( data.realBlocName, data.ja );
    String value = data.jg.toJSONString();

    if ( data.outputValue && data.outputRowMeta != null ) {
      Object[] outputRowData = RowDataUtil.addValueData( rowData, data.inputRowMetaSize, value );
      incrementLinesOutput();
      putRow( data.outputRowMeta, outputRowData );
    }

    if ( data.writeToFile && !data.ja.isEmpty() ) {
      // Open a file
      if ( !openNewFile() ) {
        throw new HopTransformException( BaseMessages.getString(
          PKG, "JsonOutput.Error.OpenNewFile", buildFilename() ) );
      }
      // Write data to file
      try {
        data.writer.write( value );
      } catch ( Exception e ) {
        throw new HopTransformException( BaseMessages.getString( PKG, "JsonOutput.Error.Writing" ), e );
      }
      // Close file
      closeFile();
    }
    // Data are safe
    data.rowsAreSafe = true;
    data.ja = new JSONArray();
  }

  public boolean init() {

    if ( super.init() ) {

      data.writeToFile = ( meta.getOperationType() != JsonOutputMeta.OPERATION_TYPE_OUTPUT_VALUE );
      data.outputValue = ( meta.getOperationType() != JsonOutputMeta.OPERATION_TYPE_WRITE_TO_FILE );

      if ( data.outputValue ) {
        // We need to have output field name
        if ( Utils.isEmpty( resolve( meta.getOutputValue() ) ) ) {
          logError( BaseMessages.getString( PKG, "JsonOutput.Error.MissingOutputFieldName" ) );
          stopAll();
          setErrors( 1 );
          return false;
        }
      }
      if ( data.writeToFile ) {
        // We need to have output field name
        if ( Utils.isEmpty( meta.getFileName() ) ) {
          logError( BaseMessages.getString( PKG, "JsonOutput.Error.MissingTargetFilename" ) );
          stopAll();
          setErrors( 1 );
          return false;
        }
        if ( !meta.isDoNotOpenNewFileInit() ) {
          if ( !openNewFile() ) {
            logError( BaseMessages.getString( PKG, "JsonOutput.Error.OpenNewFile", buildFilename() ) );
            stopAll();
            setErrors( 1 );
            return false;
          }
        }

      }
      data.realBlocName = Const.NVL( resolve( meta.getJsonBloc() ), "" );
      data.nrRowsInBloc = Const.toInt( resolve( meta.getNrRowsInBloc() ), 0 );
      return true;
    }

    return false;
  }

  public void dispose() {
    if ( data.ja != null ) {
      data.ja = null;
    }
    if ( data.jg != null ) {
      data.jg = null;
    }
    closeFile();
    super.dispose();

  }

  private void createParentFolder( String filename ) throws HopTransformException {
    if ( !meta.isCreateParentFolder() ) {
      return;
    }
    // Check for parent folder
    FileObject parentfolder = null;
    try {
      // Get parent folder
      parentfolder = HopVfs.getFileObject( filename ).getParent();
      if ( !parentfolder.exists() ) {
        if ( log.isDebug() ) {
          logDebug( BaseMessages.getString( PKG, "JsonOutput.Error.ParentFolderNotExist", parentfolder.getName() ) );
        }
        parentfolder.createFolder();
        if ( log.isDebug() ) {
          logDebug( BaseMessages.getString( PKG, "JsonOutput.Log.ParentFolderCreated" ) );
        }
      }
    } catch ( Exception e ) {
      throw new HopTransformException( BaseMessages.getString(
        PKG, "JsonOutput.Error.ErrorCreatingParentFolder", parentfolder.getName() ) );
    } finally {
      if ( parentfolder != null ) {
        try {
          parentfolder.close();
        } catch ( Exception ex ) { /* Ignore */
        }
      }
    }
  }

  public boolean openNewFile() {
    if ( data.writer != null ) {
      return true;
    }
    boolean retval = false;

    try {


      String filename = buildFilename();
      createParentFolder( filename );
      if ( meta.AddToResult() ) {
        // Add this to the result file names...
        ResultFile resultFile =
          new ResultFile(
            ResultFile.FILE_TYPE_GENERAL, HopVfs.getFileObject( filename ),
            getPipelineMeta().getName(), getTransformName() );
        resultFile.setComment( BaseMessages.getString( PKG, "JsonOutput.ResultFilenames.Comment" ) );
        addResultFile( resultFile );
      }

      OutputStream outputStream;
      OutputStream fos = HopVfs.getOutputStream( filename, meta.isFileAppended() );
      outputStream = fos;

      if ( !Utils.isEmpty( meta.getEncoding() ) ) {
        data.writer =
          new OutputStreamWriter( new BufferedOutputStream( outputStream, 5000 ), resolve( meta
            .getEncoding() ) );
      } else {
        data.writer = new OutputStreamWriter( new BufferedOutputStream( outputStream, 5000 ) );
      }

      if ( log.isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "JsonOutput.FileOpened", filename ) );
      }

      data.splitnr++;
      

      retval = true;

    } catch ( Exception e ) {
      logError( BaseMessages.getString( PKG, "JsonOutput.Error.OpeningFile", e.toString() ) );
    }

    return retval;
  }

  public String buildFilename() {
    return meta.buildFilename( variables, getCopy() + "", null, data.splitnr + "",
      false );
  }

  protected boolean closeFile() {
    if ( data.writer == null ) {
      return true;
    }
    boolean retval = false;

    try {
      data.writer.close();
      data.writer = null;
      retval = true;
    } catch ( Exception e ) {
      logError( BaseMessages.getString( PKG, "JsonOutput.Error.ClosingFile", e.toString() ) );
      setErrors( 1 );
      retval = false;
    }

    return retval;
  }
}
