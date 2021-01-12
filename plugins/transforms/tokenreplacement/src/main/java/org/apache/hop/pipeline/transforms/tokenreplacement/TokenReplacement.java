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

package org.apache.hop.pipeline.transforms.tokenreplacement;

import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;


import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.Iterator;

public class TokenReplacement extends BaseTransform<TokenReplacementMeta, TokenReplacementData>
        implements ITransform<TokenReplacementMeta, TokenReplacementData> {
  private static Class<?> PKG = TokenReplacementMeta.class; // For Translator

  public TokenReplacement(TransformMeta transformMeta, TokenReplacementMeta meta, TokenReplacementData data, int copyNr, PipelineMeta pipelineMeta,
                          Pipeline pipeline ) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }


  public synchronized boolean processRow( ) throws HopException {

    boolean result = true;
    Object[] r = getRow(); // This also waits for a row to be finished.

    if ( first && r!=null ) {
      first = false;

      data.inputRowMeta = getInputRowMeta();
      data.outputRowMeta = getInputRowMeta().clone();

      if( meta.getOutputType().equalsIgnoreCase( "field" ) )
      {
        meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );

      }
      if( meta.getOutputType().equalsIgnoreCase( "file" ) && !meta.isOutputFileNameInField() )
      {
        if( meta.getOutputFileName() != null )
        {
          String filename = meta.buildFilename( meta.getOutputFileName(), this
                  , getCopy(), getPartitionId(), data.splitnr );

          openNewOutputFile( filename );
        } else {
          throw new HopException( "Output file name cannot be null." );
        }
      }
    }

    if ( r == null ) {
      // no more input to be expected...
      closeAllOutputFiles();
      setOutputDone();
      return false;
    }

    if( meta.getOutputType().equalsIgnoreCase( "file" ) && !meta.isOutputFileNameInField() && meta.getSplitEvery() > 0
      && data.rowNumber % meta.getSplitEvery() == 0 )
    {
      if( data.rowNumber > 0 )
      {
        closeAllOutputFiles();
        data.splitnr++;
        String filename = meta.buildFilename( meta.getOutputFileName(), this
                , getCopy(), getPartitionId(), data.splitnr );
        openNewOutputFile( filename );
      }
    }

    String outputFilename = "";
    if( meta.getOutputType().equalsIgnoreCase( "file" ) && !meta.isOutputFileNameInField() )
    {
      outputFilename = meta.buildFilename( meta.getOutputFileName()
              , this, getCopy(), getPartitionId(), data.splitnr );
    } else if ( meta.getOutputType().equalsIgnoreCase( "file" ) && meta.isOutputFileNameInField() ) {
      String filenameValue = data.inputRowMeta.getString( r, resolve( meta.getOutputFileNameField() ), "" );
      if( !Utils.isEmpty( filenameValue ) )
      {
        outputFilename = filenameValue;
      } else {
        throw new HopException( "Filename cannot be empty." );
      }
    }

    //Create token resolver
    TokenResolver resolver = new TokenResolver();

    for( TokenReplacementField field : meta.getTokenReplacementFields() )
    {
      if( data.inputRowMeta.indexOfValue( field.getName() ) >= 0 ) {
        String fieldValue = resolve( data.inputRowMeta.getString( r, field.getName(), null ) );
        if( fieldValue == null && !BooleanUtils.toBoolean( Const.getEnvironmentVariable( "KETTLE_EMPTY_STRING_DIFFERS_FROM_NULL", "N" ) ) )
        {
          fieldValue = Const.nullToEmpty( fieldValue );
        }
        resolver.addToken( field.getTokenName(), fieldValue );
      } else {
        throw new HopValueException( "Field " + field.getName() + " not found on input stream." );
      }
    }

    Reader reader;
    String inputFilename = "";

    if( meta.getInputType().equalsIgnoreCase( "text" ) )
    {
      reader = new TokenReplacingReader( resolver, new StringReader( meta.getInputText() ),
        resolve( meta.getTokenStartString() ), resolve( meta.getTokenEndString() ) );

    } else if ( meta.getInputType().equalsIgnoreCase( "field" ) )
    {
      if( data.inputRowMeta.indexOfValue( meta.getInputFieldName() ) >= 0 )
      {
        String inputString = data.inputRowMeta.getString( r, meta.getInputFieldName(), "" );
        reader = new TokenReplacingReader( resolver, new StringReader( inputString ),
          resolve( meta.getTokenStartString() ), resolve( meta.getTokenEndString() ) );

      } else {
        throw new HopValueException( "Input field " + meta.getInputFieldName() + " not found on input stream." );
      }
    } else if ( meta.getInputType().equalsIgnoreCase( "file" ) )
    {
      if( meta.isInputFileNameInField() )
      {
        if( data.inputRowMeta.indexOfValue( resolve( meta.getInputFileNameField() ) ) >= 0 )
        {
          inputFilename = data.inputRowMeta.getString( r, resolve( meta.getInputFileNameField() ), "" );
        } else {
          throw new HopValueException( "Input filename field " + resolve( meta.getInputFileNameField() )
            + " not found on input stream." );
        }
      } else {
        inputFilename = resolve( meta.getInputFileName() );
      }

      if( Utils.isEmpty( inputFilename ) )
      {
        throw new HopValueException( "Input filename cannot be empty" );
      }
      if( ! HopVfs.fileExists( inputFilename ) ) {
        throw new HopException( "Input file " + inputFilename + " does not exist." );
      }
      reader = new TokenReplacingReader( resolver, new InputStreamReader( HopVfs.getInputStream( inputFilename
               ) ), resolve( meta.getTokenStartString() ),
        resolve( meta.getTokenEndString() ) );

      if( meta.isAddInputFileNameToResult() )
      {
        ResultFile resultFile =
          new ResultFile( ResultFile.FILE_TYPE_GENERAL, HopVfs.getFileObject( inputFilename ),
            getTransformMeta().getName(), getTransformName() );
        resultFile.setComment( BaseMessages.getString( PKG, "TokenReplacement.AddInputResultFile" ) );
        addResultFile( resultFile );
      }
    } else {
      throw new HopException( "Unsupported input type " + meta.getInputType() );
    }

    Writer stringWriter = null;
    OutputStream bufferedWriter = null;

    if( meta.getOutputType().equalsIgnoreCase( "field" ) )
    {
      stringWriter = new StringWriter( 5000 );
    } else {
      if ( meta.getOutputType().equalsIgnoreCase( "file" ) ) {

        if ( inputFilename.equals( outputFilename ) ) {
          throw new HopException( "Input and output filenames must not be the same " + inputFilename );
        }

        int fileIndex = data.openFiles.indexOf( outputFilename );
        if ( fileIndex < 0 ) {
          openNewOutputFile( outputFilename );
          fileIndex = data.openFiles.indexOf( outputFilename );
        }

        bufferedWriter = data.openBufferedWriters.get( fileIndex );

      } else {
        throw new HopException( "Unsupported output type " + meta.getOutputType() );
      }
    }

    String output = "";

    try {
      char[] cbuf = new char[ 5000 ];
      int length = 0;
      while ( ( length = reader.read( cbuf ) ) > 0 )
      {
        if( meta.getOutputType().equalsIgnoreCase( "field" ) )
        {
          stringWriter.write( cbuf, 0, length );
        } else if ( meta.getOutputType().equalsIgnoreCase( "file" ) )
        {
          CharBuffer cBuffer = CharBuffer.wrap( cbuf, 0, length );
          ByteBuffer bBuffer = Charset.forName( meta.getOutputFileEncoding() ).encode( cBuffer );
          byte[] bytes = new byte[ bBuffer.limit() ];
          bBuffer.get( bytes );
          bufferedWriter.write( bytes );

        } //No else.  Anything else will be thrown to a Kettle exception prior to getting here.
        cbuf = new char[ 5000 ];
      }

      if( meta.getOutputType().equalsIgnoreCase( "field" ) )
      {
        output += stringWriter.toString();
      } else if ( meta.getOutputType().equalsIgnoreCase( "file" ) ) {
        bufferedWriter.write( meta.getOutputFileFormatString().getBytes() );
      }
    } catch ( IOException ex ) {
      throw new HopException( ex.getMessage(), ex );
    } finally {
      try {
        reader.close();
        if( stringWriter != null )
        {
          stringWriter.close();
        }

        reader = null;
        stringWriter = null;

      } catch (IOException ex)
      {
        throw new HopException( ex.getMessage(), ex );
      }

    }


    if( meta.getOutputType().equalsIgnoreCase( "field" ) ) {
      r = RowDataUtil.addValueData( r, data.outputRowMeta.size() - 1, output );
    } else if ( meta.getOutputType().equalsIgnoreCase( "file" ) )
    {
      incrementLinesWritten();
    }

    putRow( data.outputRowMeta, r ); // in case we want it to go further...
    data.rowNumber ++;
    if ( checkFeedback( getLinesOutput() ) ) {
      logBasic( "linenr " + getLinesOutput() );
    }

    return result;
  }

  public boolean init( ) {

    return super.init( );

  }


  public void dispose( ) {

     try {
      closeAllOutputFiles();
    } catch( HopException ignored ) {}

	  super.dispose( );
  }

  public void openNewOutputFile( String filename ) throws HopException
  {
    if( data.openFiles.contains( filename ) )
    {
      logDetailed( "File " + filename + " is already open." );
      return;
    }
    if( meta.isCreateParentFolder() )
    {
      try {
        createParentFolder( filename );
      } catch ( Exception ex ) {
        throw new HopException( ex.getMessage(), ex );
      }
    }

    boolean fileExists = HopVfs.fileExists( filename );

    OutputStream writer = HopVfs.getOutputStream( filename, meta.isAppendOutputFileName() );
    OutputStream bufferedWriter = new BufferedOutputStream( writer, 5000 );

    try {
      if ( fileExists && meta.isAppendOutputFileName() ) {
        bufferedWriter.write( meta.getOutputFileFormatString().getBytes() );
      }
    } catch ( IOException ex )
    {
      throw new HopException( ex.getMessage(), ex );
    }

    data.openFiles.add( filename );
    data.openWriters.add( writer );
    data.openBufferedWriters.add( bufferedWriter );

  }

  public void closeAllOutputFiles() throws HopException
  {
    try {
      Iterator<OutputStream> itWriter = data.openWriters.iterator();
      Iterator<OutputStream> itBufferedWriter = data.openBufferedWriters.iterator();
      Iterator<String> itFilename = data.openFiles.iterator();
      if(  meta.isAddOutputFileNameToResult() )
      {
        while( itFilename.hasNext() ) {
          ResultFile resultFile =
            new ResultFile( ResultFile.FILE_TYPE_GENERAL, HopVfs.getFileObject( itFilename.next() ),
              getTransformMeta()
                .getName(), getTransformName() );
          resultFile.setComment( BaseMessages.getString( PKG, "TokenReplacement.AddOutputResultFile" ) );
          addResultFile( resultFile );
        }
      }


      while( itBufferedWriter.hasNext() )
      {
        OutputStream bufferedWriter = itBufferedWriter.next();
        if( bufferedWriter != null )
        {
          bufferedWriter.flush();
          bufferedWriter.close();
        }
      }

      while( itWriter.hasNext() )
      {
        OutputStream writer = itWriter.next();
        if( writer != null )
        {
          writer.close();
        }
      }

      data.openBufferedWriters.clear();
      data.openWriters.clear();
      data.openFiles.clear();
    } catch ( IOException ex )
    {
      throw new HopException( ex.getMessage(), ex );
    }
  }


  private void createParentFolder( String filename ) throws Exception {
    // Check for parent folder
    FileObject parentfolder = null;
    try {
      // Get parent folder
      parentfolder = HopVfs.getFileObject( filename ).getParent();
      if ( parentfolder.exists() ) {
        if ( isDetailed() ) {
          logDetailed( BaseMessages
            .getString( PKG, "TokenReplacement.Log.ParentFolderExist", parentfolder.getName() ) );
        }
      } else {
        if ( isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "TokenReplacement.Log.ParentFolderNotExist", parentfolder
            .getName() ) );
        }
        if ( meta.isCreateParentFolder() ) {
          parentfolder.createFolder();
          if ( isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "TokenReplacement.Log.ParentFolderCreated", parentfolder
              .getName() ) );
          }
        } else {
          throw new HopException( BaseMessages.getString(
            PKG, "TokenReplacement.Log.ParentFolderNotExistCreateIt", parentfolder.getName(), filename ) );
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


}
