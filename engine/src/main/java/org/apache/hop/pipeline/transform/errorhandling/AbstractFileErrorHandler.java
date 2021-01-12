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

package org.apache.hop.pipeline.transform.errorhandling;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.transform.BaseTransform;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public abstract class AbstractFileErrorHandler implements IFileErrorHandler {
  private static final Class<?> PKG = AbstractFileErrorHandler.class; // For Translator

  private static final String DD_MMYYYY_HHMMSS = "ddMMyyyy-HHmmss";

  public static final String NO_PARTS = "NO_PARTS";

  private final String destinationDirectory;

  private final String fileExtension;

  private final String encoding;

  private String processingFilename;

  private Map<Object, Writer> writers;

  private String dateString;

  private BaseTransform baseTransform;

  public AbstractFileErrorHandler( Date date, String destinationDirectory, String fileExtension, String encoding,
                                   BaseTransform baseTransform ) {
    this.destinationDirectory = destinationDirectory;
    this.fileExtension = fileExtension;
    this.encoding = encoding;
    this.baseTransform = baseTransform;
    this.writers = new HashMap<>();
    initDateFormatter( date );
  }

  private void initDateFormatter( Date date ) {
    dateString = createDateFormat().format( date );
  }

  public static DateFormat createDateFormat() {
    return new SimpleDateFormat( DD_MMYYYY_HHMMSS );
  }

  public static FileObject getReplayFilename( String destinationDirectory, String processingFilename,
                                              String dateString, String extension, Object source ) throws HopFileException {
    String name = null;
    String sourceAdding = "";
    if ( !NO_PARTS.equals( source ) ) {
      sourceAdding = "_" + source.toString();
    }
    if ( extension == null || extension.length() == 0 ) {
      name = processingFilename + sourceAdding + "." + dateString;
    } else {
      name = processingFilename + sourceAdding + "." + dateString + "." + extension;
    }
    return HopVfs.getFileObject( destinationDirectory + "/" + name );
  }

  public static FileObject getReplayFilename( String destinationDirectory, String processingFilename, Date date,
                                              String extension, Object source ) throws HopFileException {
    return getReplayFilename(
      destinationDirectory, processingFilename, createDateFormat().format( date ), extension, source );
  }

  /**
   * returns the OutputWiter if exists. Otherwhise it will create a new one.
   *
   * @return
   * @throws HopException
   */
  Writer getWriter( Object source ) throws HopException {
    try {
      Writer outputStreamWriter = writers.get( source );
      if ( outputStreamWriter != null ) {
        return outputStreamWriter;
      }
      FileObject file =
        getReplayFilename( destinationDirectory, processingFilename, dateString, fileExtension, source );
      ResultFile resultFile =
        new ResultFile( ResultFile.FILE_TYPE_GENERAL, file, baseTransform.getPipelineMeta().getName(), baseTransform
          .getTransformName() );
      baseTransform.addResultFile( resultFile );
      try {
        if ( encoding == null ) {
          outputStreamWriter = new OutputStreamWriter( HopVfs.getOutputStream( file, false ) );
        } else {
          outputStreamWriter = new OutputStreamWriter( HopVfs.getOutputStream( file, false ), encoding );
        }
      } catch ( Exception e ) {
        throw new HopException( BaseMessages.getString(
          PKG, "AbstractFileErrorHandler.Exception.CouldNotCreateFileErrorHandlerForFile" )
          + file.getName().getURI(), e );
      }
      writers.put( source, outputStreamWriter );
      return outputStreamWriter;
    } catch ( HopFileException e ) {
      throw new HopException( BaseMessages.getString(
        PKG, "AbstractFileErrorHandler.Exception.CouldNotCreateFileErrorHandlerForFile" ), e );
    }
  }

  public void close() throws HopException {
    for ( Iterator<Writer> iter = writers.values().iterator(); iter.hasNext(); ) {
      close( iter.next() );
    }
    writers = new HashMap<>();

  }

  private void close( Writer outputStreamWriter ) throws HopException {
    if ( outputStreamWriter != null ) {
      try {
        outputStreamWriter.flush();
      } catch ( IOException exception ) {
        baseTransform.logError(
          BaseMessages.getString( PKG, "AbstractFileErrorHandler.Log.CouldNotFlushContentToFile" ), exception
            .getLocalizedMessage() );
      }
      try {
        outputStreamWriter.close();
      } catch ( IOException exception ) {
        throw new HopException( BaseMessages.getString(
          PKG, "AbstractFileErrorHandler.Exception.CouldNotCloseFile" ), exception );
      } finally {
        outputStreamWriter = null;
      }
    }
  }

  public void handleFile( FileObject file ) throws HopException {
    close();
    this.processingFilename = file.getName().getBaseName();
  }

}
