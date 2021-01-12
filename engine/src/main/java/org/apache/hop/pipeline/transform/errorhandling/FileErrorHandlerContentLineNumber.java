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
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.transform.BaseTransform;

import java.util.Date;

public class FileErrorHandlerContentLineNumber extends AbstractFileErrorHandler {
  private static final Class<?> PKG = FileErrorHandlerContentLineNumber.class; // For Translator

  public FileErrorHandlerContentLineNumber( Date date, String destinationDirectory, String fileExtension,
                                            String encoding, BaseTransform baseTransform ) {
    super( date, destinationDirectory, fileExtension, encoding, baseTransform );
  }

  public void handleLineError( long lineNr, String filePart ) throws HopException {
    try {
      getWriter( filePart ).write( String.valueOf( lineNr ) );
      getWriter( filePart ).write( Const.CR );
    } catch ( Exception e ) {
      throw new HopException( BaseMessages.getString(
        PKG, "FileErrorHandlerContentLineNumber.Exception.CouldNotCreateWriteLine" )
        + lineNr, e );

    }
  }

  public void handleNonExistantFile( FileObject file ) {
  }

  public void handleNonAccessibleFile( FileObject file ) {
  }

}
