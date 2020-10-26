/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline.transform.errorhandling;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.exception.HopException;

import java.util.List;

public class CompositeFileErrorHandler implements IFileErrorHandler {
  private List<IFileErrorHandler> handlers;

  public CompositeFileErrorHandler( List<IFileErrorHandler> handlers ) {
    super();
    this.handlers = handlers;
  }

  public void handleFile( FileObject file ) throws HopException {
    for ( IFileErrorHandler handler : handlers ) {
      handler.handleFile( file );
    }
  }

  public void handleLineError( long lineNr, String filePart ) throws HopException {
    for ( IFileErrorHandler handler : handlers ) {
      handler.handleLineError( lineNr, filePart );
    }
  }

  public void close() throws HopException {
    for ( IFileErrorHandler handler : handlers ) {
      handler.close();
    }
  }

  public void handleNonExistantFile( FileObject file ) throws HopException {
    for ( IFileErrorHandler handler : handlers ) {
      handler.handleNonExistantFile( file );
    }
  }

  public void handleNonAccessibleFile( FileObject file ) throws HopException {
    for ( IFileErrorHandler handler : handlers ) {
      handler.handleNonAccessibleFile( file );
    }
  }
}
