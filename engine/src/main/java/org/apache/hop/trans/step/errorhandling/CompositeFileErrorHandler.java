/*! ******************************************************************************
 *
 * Pentaho Data Integration
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

package org.apache.hop.trans.step.errorhandling;

import java.util.List;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.exception.HopException;

public class CompositeFileErrorHandler implements FileErrorHandler {
  private List<FileErrorHandler> handlers;

  public CompositeFileErrorHandler( List<FileErrorHandler> handlers ) {
    super();
    this.handlers = handlers;
  }

  public void handleFile( FileObject file ) throws HopException {
    for ( FileErrorHandler handler : handlers ) {
      handler.handleFile( file );
    }
  }

  public void handleLineError( long lineNr, String filePart ) throws HopException {
    for ( FileErrorHandler handler : handlers ) {
      handler.handleLineError( lineNr, filePart );
    }
  }

  public void close() throws HopException {
    for ( FileErrorHandler handler : handlers ) {
      handler.close();
    }
  }

  public void handleNonExistantFile( FileObject file ) throws HopException {
    for ( FileErrorHandler handler : handlers ) {
      handler.handleNonExistantFile( file );
    }
  }

  public void handleNonAccessibleFile( FileObject file ) throws HopException {
    for ( FileErrorHandler handler : handlers ) {
      handler.handleNonAccessibleFile( file );
    }
  }
}
