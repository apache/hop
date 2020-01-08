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

package org.apache.hop.core.playlist;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.vfs.HopVFS;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

class FilePlayListReplayLineNumberFile extends FilePlayListReplayFile {
  Set<Long> lineNumbers = new HashSet<Long>();

  public FilePlayListReplayLineNumberFile( FileObject lineNumberFile, String encoding, FileObject processingFile,
                                           String filePart ) throws HopException {
    super( processingFile, filePart );
    initialize( lineNumberFile, encoding );
  }

  private void initialize( FileObject lineNumberFile, String encoding ) throws HopException {
    BufferedReader reader = null;
    try {
      if ( encoding == null ) {
        reader = new BufferedReader( new InputStreamReader( HopVFS.getInputStream( lineNumberFile ) ) );
      } else {
        reader =
          new BufferedReader( new InputStreamReader( HopVFS.getInputStream( lineNumberFile ), encoding ) );
      }
      String line = null;
      while ( ( line = reader.readLine() ) != null ) {
        if ( line.length() > 0 ) {
          lineNumbers.add( Long.valueOf( line ) );
        }
      }
    } catch ( Exception e ) {
      throw new HopException( "Could not read line number file " + lineNumberFile.getName().getURI(), e );
    } finally {
      if ( reader != null ) {
        try {
          reader.close();
        } catch ( IOException e ) {
          throw new HopException( "Could not close line number file " + lineNumberFile.getName().getURI(), e );
        }
      }
    }
  }

  public boolean isProcessingNeeded( FileObject file, long lineNr, String filePart ) throws HopException {
    return lineNumbers.contains( new Long( lineNr ) );
  }
}
