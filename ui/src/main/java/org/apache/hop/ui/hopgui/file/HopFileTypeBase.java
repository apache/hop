/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
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

package org.apache.hop.ui.hopgui.file;

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.vfs.HopVFS;
import org.apache.hop.core.xml.IXml;

import java.util.Properties;

public abstract class HopFileTypeBase<T extends IXml> implements IHopFileType<T> {
  @Override
  public abstract Properties getCapabilities();

  @Override
  public abstract String[] getFilterExtensions();

  @Override
  public boolean equals( Object obj ) {
    if ( obj == this ) {
      return true;
    }
    if ( obj.getClass().equals( this.getClass() ) ) {
      return true; // same class is enough
    }
    return false;
  }

  @Override
  public boolean isHandledBy( String filename, boolean checkContent ) throws HopException {
    try {
      if ( checkContent ) {
        throw new HopException( "Generic file content validation is not possible at this time for file '" + filename + "'" );
      } else {
        FileObject fileObject = HopVFS.getFileObject( filename );
        FileName fileName = fileObject.getName();
        String fileExtension = fileName.getExtension();

        // Verify the extension
        //
        for ( String typeExtension : getFilterExtensions() ) {
          if ( fileExtension.equalsIgnoreCase( typeExtension ) ) {
            return true;
          }
        }

        return false;
      }
    } catch ( Exception e ) {
      throw new HopException( "Unable to verify file handling of file '" + filename + "' by extension", e );
    }
  }

  @Override public boolean hasCapability( String capability ) {
    if ( getCapabilities() == null ) {
      return false;
    }
    return getCapabilities().get( capability ) != null;
  }
}
