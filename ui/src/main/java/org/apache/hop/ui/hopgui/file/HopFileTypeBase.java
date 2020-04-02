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
