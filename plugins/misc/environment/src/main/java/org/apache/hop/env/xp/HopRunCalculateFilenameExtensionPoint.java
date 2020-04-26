package org.apache.hop.env.xp;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.cli.HopRun;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.vfs.HopVfs;

@ExtensionPoint( id = "HopRunCalculateFilenameExtensionPoint",
  extensionPointId = "HopRunCalculateFilename",
  description = "Resolves filenames specified relative to the given environment"
)
public class HopRunCalculateFilenameExtensionPoint implements IExtensionPoint<HopRun> {

  @Override public void callExtensionPoint( ILogChannel log, HopRun hopRun ) throws HopException {

    try {
      FileObject fileObject = HopVfs.getFileObject( hopRun.getRealFilename() );
      if ( !fileObject.exists() ) {
        // Try to prepend with ${ENVIRONMENT_HOME}
        //
        String alternativeFilename = hopRun.getVariables().environmentSubstitute( "${ENVIRONMENT_HOME}/" + hopRun.getFilename() );
        fileObject = HopVfs.getFileObject( alternativeFilename );
        if ( fileObject.exists() ) {
          hopRun.setRealFilename(alternativeFilename);
          log.logMinimal( "Relative path filename specified: " + hopRun.getRealFilename() );
        }
      }
    } catch ( Exception e ) {
      throw new HopException( "Error calculating filename", e );
    }
  }
}
