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

package org.apache.hop.projects.xp;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.run.HopRun;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.vfs.HopVfs;

@ExtensionPoint( id = "HopRunCalculateFilenameExtensionPoint",
  extensionPointId = "HopRunCalculateFilename",
  description = "Resolves filenames specified relative to the given project"
)
public class HopRunCalculateFilenameExtensionPoint implements IExtensionPoint<HopRun> {

  @Override public void callExtensionPoint( ILogChannel log, IVariables variables, HopRun hopRun ) throws HopException {

    try {
      Thread.sleep( 10000 );
      FileObject fileObject = HopVfs.getFileObject( hopRun.getRealFilename() );
      if ( !fileObject.exists() ) {
        // Try to prepend with ${PROJECT_HOME}
        //
        String alternativeFilename = variables.resolve( "${PROJECT_HOME}/" + hopRun.getFilename() );
        fileObject = HopVfs.getFileObject( alternativeFilename );
        if ( fileObject.exists() ) {
          hopRun.setRealFilename(alternativeFilename);
          log.logBasic( "Relative path filename specified: " + hopRun.getRealFilename() );
        }
      }
    } catch ( Exception e ) {
      throw new HopException( "Error calculating filename", e );
    }
  }
}
