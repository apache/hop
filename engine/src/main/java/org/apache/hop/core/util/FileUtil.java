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

package org.apache.hop.core.util;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;

import java.io.File;

public class FileUtil {
  public static boolean createParentFolder( Class<?> PKG, String filename, boolean createParentFolder,
                                            ILogChannel log ) {
    // Check for parent folder
    FileObject parentfolder = null;
    boolean resultat = true;
    try {
      // Get parent folder
      parentfolder = HopVfs.getFileObject( filename ).getParent();
      if ( !parentfolder.exists() ) {
        if ( createParentFolder ) {
          if ( log.isDebug() ) {
            log.logDebug( BaseMessages.getString( PKG, "ActionPipeline.Log.ParentLogFolderNotExist", parentfolder
              .getName().toString() ) );
          }
          parentfolder.createFolder();
          if ( log.isDebug() ) {
            log.logDebug( BaseMessages.getString( PKG, "ActionPipeline.Log.ParentLogFolderCreated", parentfolder
              .getName().toString() ) );
          }
        } else {
          log.logError( BaseMessages.getString( PKG, "ActionPipeline.Log.ParentLogFolderNotExist", parentfolder
            .getName().toString() ) );
          resultat = false;
        }
      } else {
        if ( log.isDebug() ) {
          log.logDebug( BaseMessages.getString( PKG, "ActionPipeline.Log.ParentLogFolderExists", parentfolder.getName().toString() ) );
        }
      }
    } catch ( Exception e ) {
      resultat = false;
      log.logError( BaseMessages.getString( PKG, "ActionPipeline.Error.ChekingParentLogFolderTitle" ),
        BaseMessages.getString( PKG, "ActionPipeline.Error.ChekingParentLogFolder", parentfolder.getName().toString() ), e );
    } finally {
      if ( parentfolder != null ) {
        try {
          parentfolder.close();
          parentfolder = null;
        } catch ( Exception ex ) {
          // Ignore
        }
      }
    }

    return resultat;
  }

  /**
   * Tests whether this abstract pathname is absolute.
   * <p>
   * The pathname is absolute if its prefix is "/", "\" and on Microsoft Windows systems,
   * a pathname is absolute if its prefix is a drive specifier followed by "\\", or if its prefix is "\\\\".
   */
  public static boolean isFullyQualified( String pathname ) {
    return new File( pathname ).isAbsolute() || pathname.startsWith( "/" ) || pathname.startsWith( "\\" );
  }

}
