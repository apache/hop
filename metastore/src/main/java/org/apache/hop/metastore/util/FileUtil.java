/*!
 * This program is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License, version 2.1 as published by the Free Software
 * Foundation.
 *
 * You should have received a copy of the GNU Lesser General Public License along with this
 * program; if not, you can obtain a copy at http://www.gnu.org/licenses/old-licenses/lgpl-2.1.html
 * or from the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Lesser General Public License for more details.
 *
 * Copyright (c) 2002-2017 Hitachi Vantara..  All rights reserved.
 */

package org.apache.hop.metastore.util;

import java.io.File;

public class FileUtil {

  /**
   * Delete a folder with files and possible sub-folders with files.
   * 
   * @param folder
   *          The folder to delete
   * @param removeParent
   *          remove parent folder
   * @return true if the folder was deleted, false if there was a problem with that.
   */
  public static boolean cleanFolder( File folder, boolean removeParent ) {
    if ( folder.isDirectory() ) {
      String[] filenames = folder.list();
      for ( String filename : filenames ) {
        File file = new File( folder, filename );
        if ( file.isDirectory() ) {
          boolean ok = cleanFolder( new File( folder, filename ), true );
          if ( !ok ) {
            return false;
          }
        } else {
          boolean ok = file.delete();
          if ( !ok ) {
            return false;
          }
        }
      }
    }

    // The empty folder can now be deleted.
    //
    if ( removeParent ) {
      return folder.delete();
    } else {
      return true;
    }
  }
}
