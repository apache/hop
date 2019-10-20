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

import junit.framework.TestCase;

import java.io.File;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by saslan on 10/22/2015.
 */
public class FileUtilTest extends TestCase {

  public void testCleanFolder() throws Exception {
    File mockFolder = mock( File.class );
    when( mockFolder.isDirectory() ).thenReturn( true );

    String[] folderList = new String[ 0 ];
    when( mockFolder.list() ).thenReturn( folderList );
    boolean folderDeleted = FileUtil.cleanFolder( mockFolder, true );
    assertEquals( folderDeleted, false );
    folderDeleted = FileUtil.cleanFolder( mockFolder, false );
    assertEquals( folderDeleted, true );
  }
}
