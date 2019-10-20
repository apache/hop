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
package org.apache.hop.metastore.stores.xml;

import org.junit.Test;
import org.apache.hop.metastore.api.exceptions.MetaStoreException;

import java.io.File;
import java.io.FileNotFoundException;

import static org.junit.Assert.fail;

public class XmlMetaStoreElementTest {

  @Test
  public void testLongName() {
    String pattern = "1234567890";
    StringBuilder fileName = new StringBuilder( 310 );
    for ( int i = 0; i < 30; i++ ) {
      fileName.append( pattern );
    }
    String tempDir = System.getProperty( "java.io.tmpdir" );
    XmlMetaStoreElement xmse = new XmlMetaStoreElement();
    xmse.setFilename( tempDir + File.separator + fileName );
    try {
      xmse.save();
      fail();
    } catch ( MetaStoreException ex ) {
      if ( !( ex.getCause() instanceof FileNotFoundException ) || !ex.getMessage().contains( "too long" ) ) {
        fail();
      }
    }
  }

}
