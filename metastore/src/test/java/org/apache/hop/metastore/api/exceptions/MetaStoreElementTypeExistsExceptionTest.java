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
package org.apache.hop.metastore.api.exceptions;

import junit.framework.TestCase;
import org.apache.hop.metastore.api.IMetaStoreElementType;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;

/**
 * Created by saslan on 10/22/2015.
 */
public class MetaStoreElementTypeExistsExceptionTest extends TestCase {
  private MetaStoreElementTypeExistsException metaStoreElementTypeExistsException;
  private List<IMetaStoreElementType> elements;

  public void testSetDataTypes() throws Exception {
    elements = new ArrayList<>();
    IMetaStoreElementType elem1 = mock( IMetaStoreElementType.class );
    elements.add( elem1 );
    metaStoreElementTypeExistsException = new MetaStoreElementTypeExistsException( elements );
    List<IMetaStoreElementType> elements2 = new ArrayList<>();
    IMetaStoreElementType elem2 = mock( IMetaStoreElementType.class );
    elements2.add( elem2 );
    metaStoreElementTypeExistsException.setDataTypes( elements2 );
    assertEquals( metaStoreElementTypeExistsException.getDataTypes(), elements2 );

    //MetaStoreElementTypeExistsException( List<IMetaStoreElementType> dataTypes, String message ) constructor
    metaStoreElementTypeExistsException = new MetaStoreElementTypeExistsException( elements, "test" );
    metaStoreElementTypeExistsException.setDataTypes( elements2 );
    assertEquals( metaStoreElementTypeExistsException.getDataTypes(), elements2 );

    //MetaStoreElementTypeExistsException( List<IMetaStoreElementType> dataTypes, Throwable cause ) constructor
    metaStoreElementTypeExistsException = new MetaStoreElementTypeExistsException( elements, new Throwable() );
    metaStoreElementTypeExistsException.setDataTypes( elements2 );
    assertEquals( metaStoreElementTypeExistsException.getDataTypes(), elements2 );

    //MetaStoreElementTypeExistsException( List<IMetaStoreElementType> dataTypes, String message, Throwable cause ) constructor
    metaStoreElementTypeExistsException = new MetaStoreElementTypeExistsException( elements, "test", new Throwable() );
    metaStoreElementTypeExistsException.setDataTypes( elements2 );
    assertEquals( metaStoreElementTypeExistsException.getDataTypes(), elements2 );
  }
}
