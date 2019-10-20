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

package org.apache.hop.metastore.api.listeners;

import junit.framework.TestCase;
import org.apache.hop.metastore.api.IMetaStoreElementType;

import static org.mockito.Mockito.mock;

/**
 * Created by saslan on 10/23/2015.
 */
public class MetaStoreDataTypeAdapterTest extends TestCase {
  private MetaStoreDataTypeAdapter metaStoreDataTypeAdapter;
  private IMetaStoreElementType iMetaStoreElementType;

  public void setUp() throws Exception {
    metaStoreDataTypeAdapter = new MetaStoreDataTypeAdapter();
    iMetaStoreElementType = mock( IMetaStoreElementType.class );
  }

  public void testDataTypeCreated() throws Exception {
    metaStoreDataTypeAdapter.dataTypeCreated( "namespace", iMetaStoreElementType );
  }

  public void testDataTypeDeleted() throws Exception {
    metaStoreDataTypeAdapter.dataTypeDeleted( "namespace", iMetaStoreElementType );
  }

  public void testDataTypeUpdated() throws Exception {
    IMetaStoreElementType newIMetaStoreElementType = mock( IMetaStoreElementType.class );
    metaStoreDataTypeAdapter.dataTypeUpdated( "namespace", iMetaStoreElementType, newIMetaStoreElementType );
  }
}
