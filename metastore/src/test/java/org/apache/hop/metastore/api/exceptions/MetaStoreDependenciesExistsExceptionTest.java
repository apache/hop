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

import java.util.ArrayList;
import java.util.List;

/**
 * Created by saslan on 10/22/2015.
 */
public class MetaStoreDependenciesExistsExceptionTest extends TestCase {
  private MetaStoreDependenciesExistsException metaStoreDependenciesExistsException;
  private List<String> elements;

  public void testGetDependencies() throws Exception {
    elements = new ArrayList<>();
    elements.add( "string" );
    metaStoreDependenciesExistsException = new MetaStoreDependenciesExistsException( elements );
    assertEquals( metaStoreDependenciesExistsException.getDependencies(), elements );

    //metaStoreDependenciesExistsException( List<String> entities, String message ) constructor
    metaStoreDependenciesExistsException = new MetaStoreDependenciesExistsException( elements, "test" );
    assertEquals( metaStoreDependenciesExistsException.getDependencies(), elements );

    //metaStoreDependenciesExistsException( List<String> entities, Throwable cause ) constructor
    metaStoreDependenciesExistsException = new MetaStoreDependenciesExistsException( elements, new Throwable() );
    assertEquals( metaStoreDependenciesExistsException.getDependencies(), elements );

    //metaStoreDependenciesExistsException( List<String> entities, String message, Throwable cause ) constructor
    metaStoreDependenciesExistsException = new MetaStoreDependenciesExistsException( elements, "test", new Throwable() );
    assertEquals( metaStoreDependenciesExistsException.getDependencies(), elements );
  }
}
