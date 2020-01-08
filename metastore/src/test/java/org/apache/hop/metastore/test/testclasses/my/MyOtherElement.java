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
package org.apache.hop.metastore.test.testclasses.my;

import org.apache.hop.metastore.persist.MetaStoreAttribute;
import org.apache.hop.metastore.persist.MetaStoreElementType;

@MetaStoreElementType(
  name = "My other element type",
  description = "Another element type to test with" )
public class MyOtherElement {
  private String name;

  @MetaStoreAttribute( key = "some_attribute" )
  private String someAttribute;

  /**
   * Empty constructor is needed by MetaStoreFactory to instantiate this object
   */
  public MyOtherElement() {
  }

  /**
   * @param name
   * @param someAttribute
   */
  public MyOtherElement( String name, String someAttribute ) {
    this();
    this.name = name;
    this.someAttribute = someAttribute;
  }

  /**
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name the name to set
   */
  public void setName( String name ) {
    this.name = name;
  }

  /**
   * @return the someAttribute
   */
  public String getSomeAttribute() {
    return someAttribute;
  }

  /**
   * @param someAttribute the someAttribute to set
   */
  public void setSomeAttribute( String someAttribute ) {
    this.someAttribute = someAttribute;
  }
}
