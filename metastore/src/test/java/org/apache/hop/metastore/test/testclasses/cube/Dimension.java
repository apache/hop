/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

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
package org.apache.hop.metastore.test.testclasses.cube;

import org.apache.hop.metastore.persist.MetaStoreAttribute;
import org.apache.hop.metastore.persist.MetaStoreElementType;

import java.util.ArrayList;
import java.util.List;

@MetaStoreElementType( name = "Dimension", description = "A dimension" )
public class Dimension {

  private String name;

  @MetaStoreAttribute
  private List<DimensionAttribute> attributes;

  @MetaStoreAttribute( key = "dimension_type" )
  private DimensionType dimensionType;

  @MetaStoreAttribute
  private boolean shared;

  public Dimension() {
    attributes = new ArrayList<DimensionAttribute>();
    shared = true;
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
   * @return the attributes
   */
  public List<DimensionAttribute> getAttributes() {
    return attributes;
  }

  /**
   * @param attributes the attributes to set
   */
  public void setAttributes( List<DimensionAttribute> attributes ) {
    this.attributes = attributes;
  }

  /**
   * @return the dimensionType
   */
  public DimensionType getDimensionType() {
    return dimensionType;
  }

  /**
   * @param dimensionType the dimensionType to set
   */
  public void setDimensionType( DimensionType dimensionType ) {
    this.dimensionType = dimensionType;
  }

  /**
   * @return the shared
   */
  public boolean isShared() {
    return shared;
  }

  /**
   * @param shared the shared to set
   */
  public void setShared( boolean shared ) {
    this.shared = shared;
  }
}
