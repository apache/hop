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

package org.apache.hop.metastore.api;

import org.apache.hop.metastore.api.exceptions.MetaStoreException;

/**
 * This interface is used to describe objects of this type that are stored in the metastore.
 *
 * @author matt
 */
public interface IMetaStoreElementType {

  /**
   * @return The name of the IMetaStore this element type belongs to.
   */
  String getMetaStoreName();

  /**
   * @param metaStoreName The name of the IMetaStore this element type belongs to.
   */
  void setMetaStoreName( String metaStoreName );

  /**
   * Gets the identifier of the element type. This identifier is unique in a namespace.
   *
   * @return The ID of the element type, unique in a namespace
   */
  String getId();

  /**
   * Set the identifier for this element type.
   *
   * @param id the id to set
   */
  void setId( String id );

  /**
   * Gets the name of the element type.
   *
   * @return The name of the element type
   */
  String getName();

  /**
   * Sets the name for the element type.
   *
   * @param name The element type name to set
   */
  void setName( String name );

  /**
   * Gets the description of the element type.
   *
   * @return The description of the element type
   */
  String getDescription();

  /**
   * Sets the description of the element type.
   *
   * @param description the description to set
   */
  void setDescription( String description );

  /**
   * Persists the element type definition to the underlying metastore.
   *
   * @throws MetaStoreException In case there is an error in the underlying store.
   */
  void save() throws MetaStoreException;
}
