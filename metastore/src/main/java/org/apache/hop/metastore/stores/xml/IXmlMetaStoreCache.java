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
package org.apache.hop.metastore.stores.xml;

import org.apache.hop.metastore.api.IMetaStoreElementType;

import java.util.Map;

/**
 * This interface describes cache object for XmlMetaStore.
 */
public interface IXmlMetaStoreCache {

  /**
   * Register elementType id with elementTypeName
   *
   * @param elementTypeName the type's name
   * @param elementTypeId   the type's id
   */
  void registerElementTypeIdForName( String elementTypeName, String elementTypeId );

  /**
   * Find an elementType id by elementTypeName
   *
   * @param elementTypeName the type's name
   * @return the type's id or null if the type name couldn't be found in cache
   */
  String getElementTypeIdByName( String elementTypeName );

  /**
   * Unregister an elementType id
   *
   * @param elementTypeId the id of the type to remove
   */
  void unregisterElementTypeId( String elementTypeId );

  /**
   * Register an element id with elementType and elementTypeName
   *
   * @param elementType the element type to use
   * @param elementName the element's name
   * @param elementId   the element's id. IXmlMetaStoreCache doesn't register element's id with null value.
   */
  void registerElementIdForName( IMetaStoreElementType elementType, String elementName, String elementId );

  /**
   * Find an element id with elementType and elementTypeName
   *
   * @param elementType the element type to search
   * @param elementName the element's name
   * @return the element's id or null if no element name could be matched
   */
  String getElementIdByName( IMetaStoreElementType elementType, String elementName );

  /**
   * Unregister an element id an elementType
   *
   * @param elementType the element type to use
   * @param elementId   the id of the element to remove
   */
  void unregisterElementId( IMetaStoreElementType elementType, String elementId );

  /**
   * Register processed file with fullPath and last modified date
   *
   * @param fullPath     the full path to file including its name
   * @param lastModified the last modified time
   */
  void registerProcessedFile( String fullPath, long lastModified );

  /**
   * @return map [full path -> last modified time] of registered processed files.
   */
  Map<String, Long> getProcessedFiles();

  /**
   * Unregister processed file with fullPath
   *
   * @param fullPath the full path to file including its name
   */
  void unregisterProcessedFile( String fullPath );

  /**
   * Clear the cache.
   */
  void clear();

}
