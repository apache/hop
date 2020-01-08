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
public interface XmlMetaStoreCache {

  /**
   * Register elementType id in a namespace with elementTypeName
   *
   * @param namespace       the namespace to register the type in
   * @param elementTypeName the type's name
   * @param elementTypeId   the type's id
   */
  void registerElementTypeIdForName( String namespace, String elementTypeName, String elementTypeId );

  /**
   * Find an elementType id in a namespace by elementTypeName
   *
   * @param namespace       the namespace to look in
   * @param elementTypeName the type's name
   * @return the type's id or null if the type name couldn't be found in cache
   */
  String getElementTypeIdByName( String namespace, String elementTypeName );

  /**
   * Unregister an elementType id in namespace
   *
   * @param namespace     the namespace to look in
   * @param elementTypeId the id of the type to remove
   */
  void unregisterElementTypeId( String namespace, String elementTypeId );

  /**
   * Register an element id in a namespace with elementType and elementTypeName
   *
   * @param namespace   the namespace to reference
   * @param elementType the element type to use
   * @param elementName the element's name
   * @param elementId   the element's id. XmlMetaStoreCache doesn't register element's id with null value.
   */
  void registerElementIdForName( String namespace, IMetaStoreElementType elementType, String elementName, String elementId );

  /**
   * Find an element id in a namespace with elementType and elementTypeName
   *
   * @param namespace   the namespace to look in
   * @param elementType the element type to search
   * @param elementName the element's name
   * @return the element's id or null if no element name could be matched
   */
  String getElementIdByName( String namespace, IMetaStoreElementType elementType, String elementName );

  /**
   * Unregister an element id for namespace and elementType
   *
   * @param namespace   the namespace to reference
   * @param elementType the element type to use
   * @param elementId   the id of the element to remove
   */
  void unregisterElementId( String namespace, IMetaStoreElementType elementType, String elementId );

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
