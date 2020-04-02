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

import org.apache.hop.metastore.api.exceptions.MetaStoreDependenciesExistsException;
import org.apache.hop.metastore.api.exceptions.MetaStoreElementExistException;
import org.apache.hop.metastore.api.exceptions.MetaStoreElementTypeExistsException;
import org.apache.hop.metastore.api.exceptions.MetaStoreException;
import org.apache.hop.metastore.api.exceptions.MetaStoreNamespaceExistsException;
import org.apache.hop.metastore.api.security.IMetaStoreElementOwner;
import org.apache.hop.metastore.api.security.ITwoWayPasswordEncoder;
import org.apache.hop.metastore.api.security.MetaStoreElementOwnerType;

import java.util.List;

/**
 * This interface describes how metadata can be stored and retrieved in a persistence agnostic way.
 * <p>
 * It can be used to store and retrieve all sorts of data and metadata through a key/value interface which is typed and
 * supports namespaces.
 *
 * @author matt
 */
public interface IMetaStore {

  /**
   * @return A list of all defined namespaces in the metastore
   * @throws MetaStoreException in case there is a problem in the underlying store
   */
  List<String> getNamespaces() throws MetaStoreException;

  /**
   * Create a namespace in the metastore
   *
   * @param namespace the namespace to create
   * @throws MetaStoreException                in case there is a problem in the underlying store
   * @throws MetaStoreNamespaceExistsException in case the namespace already exists
   */
  void createNamespace( String namespace ) throws MetaStoreException, MetaStoreNamespaceExistsException;

  /**
   * Delete a namespace
   *
   * @param namespace The namespace to delete
   * @throws MetaStoreException                   in case there is a problem in the underlying store
   * @throws MetaStoreDependenciesExistsException In case the namespace is not empty and contains element types. The exception contains the namespaces as
   *                                              dependencies in that case.
   */
  void deleteNamespace( String namespace ) throws MetaStoreException, MetaStoreDependenciesExistsException;

  /**
   * Validate if a namespace exists.
   *
   * @param namespace The namespace to verify for existance
   * @return True if the namespace exists, false otherwise
   * @throws MetaStoreException in case there is a problem in the underlying store
   */
  boolean namespaceExists( String namespace ) throws MetaStoreException;

  /**
   * @param namespace the namespace to look in.
   * @return A list with all the defined element types in a namespace
   * @throws MetaStoreException in case there is a problem in the underlying store
   */
  List<IMetaStoreElementType> getElementTypes( String namespace ) throws MetaStoreException;

  /**
   * @param namespace the namespace to look in.
   * @return A list with the IDs of all the defined element types in a namespace
   * @throws MetaStoreException in case there is a problem in the underlying store
   */
  List<String> getElementTypeIds( String namespace ) throws MetaStoreException;

  /**
   * @param namespace     the namespace to look in.
   * @param elementTypeId the ID of the element type to reference
   * @return An element type or null if the type ID couldn't be found.
   * @throws MetaStoreException in case there is a problem in the underlying store
   */
  IMetaStoreElementType getElementType( String namespace, String elementTypeId ) throws MetaStoreException;

  /**
   * @param namespace       the namespace to look in.
   * @param elementTypeName the name of the element type to reference
   * @return An element type or null if the type name couldn't be found.
   * @throws MetaStoreException in case there is a problem in the underlying store
   */
  IMetaStoreElementType getElementTypeByName( String namespace, String elementTypeName )
    throws MetaStoreException;

  /**
   * Create a new element type in the metastore
   *
   * @param namespace   The namespace to create the type in
   * @param elementType The type to create
   * @throws MetaStoreElementTypeExistsException in case a type with the same ID already exists in the specified namespace
   * @throws MetaStoreException                  in case there is a problem in the underlying store
   */
  void createElementType( String namespace, IMetaStoreElementType elementType ) throws MetaStoreException,
    MetaStoreElementTypeExistsException;

  /**
   * Update an element type in the metastore
   *
   * @param namespace   The namespace to update the type in
   * @param elementType The type to update
   * @throws MetaStoreException in case there is a problem in the underlying store or if the type doesn't exist.
   */
  void updateElementType( String namespace, IMetaStoreElementType elementType ) throws MetaStoreException;

  /**
   * Delete an element type from a namespace
   *
   * @param namespace   The namespace to delete the type from
   * @param elementType The element type delete.
   * @throws MetaStoreException                   in case there is a problem in the underlying store
   * @throws MetaStoreDependenciesExistsException In case the type is not empty and contains elements. The exception contains the element IDs as
   *                                              dependencies in that case.
   */
  void deleteElementType( String namespace, IMetaStoreElementType elementType ) throws MetaStoreException,
    MetaStoreDependenciesExistsException;

  /**
   * Retrieve all the elements belonging to an element type
   *
   * @param namespace   The namespace to reference
   * @param elementType The type of element to retrieve
   * @return A list of entities
   * @throws MetaStoreException in case there is a problem in the underlying store
   */
  List<IMetaStoreElement> getElements( String namespace, IMetaStoreElementType elementType )
    throws MetaStoreException;

  /**
   * Retrieve all the element IDs belonging to a meta store element type
   *
   * @param namespace   The namespace to reference
   * @param elementType The element type
   * @return A list of element IDs
   * @throws MetaStoreException in case there is a problem in the underlying store
   */
  List<String> getElementIds( String namespace, IMetaStoreElementType elementType ) throws MetaStoreException;

  /**
   * Load the meta store element with the specified namespace, element type ID and element ID
   *
   * @param namespace   The namespace
   * @param elementType The type
   * @param elementId   The element ID
   * @return The element or null if it wasn't found.
   * @throws MetaStoreException in case there is a problem in the underlying store
   */
  IMetaStoreElement getElement( String namespace, IMetaStoreElementType elementType, String elementId )
    throws MetaStoreException;

  /**
   * Find an element in a namespace with a particular type, using its name
   *
   * @param namespace   The namespace to reference
   * @param elementType The element type to search
   * @param name        The name fo look for
   * @return The first encountered element with the given name or null if no element name could be matched.
   * @throws MetaStoreException in case there is a problem in the underlying store
   */
  IMetaStoreElement getElementByName( String namespace, IMetaStoreElementType elementType, String name )
    throws MetaStoreException;

  /**
   * Create a new element for a element type in a namespace
   *
   * @param namespace   The namespace to reference
   * @param elementType The element type to use
   * @param element     The element to create
   * @throws MetaStoreException             in case there is a problem in the underlying store
   * @throws MetaStoreElementExistException In case an element with the same ID already exists.
   */
  void createElement( String namespace, IMetaStoreElementType elementType, IMetaStoreElement element )
    throws MetaStoreException, MetaStoreElementExistException;

  /**
   * Remove this element from the metastore in the specified namespace and element type.
   *
   * @param namespace   The namespace to reference
   * @param elementType The element type to use
   * @param elementId   The ID of the element to remove
   * @throws MetaStoreException in case there is a problem in the underlying store
   */
  void deleteElement( String namespace, IMetaStoreElementType elementType, String elementId )
    throws MetaStoreException;

  /**
   * Update this element in the metastore with the specified namespace and element type.
   *
   * @param namespace   The namespace to reference
   * @param elementType The element type to update
   * @param elementId   The id of the old version of the element to update, make sure it has the ID of an existing element!
   * @param element     The element to update, make sure it has the ID of an existing element!
   * @throws MetaStoreException in case there is a problem in the underlying store
   */
  void updateElement( String namespace, IMetaStoreElementType elementType, String elementId,
                             IMetaStoreElement element ) throws MetaStoreException;

  /**
   * Have the meta store generate a new element type for you in the specified namespace
   *
   * @param namespace The namespace to create the element type in
   * @return The new element type. To actually create it, use createElementType();
   * @throws MetaStoreException in case something unexpected happens in a bad way.
   */
  IMetaStoreElementType newElementType( String namespace ) throws MetaStoreException;

  /**
   * Have the meta store generate a new empty element type for you
   *
   * @return A new element, to create it in a element type, use createElement()
   * @throws MetaStoreException in case something unexpected happens in a bad way.
   */
  IMetaStoreElement newElement() throws MetaStoreException;

  /**
   * Have the meta store generate a new element for you with specified ID and value.
   *
   * @param elementType the type of the element to instantiate.
   * @param id          the id or key of the element
   * @param value       the value of the element
   * @return A new element, to create it in a element type, use createElement()
   * @throws MetaStoreException in case something unexpected happens in a bad way.
   */
  IMetaStoreElement newElement( IMetaStoreElementType elementType, String id, Object value )
    throws MetaStoreException;

  /**
   * Create a new element attribute
   *
   * @param id    The attribute ID
   * @param value The attribute value
   * @return The new attribute
   * @throws MetaStoreException
   */
  IMetaStoreAttribute newAttribute( String id, Object value ) throws MetaStoreException;

  /**
   * Have the meta store generate a new element owner for you with specified name and type.
   *
   * @param name      The owner name
   * @param ownerType The owner type
   * @return A newly generated element owner
   * @throws MetaStoreException In case something unexpected happens in a bad way.
   */
  IMetaStoreElementOwner newElementOwner( String name, MetaStoreElementOwnerType ownerType )
    throws MetaStoreException;

  /**
   * @return The name of the meta store
   * @throws MetaStoreException in case there is a problem in the underlying store
   */
  String getName() throws MetaStoreException;

  /**
   * @return The description of the meta store
   * @throws MetaStoreException in case there is a problem in the underlying store
   */
  String getDescription() throws MetaStoreException;

  /**
   * @param encoder The password encoder to use in this MetaStore
   */
  void setTwoWayPasswordEncoder( ITwoWayPasswordEncoder encoder );

  /**
   * @return The password encoder used by the MetaStore.
   */
  ITwoWayPasswordEncoder getTwoWayPasswordEncoder();
}
