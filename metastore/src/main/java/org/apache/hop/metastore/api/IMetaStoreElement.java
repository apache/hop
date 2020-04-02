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

import org.apache.hop.metastore.api.security.IMetaStoreElementOwner;
import org.apache.hop.metastore.api.security.MetaStoreOwnerPermissions;

import java.util.List;

/**
 * This interface describes the element as an attribute (with children) with security on top of it.
 *
 * @author matt
 */
public interface IMetaStoreElement extends IMetaStoreAttribute, IHasName {

  /**
   * Gets the name of this element.
   *
   * @return the name of the element
   */
  String getName();

  /**
   * Sets the name for this element.
   *
   * @param name the new name
   */
  void setName( String name );

  /**
   * Gets the element type.
   *
   * @return the element type
   */
  IMetaStoreElementType getElementType();

  /**
   * Sets the element type.
   *
   * @param elementType the new element type
   */
  void setElementType( IMetaStoreElementType elementType );

  /**
   * Gets the owner of this element.
   *
   * @return the owner
   */
  IMetaStoreElementOwner getOwner();

  /**
   * Sets the owner for this element.
   *
   * @param owner the new owner
   */
  void setOwner( IMetaStoreElementOwner owner );

  /**
   * Gets the owner permissions list for this element.
   *
   * @return the owner permissions list
   */
  List<MetaStoreOwnerPermissions> getOwnerPermissionsList();

  /**
   * Sets the owner permissions list for this element.
   *
   * @param ownerPermissions the new owner permissions list
   */
  void setOwnerPermissionsList( List<MetaStoreOwnerPermissions> ownerPermissions );

}
