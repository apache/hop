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

import java.util.List;

/**
 * The IMetaStoreAttribute interface specifies methods related to metastore attributes. A metastore attribute has an
 * identifier/key, value, and possibly child attributes, which allows for hierarchical attribute relationships.
 */
public interface IMetaStoreAttribute {

  /**
   * Gets the identifier/key of this attribute.
   * 
   * @return The ID or key of the metastore attribute
   */
  public String getId();

  /**
   * Sets the identifier/key for this attribute.
   * 
   * @param id
   *          The ID or key of the attribute to set.
   */
  public void setId( String id );

  /**
   * Gets the value object for this attribute.
   * 
   * @return The value of the attribute
   */
  public Object getValue();

  /**
   * Sets the value object for this attribute.
   * 
   * @param value
   *          The attribute value to set.
   */
  public void setValue( Object value );

  /**
   * Gets the child attributes of this attribute.
   * 
   * @return A list of the child attributes
   */
  public List<IMetaStoreAttribute> getChildren();

  /**
   * Adds a child attribute to this attribute.
   * 
   * @param attribute
   *          The attribute to add
   */
  public void addChild( IMetaStoreAttribute attribute );

  /**
   * Deletes the specified child attribute from this attribute.
   * 
   * @param attributeId
   *          The ID or key of the attribute to delete
   */
  public void deleteChild( String attributeId );

  /**
   * Removes all child attributes.
   */
  public void clearChildren();

  /**
   * Retrieves the child attribute with the specified identifier/key.
   * 
   * @param id
   *          The id of the child attribute to retrieve
   * @return The attribute value or null if the attribute doesn't exist.
   */
  public IMetaStoreAttribute getChild( String id );
}
