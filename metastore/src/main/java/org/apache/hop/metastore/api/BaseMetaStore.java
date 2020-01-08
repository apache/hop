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
import org.apache.hop.metastore.api.security.Base64TwoWayPasswordEncoder;
import org.apache.hop.metastore.api.security.ITwoWayPasswordEncoder;

/**
 * This class implements common and/or default functionality between IMetaStore instances
 */
public abstract class BaseMetaStore implements IMetaStore {

  /**
   * The name of this metastore.
   */
  protected String name;

  /**
   * The description of this metastore.
   */
  protected String description;

  protected ITwoWayPasswordEncoder passwordEncoder;

  /**
   * Instantiates a new base meta store.
   */
  public BaseMetaStore() {
    passwordEncoder = new Base64TwoWayPasswordEncoder();
  }

  /**
   * Gets the name of this metastore.
   *
   * @return the name
   */
  public String getName() throws MetaStoreException {
    return name;
  }

  /**
   * Sets the name of this metastore.
   *
   * @param name the name to set
   */
  public void setName( String name ) {
    this.name = name;
  }

  /**
   * Gets the description of this metastore.
   *
   * @return the description of this metastore
   */
  public String getDescription() throws MetaStoreException {
    return description;
  }

  /**
   * Sets the description for this metastore.
   *
   * @param description the description to set
   */
  public void setDescription( String description ) {
    this.description = description;
  }

  public ITwoWayPasswordEncoder getTwoWayPasswordEncoder() {
    return passwordEncoder;
  }

  public void setTwoWayPasswordEncoder( ITwoWayPasswordEncoder passwordEncoder ) {
    this.passwordEncoder = passwordEncoder;
  }

}
