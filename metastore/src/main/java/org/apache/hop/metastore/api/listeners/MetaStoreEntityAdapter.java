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

package org.apache.hop.metastore.api.listeners;

import org.apache.hop.metastore.api.IMetaStoreElement;

/**
 * Convenience listener adapter for listening to meta store entity changes.
 *
 * @author matt
 */
public class MetaStoreEntityAdapter implements IMetaStoreElementListener {

  @Override
  public void elementUpdated( String namespace, String dataTypeId, IMetaStoreElement oldEntity,
                              IMetaStoreElement newEntity ) {
  }

  @Override
  public void elementCreated( String namespace, String dataTypeId, IMetaStoreElement entity ) {
  }

  @Override
  public void elementDeleted( String namespace, String dataTypeId, IMetaStoreElement entity ) {
  }

}
