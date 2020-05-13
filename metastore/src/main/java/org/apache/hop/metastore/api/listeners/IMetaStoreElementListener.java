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

package org.apache.hop.metastore.api.listeners;

import org.apache.hop.metastore.api.IMetaStoreElement;

/**
 * Set of methods that are called in various parts of the meta store element life-cycle.
 *
 * @author matt
 */
public interface IMetaStoreElementListener {

  /**
   * This method is called after an element was created in the store
   *
   * @param namespace     The namespace of the element
   * @param elementTypeId The element type ID of the element
   * @param element       The element that was created
   */
  void elementCreated( String namespace, String elementTypeId, IMetaStoreElement element );

  /**
   * This method is called when an element is changed
   *
   * @param namespace  The namespace of the element
   * @param elementTypeId   The element type of the element
   * @param oldElement The element before the change
   * @param newElement The element after the change
   */
  void elementUpdated( String namespace, String elementTypeId, IMetaStoreElement oldElement,
                       IMetaStoreElement newElement );

  /**
   * This method is called after an element was deleted from the store
   *
   * @param namespace The namespace of the element
   * @param elementTypeId  The element type ID of the element
   * @param element   The element that was deleted
   */
  void elementDeleted( String namespace, String elementTypeId, IMetaStoreElement element );
}
