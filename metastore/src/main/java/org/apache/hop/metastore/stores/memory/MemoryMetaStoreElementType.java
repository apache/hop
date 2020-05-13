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

package org.apache.hop.metastore.stores.memory;

import org.apache.hop.metastore.api.BaseElementType;
import org.apache.hop.metastore.api.IMetaStoreElement;
import org.apache.hop.metastore.api.IMetaStoreElementType;
import org.apache.hop.metastore.api.exceptions.MetaStoreException;
import org.apache.hop.metastore.util.MetaStoreUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

public class MemoryMetaStoreElementType extends BaseElementType {

  private final Map<String, MemoryMetaStoreElement> elementMap = new HashMap<String, MemoryMetaStoreElement>();

  private final ReadLock readLock;
  private final WriteLock writeLock;

  {
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    readLock = lock.readLock();
    writeLock = lock.writeLock();
  }

  public MemoryMetaStoreElementType() {
    super();
  }

  /**
   * Copy data from another meta store persistence type...
   *
   * @param elementType The type to copy over.
   */
  public MemoryMetaStoreElementType( IMetaStoreElementType elementType ) {
    this();
    copyFrom( elementType );
  }

  @Override
  public void save() throws MetaStoreException {
    // Nothing to save.
  }

  public Map<String, MemoryMetaStoreElement> getElementMap() {
    return MetaStoreUtil.executeLockedOperationQuietly( readLock, new Callable<Map<String, MemoryMetaStoreElement>>() {

      @Override
      public Map<String, MemoryMetaStoreElement> call() throws Exception {
        return new HashMap<String, MemoryMetaStoreElement>( elementMap );
      }
    } );
  }

  public List<String> getElementIds() {
    return MetaStoreUtil.executeLockedOperationQuietly( readLock, new Callable<List<String>>() {

      @Override
      public List<String> call() throws Exception {
        List<String> ids = new ArrayList<>();
        for ( String id : elementMap.keySet() ) {
          ids.add( id );
        }
        return ids;
      }
    } );
  }

  public MemoryMetaStoreElement getElement( final String elementId ) {
    return MetaStoreUtil.executeLockedOperationQuietly( readLock, new Callable<MemoryMetaStoreElement>() {

      @Override
      public MemoryMetaStoreElement call() throws Exception {
        return elementMap.get( elementId );
      }
    } );
  }

  protected ReadLock getReadLock() {
    return readLock;
  }

  public boolean isElementMapEmpty() {
    return MetaStoreUtil.executeLockedOperationQuietly( readLock, new Callable<Boolean>() {

      @Override
      public Boolean call() throws Exception {
        return elementMap.isEmpty();
      }
    } );
  }

  public List<IMetaStoreElement> getElements() {
    return MetaStoreUtil.executeLockedOperationQuietly( readLock, new Callable<List<IMetaStoreElement>>() {

      @Override
      public List<IMetaStoreElement> call() throws Exception {
        return new ArrayList<IMetaStoreElement>( elementMap.values() );
      }
    } );
  }

  public IMetaStoreElement getElementByName( final String elementName ) {
    return MetaStoreUtil.executeLockedOperationQuietly( readLock, new Callable<IMetaStoreElement>() {

      @Override
      public IMetaStoreElement call() throws Exception {
        for ( MemoryMetaStoreElement element : elementMap.values() ) {
          if ( element.getName() != null && element.getName().equalsIgnoreCase( elementName ) ) {
            return element;
          }
        }
        return null;
      }
    } );
  }

  public void createElement( final IMetaStoreElement element ) {
    // For the memory store, the ID is the same as the name if empty
    if ( element.getId() == null ) {
      element.setId( element.getName() );
    }
    MetaStoreUtil.executeLockedOperationQuietly( writeLock, new Callable<Void>() {

      @Override
      public Void call() throws Exception {
        elementMap.put( element.getId(), new MemoryMetaStoreElement( element ) );
        return null;
      }
    } );
  }

  public void updateElement( final String elementId, final IMetaStoreElement element ) {
    // For the memory store, the ID is the same as the name if empty
    if ( element.getId() == null ) {
      element.setId( element.getName() );
    }
    MetaStoreUtil.executeLockedOperationQuietly( writeLock, new Callable<Void>() {

      @Override
      public Void call() throws Exception {
        elementMap.put( elementId, new MemoryMetaStoreElement( element ) );
        return null;
      }
    } );
  }

  public void deleteElement( final String elementId ) {
    MetaStoreUtil.executeLockedOperationQuietly( writeLock, new Callable<Void>() {

      @Override
      public Void call() throws Exception {
        elementMap.remove( elementId );
        return null;
      }
    } );
  }

}
