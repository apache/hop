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

import org.apache.hop.metastore.api.BaseMetaStore;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.metastore.api.IMetaStoreAttribute;
import org.apache.hop.metastore.api.IMetaStoreElement;
import org.apache.hop.metastore.api.IMetaStoreElementType;
import org.apache.hop.metastore.api.exceptions.MetaStoreDependenciesExistsException;
import org.apache.hop.metastore.api.exceptions.MetaStoreElementExistException;
import org.apache.hop.metastore.api.exceptions.MetaStoreElementTypeExistsException;
import org.apache.hop.metastore.api.exceptions.MetaStoreException;
import org.apache.hop.metastore.api.security.IMetaStoreElementOwner;
import org.apache.hop.metastore.api.security.MetaStoreElementOwnerType;
import org.apache.hop.metastore.util.MetaStoreUtil;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

public class MemoryMetaStore extends BaseMetaStore implements IMetaStore {

  private final MemoryMetaStoreContent content;

  private final ReadLock readLock;
  private final WriteLock writeLock;

  public MemoryMetaStore() {
    content = new MemoryMetaStoreContent();

    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    readLock = lock.readLock();
    writeLock = lock.writeLock();
  }

  @Override
  public boolean equals( Object obj ) {
    if ( this == obj ) {
      return true;
    }
    if ( !( obj instanceof MemoryMetaStore ) ) {
      return false;
    }
    return ( (MemoryMetaStore) obj ).name.equalsIgnoreCase( name );
  }

  @Override
  public List<IMetaStoreElementType> getElementTypes() throws MetaStoreException {
    return MetaStoreUtil.executeLockedOperation( readLock, () -> content.getElementTypes() );
  }

  @Override
  public IMetaStoreElementType getElementType( final String elementTypeId )
    throws MetaStoreException {
    return MetaStoreUtil.executeLockedOperation( readLock, () -> content.getElementTypeById( elementTypeId ) );
  }

  @Override
  public IMetaStoreElementType getElementTypeByName( final String elementTypeName )
    throws MetaStoreException {
    return MetaStoreUtil.executeLockedOperation( readLock, (Callable<IMetaStoreElementType>) () -> content.getElementTypeByName( elementTypeName ) );
  }

  @Override
  public List<String> getElementTypeIds() throws MetaStoreException {
    return MetaStoreUtil.executeLockedOperation( readLock, () -> content.getElementTypeIds() );
  }

  @Override
  public void createElementType( final IMetaStoreElementType elementType )
    throws MetaStoreException, MetaStoreElementTypeExistsException {
    MetaStoreUtil.executeLockedOperation( readLock, (Callable<Void>) () -> {
      content.createElementType( getName(), elementType );
      return null;
    } );
  }

  @Override
  public void updateElementType( final IMetaStoreElementType elementType )
    throws MetaStoreException {
    MetaStoreUtil.executeLockedOperation( readLock, (Callable<Void>) () -> {
      content.updateElementType( getName(), elementType );
      return null;
    } );
  }

  @Override
  public void deleteElementType( final IMetaStoreElementType elementType )
    throws MetaStoreException, MetaStoreDependenciesExistsException {
    MetaStoreUtil.executeLockedOperation( readLock, (Callable<Void>) () -> {
      content.deleteElementType( elementType );
      return null;
    } );
  }

  @Override
  public List<IMetaStoreElement> getElements( final IMetaStoreElementType elementType )
    throws MetaStoreException {
    return MetaStoreUtil.executeLockedOperation( readLock, () -> content.getElementsByElementTypeName( elementType.getName() ) );
  }

  @Override
  public List<String> getElementIds( final IMetaStoreElementType elementType )
    throws MetaStoreException {
    return MetaStoreUtil.executeLockedOperation( readLock, () -> content.getElementIdsByElementTypeName( elementType.getName() ) );
  }

  @Override
  public IMetaStoreElement getElement( final IMetaStoreElementType elementType,
                                       final String elementId ) throws MetaStoreException {
    return MetaStoreUtil.executeLockedOperation( readLock, () -> content.getElementByTypeNameId( elementType.getName(), elementId ) );
  }

  @Override
  public IMetaStoreElement getElementByName( final IMetaStoreElementType elementType,
                                             final String name ) throws MetaStoreException {
    return MetaStoreUtil.executeLockedOperation( readLock, () -> content.getElementByNameTypeName( elementType.getName(), name ) );
  }

  @Override
  public void createElement( final IMetaStoreElementType elementType,
                             final IMetaStoreElement element ) throws MetaStoreException, MetaStoreElementExistException {
    MetaStoreUtil.executeLockedOperation( readLock, (Callable<Void>) () -> {
      content.createElement( elementType, element );
      return null;
    } );
  }

  @Override
  public void updateElement( final IMetaStoreElementType elementType, final String elementId,
                             final IMetaStoreElement element ) throws MetaStoreException {

    // verify that the element type belongs to this meta store
    //
    if ( elementType.getMetaStoreName() == null || !elementType.getMetaStoreName().equals( getName() ) ) {
      throw new MetaStoreException( "The element type '" + elementType.getName()
        + "' needs to explicitly belong to the meta store in which you are updating." );
    }

    MetaStoreUtil.executeLockedOperation( readLock, (Callable<Void>) () -> {
      content.updateElement( elementType, elementId, element );
      return null;
    } );
  }

  @Override
  public void deleteElement( final IMetaStoreElementType elementType, final String elementId )
    throws MetaStoreException {
    MetaStoreUtil.executeLockedOperation( readLock, (Callable<Void>) () -> {
        content.deleteElement( elementType, elementId );
        return null;
    } );
  }

  @Override
  public IMetaStoreElementType newElementType() throws MetaStoreException {
    return new MemoryMetaStoreElementType();
  }

  @Override
  public IMetaStoreElement newElement() throws MetaStoreException {
    return new MemoryMetaStoreElement();
  }

  @Override
  public IMetaStoreElement newElement( IMetaStoreElementType elementType, String id, Object value )
    throws MetaStoreException {
    return new MemoryMetaStoreElement( elementType, id, value );
  }

  public IMetaStoreAttribute newAttribute( String id, Object value ) throws MetaStoreException {
    return new MemoryMetaStoreAttribute( id, value );
  }

  @Override
  public IMetaStoreElementOwner newElementOwner( String name, MetaStoreElementOwnerType ownerType )
    throws MetaStoreException {
    return new MemoryMetaStoreElementOwner( name, ownerType );
  }

}
