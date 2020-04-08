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
import org.apache.hop.metastore.api.exceptions.MetaStoreNamespaceExistsException;
import org.apache.hop.metastore.api.security.IMetaStoreElementOwner;
import org.apache.hop.metastore.api.security.MetaStoreElementOwnerType;
import org.apache.hop.metastore.util.MetaStoreUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

public class MemoryMetaStore extends BaseMetaStore implements IMetaStore {

  private final Map<String, MemoryMetaStoreNamespace> namespacesMap;

  private final ReadLock readLock;
  private final WriteLock writeLock;

  public MemoryMetaStore() {
    namespacesMap = new HashMap<String, MemoryMetaStoreNamespace>();

    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    readLock = lock.readLock();
    writeLock = lock.writeLock();
  }

  @Override
  public List<String> getNamespaces() throws MetaStoreException {
    return MetaStoreUtil.executeLockedOperation( readLock, new Callable<List<String>>() {

      @Override
      public List<String> call() throws Exception {
        return new ArrayList<>( namespacesMap.keySet() );
      }
    } );
  }

  @Override
  public boolean namespaceExists( final String namespace ) throws MetaStoreException {
    return MetaStoreUtil.executeLockedOperation( readLock, new Callable<Boolean>() {

      @Override
      public Boolean call() throws Exception {
        return namespacesMap.get( namespace ) != null;
      }
    } );

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
  public void createNamespace( final String namespace ) throws MetaStoreException, MetaStoreNamespaceExistsException {
    MetaStoreUtil.executeLockedOperation( writeLock, new Callable<Void>() {

      @Override
      public Void call() throws Exception {
        if ( namespacesMap.containsKey( namespace ) ) {
          throw new MetaStoreNamespaceExistsException( "Unable to create namespace '" + namespace
            + "' as it already exists!" );
        } else {
          MemoryMetaStoreNamespace storeNamespace = new MemoryMetaStoreNamespace( namespace );
          namespacesMap.put( namespace, storeNamespace );
        }
        return null;
      }
    } );

  }

  @Override
  public void deleteNamespace( final String namespace ) throws MetaStoreException, MetaStoreDependenciesExistsException {

    MetaStoreUtil.executeLockedOperation( writeLock, new Callable<Void>() {

      @Override
      public Void call() throws Exception {
        final MemoryMetaStoreNamespace storeNamespace = namespacesMap.get( namespace );

        if ( storeNamespace == null ) {
          throw new MetaStoreException( "Unable to delete namespace '" + namespace + "' as it doesn't exist" );
        }

        MetaStoreUtil.executeLockedOperation( storeNamespace.getReadLock(), new Callable<Void>() {

          @Override
          public Void call() throws Exception {
            List<String> elementTypeIds = storeNamespace.getElementTypeIds();
            if ( elementTypeIds.isEmpty() ) {
              namespacesMap.remove( namespace );
            } else {
              throw new MetaStoreDependenciesExistsException( elementTypeIds, "Namespace '" + namespace
                + "' is not empty!" );
            }
            return null;
          }
        } );

        return null;
      }
    } );

  }

  @Override
  public List<IMetaStoreElementType> getElementTypes( final String namespace ) throws MetaStoreException {
    return MetaStoreUtil.executeLockedOperation( readLock, new Callable<List<IMetaStoreElementType>>() {

      @Override
      public List<IMetaStoreElementType> call() throws Exception {
        MemoryMetaStoreNamespace storeNamespace = namespacesMap.get( namespace );
        if ( storeNamespace != null ) {
          return storeNamespace.getElementTypes();
        }
        return Collections.emptyList();
      }

    } );
  }

  @Override
  public IMetaStoreElementType getElementType( final String namespace, final String elementTypeId )
    throws MetaStoreException {
    return MetaStoreUtil.executeLockedOperation( readLock, new Callable<IMetaStoreElementType>() {

      @Override
      public IMetaStoreElementType call() throws Exception {
        MemoryMetaStoreNamespace storeNamespace = namespacesMap.get( namespace );
        if ( storeNamespace != null ) {
          return storeNamespace.getElementTypeById( elementTypeId );
        }
        return null;
      }

    } );
  }

  @Override
  public IMetaStoreElementType getElementTypeByName( final String namespace, final String elementTypeName )
    throws MetaStoreException {
    return MetaStoreUtil.executeLockedOperation( readLock, new Callable<IMetaStoreElementType>() {

      @Override
      public IMetaStoreElementType call() throws Exception {
        MemoryMetaStoreNamespace storeNamespace = namespacesMap.get( namespace );
        if ( storeNamespace != null ) {
          return storeNamespace.getElementTypeByName( elementTypeName );
        }
        return null;
      }
    } );
  }

  @Override
  public List<String> getElementTypeIds( final String namespace ) throws MetaStoreException {
    return MetaStoreUtil.executeLockedOperation( readLock, new Callable<List<String>>() {

      @Override
      public List<String> call() throws Exception {
        MemoryMetaStoreNamespace storeNamespace = namespacesMap.get( namespace );
        if ( storeNamespace != null ) {
          return storeNamespace.getElementTypeIds();
        }
        return Collections.emptyList();
      }
    } );
  }

  @Override
  public void createElementType( final String namespace, final IMetaStoreElementType elementType )
    throws MetaStoreException, MetaStoreElementTypeExistsException {
    MetaStoreUtil.executeLockedOperation( readLock, new Callable<Void>() {

      @Override
      public Void call() throws Exception {
        MemoryMetaStoreNamespace storeNamespace = namespacesMap.get( namespace );
        if ( storeNamespace != null ) {
          storeNamespace.createElementType( getName(), elementType );
        } else {
          throw new MetaStoreException( "Namespace '" + namespace + "' doesn't exist!" );
        }
        return null;
      }
    } );
  }

  @Override
  public void updateElementType( final String namespace, final IMetaStoreElementType elementType )
    throws MetaStoreException {
    MetaStoreUtil.executeLockedOperation( readLock, new Callable<Void>() {

      @Override
      public Void call() throws Exception {
        MemoryMetaStoreNamespace storeNamespace = namespacesMap.get( namespace );
        if ( storeNamespace != null ) {
          storeNamespace.updateElementType( getName(), elementType );
        } else {
          throw new MetaStoreException( "Namespace '" + namespace + "' doesn't exist!" );
        }
        return null;
      }
    } );
  }

  @Override
  public void deleteElementType( final String namespace, final IMetaStoreElementType elementType )
    throws MetaStoreException, MetaStoreDependenciesExistsException {
    MetaStoreUtil.executeLockedOperation( readLock, new Callable<Void>() {

      @Override
      public Void call() throws Exception {
        MemoryMetaStoreNamespace storeNamespace = namespacesMap.get( namespace );
        if ( storeNamespace != null ) {
          storeNamespace.deleteElementType( elementType );
        } else {
          throw new MetaStoreException( "Namespace '" + namespace + "' doesn't exist!" );
        }
        return null;
      }
    } );
  }

  @Override
  public List<IMetaStoreElement> getElements( final String namespace, final IMetaStoreElementType elementType )
    throws MetaStoreException {
    return MetaStoreUtil.executeLockedOperation( readLock, new Callable<List<IMetaStoreElement>>() {

      @Override
      public List<IMetaStoreElement> call() throws Exception {
        MemoryMetaStoreNamespace storeNamespace = namespacesMap.get( namespace );
        if ( storeNamespace != null ) {
          return storeNamespace.getElementsByElementTypeName( elementType.getName() );
        }
        return Collections.emptyList();
      }
    } );
  }

  @Override
  public List<String> getElementIds( final String namespace, final IMetaStoreElementType elementType )
    throws MetaStoreException {
    return MetaStoreUtil.executeLockedOperation( readLock, new Callable<List<String>>() {

      @Override
      public List<String> call() throws Exception {
        MemoryMetaStoreNamespace storeNamespace = namespacesMap.get( namespace );
        if ( storeNamespace != null ) {
          return storeNamespace.getElementIdsByElementTypeName( elementType.getName() );
        }
        return Collections.emptyList();
      }
    } );
  }

  @Override
  public IMetaStoreElement getElement( final String namespace, final IMetaStoreElementType elementType,
                                       final String elementId ) throws MetaStoreException {
    return MetaStoreUtil.executeLockedOperation( readLock, new Callable<IMetaStoreElement>() {

      @Override
      public IMetaStoreElement call() throws Exception {
        MemoryMetaStoreNamespace storeNamespace = namespacesMap.get( namespace );
        if ( storeNamespace != null ) {
          return storeNamespace.getElementByTypeNameId( elementType.getName(), elementId );
        }
        return null;
      }
    } );
  }

  @Override
  public IMetaStoreElement getElementByName( final String namespace, final IMetaStoreElementType elementType,
                                             final String name ) throws MetaStoreException {
    return MetaStoreUtil.executeLockedOperation( readLock, new Callable<IMetaStoreElement>() {

      @Override
      public IMetaStoreElement call() throws Exception {
        MemoryMetaStoreNamespace storeNamespace = namespacesMap.get( namespace );
        if ( storeNamespace != null ) {
          return storeNamespace.getElementByNameTypeName( elementType.getName(), name );
        }
        return null;
      }
    } );
  }

  @Override
  public void createElement( final String namespace, final IMetaStoreElementType elementType,
                             final IMetaStoreElement element ) throws MetaStoreException, MetaStoreElementExistException {
    MetaStoreUtil.executeLockedOperation( readLock, new Callable<Void>() {

      @Override
      public Void call() throws Exception {
        MemoryMetaStoreNamespace storeNamespace = namespacesMap.get( namespace );
        if ( storeNamespace != null ) {
          storeNamespace.createElement( elementType, element );
        } else {
          throw new MetaStoreException( "Namespace '" + namespace + "' doesn't exist!" );
        }
        return null;
      }
    } );
  }

  @Override
  public void updateElement( final String namespace, final IMetaStoreElementType elementType, final String elementId,
                             final IMetaStoreElement element ) throws MetaStoreException {

    // verify that the element type belongs to this meta store
    //
    if ( elementType.getMetaStoreName() == null || !elementType.getMetaStoreName().equals( getName() ) ) {
      throw new MetaStoreException( "The element type '" + elementType.getName()
        + "' needs to explicitly belong to the meta store in which you are updating." );
    }

    MetaStoreUtil.executeLockedOperation( readLock, new Callable<Void>() {

      @Override
      public Void call() throws Exception {
        MemoryMetaStoreNamespace storeNamespace = namespacesMap.get( namespace );
        if ( storeNamespace != null ) {
          storeNamespace.updateElement( elementType, elementId, element );
        } else {
          throw new MetaStoreException( "Namespace '" + namespace + "' doesn't exist!" );
        }
        return null;
      }
    } );
  }

  @Override
  public void deleteElement( final String namespace, final IMetaStoreElementType elementType, final String elementId )
    throws MetaStoreException {
    MetaStoreUtil.executeLockedOperation( readLock, new Callable<Void>() {

      @Override
      public Void call() throws Exception {
        MemoryMetaStoreNamespace storeNamespace = namespacesMap.get( namespace );
        if ( storeNamespace != null ) {
          storeNamespace.deleteElement( elementType, elementId );
        } else {
          throw new MetaStoreException( "Namespace '" + namespace + "' doesn't exist!" );
        }
        return null;
      }
    } );
  }

  @Override
  public IMetaStoreElementType newElementType( String namespace ) throws MetaStoreException {
    return new MemoryMetaStoreElementType( namespace );
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
