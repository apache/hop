/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.core.attributes.metastore;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hop.core.IAttributes;
import org.apache.hop.metastore.api.BaseMetaStore;
import org.apache.hop.metastore.api.IMetaStoreAttribute;
import org.apache.hop.metastore.api.IMetaStoreElement;
import org.apache.hop.metastore.api.IMetaStoreElementType;
import org.apache.hop.metastore.api.exceptions.MetaStoreDependenciesExistsException;
import org.apache.hop.metastore.api.exceptions.MetaStoreElementExistException;
import org.apache.hop.metastore.api.exceptions.MetaStoreElementTypeExistsException;
import org.apache.hop.metastore.api.exceptions.MetaStoreException;
import org.apache.hop.metastore.api.security.IMetaStoreElementOwner;
import org.apache.hop.metastore.api.security.MetaStoreElementOwnerType;
import org.apache.hop.metastore.stores.memory.MemoryMetaStoreAttribute;
import org.apache.hop.metastore.stores.memory.MemoryMetaStoreElement;
import org.apache.hop.metastore.stores.memory.MemoryMetaStoreElementOwner;
import org.apache.hop.metastore.util.MetaStoreUtil;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author nhudak
 */
public class EmbeddedMetaStore extends BaseMetaStore implements ReadWriteLock {
  static final String METASTORE_PREFIX = "METASTORE.";

  static final String TYPE_PREFIX = "TYPE.";

  private final IAttributes IAttributes;
  private final ReadWriteLock lock;

  public EmbeddedMetaStore( IAttributes IAttributes ) {
    this.IAttributes = IAttributes;
    this.lock = new ReentrantReadWriteLock();
  }

  @Override public void createNamespace( final String namespace ) throws MetaStoreException {
    // Optional, namespace will be automatically created if not already existing
    MetaStoreUtil.executeLockedOperation( writeLock(), new Callable<Void>() {
      @Override public Void call() throws Exception {
        String groupName = JsonElementType.groupName( namespace );
        if ( !IAttributes.getAttributesMap().containsKey( groupName ) ) {
          IAttributes.setAttributes( groupName, Maps.<String, String>newHashMap() );
        }
        return null;
      }
    } );
  }

  @Override public List<String> getNamespaces() throws MetaStoreException {
    return MetaStoreUtil.executeLockedOperation( readLock(), new Callable<List<String>>() {
      @Override public List<String> call() throws Exception {
        return FluentIterable.from( IAttributes.getAttributesMap().keySet() )
          .filter( new Predicate<String>() {
            @Override public boolean apply( String groupName ) {
              return groupName.startsWith( METASTORE_PREFIX );
            }
          } )
          .transform( new Function<String, String>() {
            @Override public String apply( String input ) {
              return input.substring( METASTORE_PREFIX.length() );
            }
          } )
          .toList();
      }
    } );
  }

  @Override public boolean namespaceExists( final String namespace ) throws MetaStoreException {
    return MetaStoreUtil.executeLockedOperation( readLock(), new Callable<Boolean>() {
      @Override public Boolean call() throws Exception {
        return IAttributes.getAttributesMap().containsKey( JsonElementType.groupName( namespace ) );
      }
    } );
  }

  @Override public synchronized void deleteNamespace( final String namespace ) throws MetaStoreException {
    MetaStoreUtil.executeLockedOperation( writeLock(), new Callable<Void>() {
      @Override public Void call() throws Exception {
        List<String> dependencies = getElementTypeIds( namespace );
        if ( !dependencies.isEmpty() ) {
          throw new MetaStoreDependenciesExistsException( dependencies,
            "Unable to delete the meta store namespace '" + namespace + "' as it still contains element types" );
        }
        IAttributes.getAttributesMap().remove( JsonElementType.groupName( namespace ) );
        return null;
      }
    } );
  }

  @Override
  public JsonElementType newElementType( final String namespace ) {
    return new EmbeddedElementType( namespace );
  }

  private class EmbeddedElementType extends JsonElementType {
    public EmbeddedElementType( String namespace ) {
      super( namespace );
    }

    @Override public void save() throws MetaStoreException {
      update( this );
    }
  }

  @Override
  public void createElementType( String namespace, IMetaStoreElementType elementType ) throws MetaStoreException {
    elementType.setNamespace( namespace );
    if ( !create( JsonElementType.from( elementType ) ) ) {
      throw new MetaStoreElementTypeExistsException( getElementTypes( namespace ) );
    }
  }

  @Override public JsonElementType getElementType( final String namespace, final String elementTypeId )
    throws MetaStoreException {
    return MetaStoreUtil.executeLockedOperation( readLock(), new Callable<JsonElementType>() {
      @Override public JsonElementType call() throws Exception {
        JsonElementType type = newElementType( namespace );
        type.setId( elementTypeId );
        String jsonData = IAttributes.getAttribute( type.groupName(), type.key() );
        return jsonData == null ? null : type.load( jsonData );
      }
    } );
  }

  @Override public List<String> getElementTypeIds( final String namespace ) throws MetaStoreException {
    return MetaStoreUtil.executeLockedOperation( readLock(), new Callable<List<String>>() {
      @Override public List<String> call() throws Exception {
        Map<String, String> attributes = IAttributes.getAttributes( JsonElementType.groupName( namespace ) );
        return attributes == null ? ImmutableList.<String>of() : ImmutableList.copyOf( attributes.keySet() );
      }
    } );
  }

  @Override public List<IMetaStoreElementType> getElementTypes( final String namespace )
    throws MetaStoreException {
    return MetaStoreUtil.executeLockedOperation( readLock(), new Callable<List<IMetaStoreElementType>>() {
      @Override public List<IMetaStoreElementType> call() throws Exception {
        List<String> ids = getElementTypeIds( namespace );
        List<IMetaStoreElementType> types = Lists.newArrayListWithExpectedSize( ids.size() );
        for ( String id : ids ) {
          types.add( getElementType( namespace, id ) );
        }
        return types;
      }
    } );
  }

  @Override public IMetaStoreElementType getElementTypeByName( final String namespace, final String elementTypeName )
    throws MetaStoreException {
    return MetaStoreUtil.executeLockedOperation( readLock(), new Callable<IMetaStoreElementType>() {
      @Override public IMetaStoreElementType call() throws Exception {
        List<IMetaStoreElementType> elementTypes = getElementTypes( namespace );
        for ( IMetaStoreElementType elementType : elementTypes ) {
          if ( elementType.getName().equals( elementTypeName ) ) {
            return elementType;
          }
        }
        return null;
      }
    } );
  }

  @Override public void updateElementType( String namespace, IMetaStoreElementType elementType )
    throws MetaStoreException {
    elementType.setNamespace( namespace );

    update( JsonElementType.from( elementType ) );
  }

  @Override public void deleteElementType( final String namespace,
                                           final IMetaStoreElementType elementType ) throws MetaStoreException {
    MetaStoreUtil.executeLockedOperation( writeLock(), new Callable<Void>() {
        @Override public Void call() throws Exception {
          List<String> dependencies = getElementIds( namespace, elementType );
          if ( dependencies.isEmpty() ) {
            IAttributes.getAttributesMap().remove( JsonElement.groupName( elementType ) );
            Map<String, String> typeMap = IAttributes.getAttributes( JsonElementType.groupName( namespace ) );
            if ( typeMap != null ) {
              typeMap.remove( elementType.getId() );
            }
          } else {
            throw new MetaStoreDependenciesExistsException( dependencies );
          }
          return null;
        }
      }
    );
  }

  @Override public JsonElement newElement() {
    return new JsonElement();
  }

  @Override public JsonElement newElement( IMetaStoreElementType elementType, String id, Object value ) {
    return new JsonElement( new MemoryMetaStoreElement( elementType, id, value ) );
  }

  @Override public IMetaStoreAttribute newAttribute( String id, Object value ) {
    return new MemoryMetaStoreAttribute( id, value );
  }

  @Override public IMetaStoreElementOwner newElementOwner( String name, MetaStoreElementOwnerType ownerType ) {
    return new MemoryMetaStoreElementOwner( name, ownerType );
  }

  @Override public void createElement( String namespace, IMetaStoreElementType elementType,
                                       IMetaStoreElement element ) throws MetaStoreException {
    elementType.setNamespace( namespace );
    element.setElementType( elementType );
    update( JsonElementType.from( elementType ) );
    if ( !create( JsonElement.from( element ) ) ) {
      throw new MetaStoreElementExistException( getElements( namespace, elementType ) );
    }
  }

  @Override
  public List<IMetaStoreElement> getElements( final String namespace,
                                              final IMetaStoreElementType elementType ) throws MetaStoreException {
    return MetaStoreUtil.executeLockedOperation( readLock(), new Callable<List<IMetaStoreElement>>() {
      @Override public List<IMetaStoreElement> call() throws Exception {
        List<String> ids = getElementIds( namespace, elementType );
        List<IMetaStoreElement> types = Lists.newArrayListWithExpectedSize( ids.size() );
        for ( String id : ids ) {
          types.add( getElement( namespace, elementType, id ) );
        }
        return types;
      }
    } );
  }

  @Override public List<String> getElementIds( final String namespace,
                                               final IMetaStoreElementType elementType ) throws MetaStoreException {
    return MetaStoreUtil.executeLockedOperation( readLock(), new Callable<List<String>>() {
        @Override public List<String> call() throws Exception {
          elementType.setNamespace( namespace );
          Map<String, String> attributes = IAttributes.getAttributes( JsonElement.groupName( elementType ) );
          return attributes == null ? ImmutableList.<String>of() : ImmutableList.copyOf( attributes.keySet() );
        }
      }
    );
  }

  @Override public JsonElement getElement( final String namespace, final IMetaStoreElementType elementType,
                                           final String elementId ) throws MetaStoreException {
    return MetaStoreUtil.executeLockedOperation( readLock(), new Callable<JsonElement>() {
      @Override public JsonElement call() throws Exception {
        JsonElement element = newElement();
        elementType.setNamespace( namespace );
        element.setId( elementId );
        element.setElementType( elementType );
        String jsonData = IAttributes.getAttribute( element.groupName(), element.key() );
        return jsonData == null ? null : element.load( jsonData );
      }
    } );
  }

  @Override
  public IMetaStoreElement getElementByName( String namespace, IMetaStoreElementType elementType, final String name )
    throws MetaStoreException {
    for ( IMetaStoreElement element : getElements( namespace, elementType ) ) {
      if ( element.getName().equals( name ) ) {
        return element;
      }
    }
    return null;
  }

  @Override public void updateElement( String namespace, IMetaStoreElementType elementType, String elementId,
                                       IMetaStoreElement element ) throws MetaStoreException {
    elementType.setNamespace( namespace );
    element.setId( elementId );
    element.setElementType( elementType );
    update( JsonElementType.from( elementType ) );
    update( JsonElement.from( element ) );
  }

  @Override
  public void deleteElement( final String namespace, final IMetaStoreElementType elementType, final String elementId )
    throws MetaStoreException {
    MetaStoreUtil.executeLockedOperation( writeLock(), new Callable<Boolean>() {
      @Override public Boolean call() throws Exception {
        elementType.setNamespace( namespace );
        Map<String, String> attributes = IAttributes.getAttributes( JsonElement.groupName( elementType ) );

        return attributes != null && attributes.remove( elementId ) != null;
      }
    } );
  }

  private boolean create( final AttributesInterfaceEntry entry ) throws MetaStoreException {
    return MetaStoreUtil.executeLockedOperation( writeLock(), new Callable<Boolean>() {
      @Override public Boolean call() throws Exception {
        String groupName = entry.groupName();
        String key = entry.key();

        String existing = IAttributes.getAttribute( groupName, key );
        if ( existing == null ) {
          IAttributes.setAttribute( groupName, key, entry.jsonValue() );
          return true;
        } else {
          return false;
        }
      }
    } );
  }

  private void update( final AttributesInterfaceEntry entry ) throws MetaStoreException {
    MetaStoreUtil.executeLockedOperation( writeLock(), new Callable<Void>() {
      @Override public Void call() throws Exception {
        IAttributes.setAttribute( entry.groupName(), entry.key(), entry.jsonValue() );
        return null;
      }
    } );
  }

  @Override public Lock readLock() {
    return lock.readLock();
  }

  @Override public Lock writeLock() {
    return lock.writeLock();
  }
}
