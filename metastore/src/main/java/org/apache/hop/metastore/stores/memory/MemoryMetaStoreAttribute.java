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

import org.apache.hop.metastore.api.IMetaStoreAttribute;
import org.apache.hop.metastore.util.MetaStoreUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

public class MemoryMetaStoreAttribute implements IMetaStoreAttribute {

  private final ReadLock readLock;
  private final WriteLock writeLock;

  protected final Map<String, IMetaStoreAttribute> children;

  protected String id;
  protected Object value;

  public MemoryMetaStoreAttribute() {
    this( null, null );
  }

  public MemoryMetaStoreAttribute( String id, Object value ) {
    this.id = id;
    this.value = value;
    this.children = new HashMap<String, IMetaStoreAttribute>();

    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    readLock = lock.readLock();
    writeLock = lock.writeLock();
  }

  public MemoryMetaStoreAttribute( IMetaStoreAttribute attribute ) {
    this( attribute.getId(), attribute.getValue() );

    for ( IMetaStoreAttribute childElement : attribute.getChildren() ) {
      addChild( new MemoryMetaStoreAttribute( childElement ) );
    }
  }

  /**
   * @return the id
   */
  public String getId() {
    return id;
  }

  /**
   * @param id the id to set
   */
  public void setId( String id ) {
    this.id = id;
  }

  /**
   * @return the value
   */
  public Object getValue() {
    return value;
  }

  /**
   * @param value the value to set
   */
  public void setValue( Object value ) {
    this.value = value;
  }

  /**
   * @return the children
   */
  public List<IMetaStoreAttribute> getChildren() {
    return MetaStoreUtil.executeLockedOperationQuietly( readLock, new Callable<List<IMetaStoreAttribute>>() {

      @Override
      public List<IMetaStoreAttribute> call() throws Exception {
        return new ArrayList<IMetaStoreAttribute>( children.values() );
      }
    } );
  }

  /**
   * @param children the children to set
   */
  public void setChildren( final List<IMetaStoreAttribute> childrenList ) {
    MetaStoreUtil.executeLockedOperationQuietly( writeLock, new Callable<Void>() {

      @Override
      public Void call() throws Exception {

        children.clear();
        for ( IMetaStoreAttribute child : childrenList ) {
          children.put( child.getId(), child );
        }
        return null;
      }
    } );
  }

  @Override
  public void addChild( final IMetaStoreAttribute attribute ) {
    MetaStoreUtil.executeLockedOperationQuietly( writeLock, new Callable<Void>() {

      @Override
      public Void call() throws Exception {
        children.put( attribute.getId(), attribute );
        return null;
      }
    } );
  }

  @Override
  public void deleteChild( final String attributeId ) {
    MetaStoreUtil.executeLockedOperationQuietly( writeLock, new Callable<Void>() {

      @Override
      public Void call() throws Exception {
        Iterator<IMetaStoreAttribute> iterator = children.values().iterator();
        while ( iterator.hasNext() ) {
          IMetaStoreAttribute next = iterator.next();
          if ( next.getId().equals( attributeId ) ) {
            iterator.remove();
            break;
          }
        }
        return null;
      }
    } );
  }

  /**
   * Remove all child attributes
   */
  public void clearChildren() {
    MetaStoreUtil.executeLockedOperationQuietly( writeLock, new Callable<Void>() {

      @Override
      public Void call() throws Exception {
        children.clear();
        return null;
      }
    } );
  }

  public IMetaStoreAttribute getChild( final String id ) {
    return MetaStoreUtil.executeLockedOperationQuietly( readLock, new Callable<IMetaStoreAttribute>() {

      @Override
      public IMetaStoreAttribute call() throws Exception {
        return children.get( id );
      }
    } );
  }
}
