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

package org.apache.hop.metastore.util;

import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.metastore.api.IMetaStoreAttribute;
import org.apache.hop.metastore.api.IMetaStoreElement;
import org.apache.hop.metastore.api.IMetaStoreElementType;
import org.apache.hop.metastore.api.exceptions.MetaStoreException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;

/**
 * Generally useful methods for extracting data
 *
 * @author matt
 */
public class MetaStoreUtil {

  public static String getChildString( IMetaStoreAttribute attribute, String id ) {
    IMetaStoreAttribute child = attribute.getChild( id );
    if ( child == null ) {
      return null;
    }

    return getAttributeString( child );
  }

  public static String getAttributeString( IMetaStoreAttribute attribute ) {
    if ( attribute.getValue() == null ) {
      return null;
    }
    return attribute.getValue().toString();
  }

  public static boolean getAttributeBoolean( IMetaStoreAttribute attribute, String id ) {
    String b = getChildString( attribute, id );
    if ( b == null ) {
      return false;
    }
    return "true".equalsIgnoreCase( b ) || "y".equalsIgnoreCase( b );
  }

  public static <T> T executeLockedOperation( Lock lock, Callable<T> callee ) throws MetaStoreException {
    lock.lock();
    try {
      if ( callee != null ) {
        return callee.call();
      }
    } catch ( Exception e ) {
      if ( e instanceof MetaStoreException ) {
        throw (MetaStoreException) e;
      }
      throw new MetaStoreException( e );
    } finally {
      lock.unlock();
    }
    return null;
  }

  public static <T> T executeLockedOperationQuietly( Lock lock, Callable<T> callee ) {
    T result = null;
    try {
      result = executeLockedOperation( lock, callee );
    } catch ( Exception e ) {
      // ignore
    }
    return result;
  }

  /**
   * Get a sorted list of element names for the specified element type.
   *
   * @param metaStore
   * @param elementType
   * @return
   * @throws MetaStoreException
   */
  public String[] getElementNames( IMetaStore metaStore, IMetaStoreElementType elementType )
    throws MetaStoreException {
    List<String> names = new ArrayList<>();

    List<IMetaStoreElement> elements = metaStore.getElements( elementType );
    for ( IMetaStoreElement element : elements ) {
      names.add( element.getName() );
    }

    // Alphabetical sort
    //
    Collections.sort( names );

    return names.toArray( new String[ names.size() ] );
  }

  public static void copy( IMetaStore from, IMetaStore to ) throws MetaStoreException {
    copy( from, to, false );
  }

  public static void copy( IMetaStore from, IMetaStore to, boolean overwrite ) throws MetaStoreException {


    // get all of the element types defined in this namespace
    List<IMetaStoreElementType> elementTypes = from.getElementTypes();
    for ( IMetaStoreElementType elementType : elementTypes ) {
      // See if it's already there
      IMetaStoreElementType existingType = to.getElementTypeByName( elementType.getName() );
      if ( existingType != null ) {
        if ( overwrite ) {
          // we looked it up by name, but need to update it by id
          elementType.setId( existingType.getId() );
          to.updateElementType( elementType );
        } else {
          elementType = existingType;
        }
      } else {
        // create the elementType in the "to" metastore
        to.createElementType( elementType );
      }

      // get all of the elements defined for this type
      List<IMetaStoreElement> elements = from.getElements( elementType );
      for ( IMetaStoreElement element : elements ) {
        IMetaStoreElement existingElement = to.getElementByName( elementType, element.getName() );
        if ( existingElement != null ) {
          element.setId( existingElement.getId() );
          if ( overwrite ) {
            to.updateElement( elementType, existingElement.getId(), element );
          }
        } else {
          to.createElement( elementType, element );
        }
      }
    }
  }
}
