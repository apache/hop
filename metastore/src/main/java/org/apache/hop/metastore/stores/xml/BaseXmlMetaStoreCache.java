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

package org.apache.hop.metastore.stores.xml;

import org.apache.hop.metastore.api.IMetaStoreElementType;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public abstract class BaseXmlMetaStoreCache implements IXmlMetaStoreCache {

  private final Map<String, Long> processedFiles = new HashMap<String, Long>();

  private final Map<String, ElementType> elementTypesMap = new HashMap<String, ElementType>();

  @Override
  public synchronized void registerElementTypeIdForName( String elementTypeName, String elementId ) {
    ElementType elementType = elementTypesMap.get( elementTypeName );
    if ( elementType == null ) {
      elementType = createElementType( elementId );
      elementTypesMap.put( elementTypeName, elementType );
    } else if ( !elementType.getId().equals( elementId ) ) {
      elementType.unregisterElements();
      elementType.setId( elementId );
    }
  }

  @Override
  public synchronized String getElementTypeIdByName( String elementTypeName ) {
    ElementType element = elementTypesMap.get( elementTypeName );
    return element == null ? null : element.getId();
  }

  @Override
  public synchronized void unregisterElementTypeId( String elementTypeId ) {

    Iterator<Entry<String, ElementType>> iterator = elementTypesMap.entrySet().iterator();
    while ( iterator.hasNext() ) {
      Entry<String, ElementType> elementType = iterator.next();
      if ( elementType.getValue().getId().equals( elementTypeId ) ) {
        iterator.remove();
        return;
      }
    }
  }

  @Override
  public synchronized void registerElementIdForName( IMetaStoreElementType elementType, String elementName,
                                                     String elementId ) {
    ElementType type = elementTypesMap.get( elementType.getName() );
    if ( type == null ) {
      registerElementTypeIdForName( elementType.getName(), elementType.getId() );
      type = elementTypesMap.get( elementType.getName() );
    }
    type.registerElementIdForName( elementName, elementId );
  }

  @Override
  public synchronized String getElementIdByName( IMetaStoreElementType elementType, String elementName ) {
    ElementType type = elementTypesMap.get( elementType.getName() );
    return type == null ? null : type.getElementIdByName( elementName );
  }

  @Override
  public synchronized void unregisterElementId( IMetaStoreElementType elementType, String elementId ) {
    ElementType type = elementTypesMap.get( elementType.getName() );
    if ( type == null ) {
      return;
    }
    type.unregisterElementId( elementId );
  }

  @Override
  public synchronized void registerProcessedFile( String fullPath, long lastUpdate ) {
    processedFiles.put( fullPath, lastUpdate );
  }

  @Override
  public synchronized Map<String, Long> getProcessedFiles() {
    return Collections.unmodifiableMap( processedFiles );
  }

  @Override
  public synchronized void unregisterProcessedFile( String fullPath ) {
    processedFiles.remove( fullPath );
  }

  public synchronized void clear() {
    processedFiles.clear();
    for ( ElementType elementType : elementTypesMap.values() ) {
      elementType.unregisterElements();
    }
    elementTypesMap.clear();
  }

  protected abstract <K, V> Map<K, V> createStorage();

  protected abstract ElementType createElementType( String elementId );

  protected abstract static class ElementType {

    private String id;

    public ElementType( String id ) {
      this.id = id;

    }

    public String getId() {
      return id;
    }

    public void setId( String id ) {
      this.id = id;
    }

    protected abstract Map<String, String> getElementNameToIdMap();

    public void registerElementIdForName( String elementName, String elementId ) {
      if ( elementId == null ) {
        return;
      }
      Map<String, String> elementNameToIdMap = getElementNameToIdMap();
      elementNameToIdMap.put( elementName, elementId );
    }

    public String getElementIdByName( String elementName ) {
      Map<String, String> elementNameToIdMap = getElementNameToIdMap();
      return elementNameToIdMap.get( elementName );
    }

    public void unregisterElementId( String elementId ) {
      Map<String, String> elementNameToIdMap = getElementNameToIdMap();
      Iterator<Entry<String, String>> iterator = elementNameToIdMap.entrySet().iterator();
      while ( iterator.hasNext() ) {
        Entry<String, String> element = iterator.next();
        if ( element.getValue().equals( elementId ) ) {
          iterator.remove();
          return;
        }
      }
    }

    public void unregisterElements() {
      Map<String, String> elementNameToIdMap = getElementNameToIdMap();
      elementNameToIdMap.clear();
    }
  }
}
