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

  private final Map<String, Map<String, ElementType>> elementTypesMap = new HashMap<String, Map<String, ElementType>>();

  @Override
  public synchronized void registerElementTypeIdForName( String namespace, String elementTypeName, String elementId ) {
    Map<String, ElementType> elementTypeNameToId = elementTypesMap.get( namespace );
    if ( elementTypeNameToId == null ) {
      elementTypeNameToId = createStorage();
      elementTypesMap.put( namespace, elementTypeNameToId );
    }
    ElementType elementType = elementTypeNameToId.get( elementTypeName );
    if ( elementType == null ) {
      elementType = createElementType( elementId );
      elementTypeNameToId.put( elementTypeName, elementType );
    } else if ( !elementType.getId().equals( elementId ) ) {
      elementType.unregisterElements();
      elementType.setId( elementId );
    }
  }

  @Override
  public synchronized String getElementTypeIdByName( String namespace, String elementTypeName ) {
    Map<String, ElementType> elementTypeNameToId = elementTypesMap.get( namespace );
    if ( elementTypeNameToId == null ) {
      return null;
    }

    ElementType element = elementTypeNameToId.get( elementTypeName );
    return element == null ? null : element.getId();
  }

  @Override
  public synchronized void unregisterElementTypeId( String namespace, String elementTypeId ) {
    Map<String, ElementType> elementTypeNameToId = elementTypesMap.get( namespace );
    if ( elementTypeNameToId == null ) {
      return;
    }
    Iterator<Entry<String, ElementType>> iterator = elementTypeNameToId.entrySet().iterator();
    while ( iterator.hasNext() ) {
      Entry<String, ElementType> elementType = iterator.next();
      if ( elementType.getValue().getId().equals( elementTypeId ) ) {
        iterator.remove();
        return;
      }
    }
  }

  @Override
  public synchronized void registerElementIdForName( String namespace, IMetaStoreElementType elementType, String elementName,
                                                     String elementId ) {
    Map<String, ElementType> nameToElementType = elementTypesMap.get( namespace );
    if ( nameToElementType == null ) {
      registerElementTypeIdForName( namespace, elementType.getName(), elementType.getId() );
      nameToElementType = elementTypesMap.get( namespace );
    }
    ElementType type = nameToElementType.get( elementType.getName() );
    if ( type == null ) {
      registerElementTypeIdForName( namespace, elementType.getName(), elementType.getId() );
      type = nameToElementType.get( elementType.getName() );
    }
    type.registerElementIdForName( elementName, elementId );
  }

  @Override
  public synchronized String getElementIdByName( String namespace, IMetaStoreElementType elementType, String elementName ) {
    Map<String, ElementType> elementTypeNameToId = elementTypesMap.get( namespace );
    if ( elementTypeNameToId == null ) {
      return null;
    }
    ElementType type = elementTypeNameToId.get( elementType.getName() );
    return type == null ? null : type.getElementIdByName( elementName );
  }

  @Override
  public synchronized void unregisterElementId( String namespace, IMetaStoreElementType elementType, String elementId ) {
    Map<String, ElementType> elementTypeNameToId = elementTypesMap.get( namespace );
    if ( elementTypeNameToId == null ) {
      return;
    }
    ElementType type = elementTypeNameToId.get( elementType.getName() );
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
    for ( Map<String, ElementType> namespaceElementType : elementTypesMap.values() ) {
      for ( ElementType elementType : namespaceElementType.values() ) {
        elementType.unregisterElements();
      }
      namespaceElementType.clear();
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
