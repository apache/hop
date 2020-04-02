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
package org.apache.hop.metastore.stores.xml;

import java.util.HashMap;
import java.util.Map;

/**
 * This implementation provides a simple IXmlMetaStoreCache.
 * It uses strong references thus client should clear it manually.
 */
public class PersistentXmlMetaStoreCache extends BaseXmlMetaStoreCache implements IXmlMetaStoreCache {

  @Override
  protected <K, V> Map<K, V> createStorage() {
    return new HashMap<K, V>();
  }

  @Override
  protected ElementType createElementType( String elementId ) {
    return new ElementType( elementId, this.<String, String>createStorage() );
  }

  protected static class ElementType extends BaseXmlMetaStoreCache.ElementType {

    private final Map<String, String> elementNameToIdMap;

    public ElementType( String id, Map<String, String> elementNameToIdMap ) {
      super( id );
      this.elementNameToIdMap = elementNameToIdMap;
    }

    @Override
    protected Map<String, String> getElementNameToIdMap() {
      return elementNameToIdMap;
    }

  }

}
