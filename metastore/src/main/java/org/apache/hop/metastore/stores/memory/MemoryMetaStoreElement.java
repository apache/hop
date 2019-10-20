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

import java.util.ArrayList;
import java.util.List;

import org.apache.hop.metastore.api.IMetaStoreElement;
import org.apache.hop.metastore.api.IMetaStoreElementType;
import org.apache.hop.metastore.api.security.IMetaStoreElementOwner;
import org.apache.hop.metastore.api.security.MetaStoreOwnerPermissions;
import org.apache.hop.metastore.stores.xml.XmlMetaStoreElementOwner;

public class MemoryMetaStoreElement extends MemoryMetaStoreAttribute implements IMetaStoreElement {

  protected String name;

  protected IMetaStoreElementType elementType;

  protected IMetaStoreElementOwner owner;
  protected List<MetaStoreOwnerPermissions> ownerPermissionsList;

  public MemoryMetaStoreElement() {
    this( null, null, null );
  }

  public MemoryMetaStoreElement( IMetaStoreElementType elementType, String id, Object value ) {
    super( id, value );
    this.elementType = elementType;

    this.ownerPermissionsList = new ArrayList<MetaStoreOwnerPermissions>();
  }

  public MemoryMetaStoreElement( IMetaStoreElement element ) {
    super( element );
    this.name = element.getName();
    this.elementType = element.getElementType();
    this.ownerPermissionsList = new ArrayList<MetaStoreOwnerPermissions>();
    if ( element.getOwner() != null ) {
      this.owner = new XmlMetaStoreElementOwner( element.getOwner() );
    }
    for ( MetaStoreOwnerPermissions ownerPermissions : element.getOwnerPermissionsList() ) {
      this.getOwnerPermissionsList().add(
          new MetaStoreOwnerPermissions( ownerPermissions.getOwner(), ownerPermissions.getPermissions() ) );
    }
  }

  public IMetaStoreElementOwner getOwner() {
    return owner;
  }

  public void setOwner( IMetaStoreElementOwner owner ) {
    this.owner = owner;
  }

  public List<MetaStoreOwnerPermissions> getOwnerPermissionsList() {
    return ownerPermissionsList;
  }

  public void setOwnerPermissionsList( List<MetaStoreOwnerPermissions> ownerPermissions ) {
    this.ownerPermissionsList = ownerPermissions;
  }

  public String getName() {
    return name;
  }

  public void setName( String name ) {
    this.name = name;
  }

  public IMetaStoreElementType getElementType() {
    return elementType;
  }

  public void setElementType( IMetaStoreElementType elementType ) {
    this.elementType = elementType;
  }
}
