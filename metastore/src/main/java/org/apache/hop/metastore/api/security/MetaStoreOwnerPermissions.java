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

package org.apache.hop.metastore.api.security;

import org.apache.hop.metastore.api.exceptions.MetaStoreException;
import org.apache.hop.metastore.stores.xml.XmlMetaStoreElementOwner;
import org.apache.hop.metastore.stores.xml.XmlUtil;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.ArrayList;
import java.util.List;

public class MetaStoreOwnerPermissions {

  private IMetaStoreElementOwner owner;
  private List<MetaStoreObjectPermission> permissions;

  public MetaStoreOwnerPermissions() {
    this( (IMetaStoreElementOwner) null );
  }

  public MetaStoreOwnerPermissions( IMetaStoreElementOwner owner ) {
    this( owner, new ArrayList<MetaStoreObjectPermission>() );
  }

  public MetaStoreOwnerPermissions( IMetaStoreElementOwner owner, MetaStoreObjectPermission... permissions ) {
    super();
    this.permissions = new ArrayList<MetaStoreObjectPermission>();
    if ( owner != null ) {
      this.owner = new XmlMetaStoreElementOwner( owner );
    }
    for ( MetaStoreObjectPermission permission : permissions ) {
      this.permissions.add( permission );
    }
  }

  public MetaStoreOwnerPermissions( IMetaStoreElementOwner owner, List<MetaStoreObjectPermission> permissions ) {
    super();
    this.permissions = new ArrayList<MetaStoreObjectPermission>();
    if ( owner != null ) {
      this.owner = new XmlMetaStoreElementOwner( owner );
    }
    for ( MetaStoreObjectPermission permission : permissions ) {
      this.permissions.add( permission );
    }
  }

  public MetaStoreOwnerPermissions( Node opNode ) throws MetaStoreException {
    this();
    NodeList childNodes = opNode.getChildNodes();
    for ( int c = 0; c < childNodes.getLength(); c++ ) {
      Node childNode = childNodes.item( c );
      if ( "owner".equals( childNode.getNodeName() ) ) {
        owner = new XmlMetaStoreElementOwner( childNode );
        if ( owner.getName() == null || owner.getOwnerType() == null ) {
          owner = null;
        }
      }
      if ( "permissions".equals( childNode.getNodeName() ) ) {
        NodeList pNodes = childNode.getChildNodes();
        for ( int p = 0; p < pNodes.getLength(); p++ ) {
          Node pNode = pNodes.item( p );
          if ( "permission".equals( pNode.getNodeName() ) ) {
            String permissionString = XmlUtil.getNodeValue( pNode );
            try {
              permissions.add( MetaStoreObjectPermission.valueOf( permissionString ) );
            } catch ( Exception e ) {
              throw new MetaStoreException( "Unable to recognize permission '" + permissionString
                + "' as one of CREATE, READ, UPDATE or DELETE", e );
            }
          }
        }
      }
    }
  }

  public void append( Document doc, Element element ) {
    Element ownerElement = doc.createElement( "owner" );
    if ( owner != null ) {
      ( (XmlMetaStoreElementOwner) owner ).append( doc, ownerElement );
    }
    element.appendChild( ownerElement );

    Element permissionsElement = doc.createElement( "permissions" );
    for ( MetaStoreObjectPermission permission : permissions ) {
      Element permissionElement = doc.createElement( "permission" );
      permissionElement.appendChild( doc.createTextNode( permission.name() ) );
      permissionsElement.appendChild( permissionElement );
    }
    element.appendChild( permissionsElement );
  }

  public IMetaStoreElementOwner getOwner() {
    return owner;
  }

  public List<MetaStoreObjectPermission> getPermissions() {
    return permissions;
  }

  public void setOwner( IMetaStoreElementOwner owner ) {
    this.owner = owner;
  }

  public void setPermissions( List<MetaStoreObjectPermission> permissions ) {
    this.permissions = permissions;
  }

}
