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

import org.apache.hop.metastore.api.exceptions.MetaStoreException;
import org.apache.hop.metastore.api.security.IMetaStoreElementOwner;
import org.apache.hop.metastore.api.security.MetaStoreElementOwnerType;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class XmlMetaStoreElementOwner implements IMetaStoreElementOwner {

  private String name;
  private MetaStoreElementOwnerType type;

  public XmlMetaStoreElementOwner( String name, MetaStoreElementOwnerType type ) {
    super();
    this.name = name;
    this.type = type;
  }

  /**
   * Load an element owner from an XML node
   * 
   * @param elementNode
   *          The node to load the element owner data from
   * @throws MetaStoreException
   *           In case there was an error loading the data, if data was incomplete, ...
   */
  public XmlMetaStoreElementOwner( Node elementNode ) throws MetaStoreException {

    NodeList ownerNodes = elementNode.getChildNodes();
    for ( int o = 0; o < ownerNodes.getLength(); o++ ) {
      Node ownerNode = ownerNodes.item( o );
      if ( "name".equals( ownerNode.getNodeName() ) ) {
        name = XmlUtil.getNodeValue( ownerNode );
      }
      if ( "type".equals( ownerNode.getNodeName() ) ) {
        String typeString = XmlUtil.getNodeValue( ownerNode );
        try {
          type = MetaStoreElementOwnerType.getOwnerType( typeString );
        } catch ( Exception ex ) {
          throw new MetaStoreException( "Unable to convert owner type [" + typeString
              + "] to one of USER, ROLE or SYSTEM_ROLE", ex );
        }
      }
    }

    /*
     * if (name==null) { throw new
     * MetaStoreException("An owner needs to have a name in the <security><owner><name> element"); } if (type==null) {
     * throw new MetaStoreException("An owner needs to have a type in the <security><owner><type> element"); }
     */
  }

  public XmlMetaStoreElementOwner( IMetaStoreElementOwner owner ) {
    this.name = owner.getName();
    this.type = owner.getOwnerType();
  }

  public void append( Document doc, Element ownerElement ) {
    Element nameElement = doc.createElement( "name" );
    nameElement.appendChild( doc.createTextNode( name == null ? "" : name ) );
    ownerElement.appendChild( nameElement );

    Element typeElement = doc.createElement( "type" );
    typeElement.appendChild( doc.createTextNode( type == null ? "" : type.name() ) );
    ownerElement.appendChild( typeElement );
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public void setName( String name ) {
    this.name = name;
  }

  @Override
  public MetaStoreElementOwnerType getOwnerType() {
    return type;
  }

  @Override
  public void setOwnerType( MetaStoreElementOwnerType type ) {
    this.type = type;
  }

}
