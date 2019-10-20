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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hop.metastore.api.IMetaStoreAttribute;
import org.apache.hop.metastore.api.exceptions.MetaStoreException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class XmlMetaStoreAttribute implements IMetaStoreAttribute {

  public static final String XML_TAG = "attribute";

  protected String id;
  protected Object value;

  protected Map<String, IMetaStoreAttribute> children;

  protected String filename;

  public XmlMetaStoreAttribute() {
    children = new HashMap<String, IMetaStoreAttribute>();
    this.id = null;
    this.value = null;
  }

  public XmlMetaStoreAttribute( String id, Object value ) {
    this();
    this.id = id;
    this.value = value;
  }

  /**
   * Duplicate the element data into this structure.
   * 
   * @param element
   */
  public XmlMetaStoreAttribute( IMetaStoreAttribute element ) {
    this();
    id = element.getId();
    value = element.getValue();
    for ( IMetaStoreAttribute childElement : element.getChildren() ) {
      addChild( new XmlMetaStoreAttribute( childElement ) );
    }
  }

  protected void loadAttribute( Node attributeNode ) {
    NodeList elementNodes = attributeNode.getChildNodes();
    for ( int e = 0; e < elementNodes.getLength(); e++ ) {
      Node elementNode = elementNodes.item( e );
      if ( "id".equals( elementNode.getNodeName() ) ) {
        id = XmlUtil.getNodeValue( elementNode );
      } else if ( "value".equals( elementNode.getNodeName() ) ) {
        value = XmlUtil.getNodeValue( elementNode );
      } else if ( "type".equals( elementNode.getNodeName() ) ) {
        String type = XmlUtil.getNodeValue( elementNode );
        if ( "Integer".equals( type ) ) {
          value = Integer.valueOf( (String) value );
        } else if ( "Double".equals( type ) ) {
          value = Double.valueOf( (String) value );
        } else if ( "Long".equals( type ) ) {
          value = Long.valueOf( (String) value );
        } /*
           * else { value = value; }
           */
      } else if ( "children".equals( elementNode.getNodeName() ) ) {
        NodeList childNodes = elementNode.getChildNodes();
        for ( int c = 0; c < childNodes.getLength(); c++ ) {
          Node childNode = childNodes.item( c );
          if ( childNode.getNodeName().equals( "child" ) ) {
            XmlMetaStoreAttribute childElement = new XmlMetaStoreAttribute();
            childElement.loadAttribute( childNode );
            addChild( childElement );
          }
        }
      }
    }
  }

  @Override
  public void deleteChild( String entityId ) {
    Iterator<IMetaStoreAttribute> it = children.values().iterator();
    while ( it.hasNext() ) {
      IMetaStoreAttribute element = it.next();
      if ( element.getId().equals( entityId ) ) {
        it.remove();
        return;
      }
    }
  }

  /**
   * @return the id
   */
  public String getId() {
    return id;
  }

  /**
   * @param id
   *          the id to set
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
   * @param value
   *          the value to set
   */
  public void setValue( Object value ) {
    this.value = value;
  }

  /**
   * @return the children
   */
  public List<IMetaStoreAttribute> getChildren() {
    return new ArrayList<IMetaStoreAttribute>( children.values() );
  }

  /**
   * @param children
   *          the children to set
   */
  public void setChildren( List<IMetaStoreAttribute> children ) {
    this.children.clear();
    for ( IMetaStoreAttribute child : children ) {
      this.children.put( child.getId(), child );
    }
  }

  public void addChild( IMetaStoreAttribute element ) {
    children.put( element.getId(), element );
  }

  @Override
  public void clearChildren() {
    children.clear();
  }

  @Override
  public IMetaStoreAttribute getChild( String id ) {
    return children.get( id );
  }

  /**
   * @return the filename
   */
  public String getFilename() {
    return filename;
  }

  /**
   * @param filename
   *          the filename to set
   */
  public void setFilename( String filename ) {
    this.filename = filename;
  }

  protected void appendAttribute( IMetaStoreAttribute attribute, Document doc, Element parentElement )
    throws MetaStoreException {
    if ( attribute.getId() == null ) {
      throw new MetaStoreException( "An attribute has to be non-null" );
    }
    Element idElement = doc.createElement( "id" );
    idElement.appendChild( doc.createTextNode( attribute.getId() ) );
    parentElement.appendChild( idElement );

    Element valueElement = doc.createElement( "value" );
    valueElement
        .appendChild( doc.createTextNode( attribute.getValue() != null ? attribute.getValue().toString() : "" ) );
    parentElement.appendChild( valueElement );

    Element typeElement = doc.createElement( "type" );
    typeElement.appendChild( doc.createTextNode( getType( attribute.getValue() ) ) );
    parentElement.appendChild( typeElement );

    if ( !attribute.getChildren().isEmpty() ) {
      Element childrenElement = doc.createElement( "children" );
      parentElement.appendChild( childrenElement );
      for ( IMetaStoreAttribute childElement : attribute.getChildren() ) {
        Element child = doc.createElement( "child" );
        childrenElement.appendChild( child );
        appendAttribute( childElement, doc, child );
      }
    }
  }

  protected String getType( Object object ) {

    if ( object == null ) {
      return "String";
    }
    if ( object instanceof String ) {
      return "String";
    }
    if ( object instanceof Integer ) {
      return "Integer";
    }
    if ( object instanceof Long ) {
      return "Long";
    }
    if ( object instanceof Double ) {
      return "Double";
    }

    return "String";
  }

}
